# Profiler: план работ

Источники: расшифровка созвона `Profiler-discussions.txt`, рабочие заметки `profiler-mom.md`, фактическое состояние кода в `backend/`.

Документ расширяет пункты из `profiler-mom.md` ссылками на конкретные места в коде и фиксирует открытые вопросы, ответы на которые нужны до старта работ.

---

## 0. Текущее состояние (что выяснилось при анализе кода)

Репо `backend/` содержит четыре приложения, и у них **разные языки и разное состояние готовности**:

| Приложение | Язык / стек | Статус сборки | Хранилища |
|---|---|---|---|
| `backend/apps/collector` | Java 21 / Quarkus 3.25 (Maven) | **исключён из `make build-all`**: `APPS := dumps-collector maintenance` в `backend/Makefile` | Postgres + S3 (MinIO) |
| `backend/apps/maintenance` | Go 1.25 | Собирается | Postgres + S3 |
| `backend/apps/query` | React 18 + AntD 4.24 + `@netcracker/ux-react` 4.5 + `@netcracker/cse-ui-components` 2.1 | **исключён из `make build-all`** | — (UI) |
| `backend/apps/dumps-collector` | Go 1.25 | Собирается | SQLite (`glebarez/sqlite`) + PV |

Важное следствие: утверждение на митинге «все три куска написаны на Go» относится к go-шным `maintenance` + `dumps-collector` + (будущему Go-переписанному) коллектору. Сейчас приёмник TCP-трафика от агентов — это именно Java-приложение `collector`, никакой go-шной замены ему пока нет.

Дополнительно:
- `backend/charts/profiler-stack/values.yaml` — единый зонтичный чарт, ожидающий на входе `INFRA_POSTGRES_*`, `INFRA_S3_MINIO_*`. То есть развёртывание «как задумано» сейчас требует Postgres + S3.
- `backend/libs/pg/db.go` описывает схему: темп-таблицы `calls_<ts>`, `traces_<ts>`, `suspend_<ts>` — гранулярность 5 минут; `dumps_*` — 1 час, TTL 7 дней; инвертированный индекс — 1 час, TTL 14 дней. Старые партиции целиком дропаются (см. `backend/libs/pg/resources/schema/migration/*`).
- Код-путь коллектора, который пишет в Postgres, живёт в `backend/apps/collector/src/main/java/com/netcracker/persistence/adapters/cloud/` (DAO per-entity: calls, traces, dictionary, params, dump-и, pod_statistics). Вопрос «что именно пишется в Postgres» — отвечает: метаданные + распакованные call-записи + сами байты трейсов (`traces_<ts>.trace bytea`).
- `dumps-collector` уже на SQLite и PV, без Postgres — это живой пример «all-in-one». Его паттерн (`backend/apps/dumps-collector/pkg/client/sqlite`, задачи rescan/insert/pack/remove через `oklog/run`) удобно взять за основу при сведении профайлерного стека к одному бинарю.
- Агент (`agent/`, `dumper/`, `runtime/`) — отдельный проект, он инструментирует java-приложения и по TCP шлёт поток в collector-service (порт 1715 из Helm values).

---

## Подход: contracts-first, последовательная замена, без реанимации Postgres-пути

**Не реанимируем существующий Postgres-путь как baseline.** Мы уже знаем, что писать сырой поток от агента в Postgres — тупик (раздел 0, про `traces_<ts>.trace bytea`). Чинить этот код и лечить его багов, только чтобы потом удалить, — потеря времени.

**Не делаем golden output / walking skeleton.** Новая архитектура легитимно меняет формат агрегатов и границы ротации, бит-в-бит сравнение бессмысленно. Валидация — через синтетические input-фикстуры (уже есть `backend/tools/load-generator/` и `backend/tools/data-generator/`) и ассерты на семантику (топ-метод, p95, число вызовов в диапазоне). Ничего бинарного в репо.

**Работа идёт последовательно от контрактов к коду.** Сначала — четыре контрактных документа + архитектурные диаграммы. Потом — реализация сервисов по одному, каждый с интеграционным тестом поверх синтетического входа.

Сохраняется **переиспользование кода, который переживает смену архитектуры** — парсинг агентского протокола в Java collector, парquet writer/reader, S3 abstraction, UI-компоненты (кроме архивных зависимостей). Выбрасываются Postgres-DAO, temp-table jobs и Postgres read path в query.

---

## Stage 0. Контракты и диаграммы (≈3 дня)

Этап-ворота: без этих документов дальше не двигаемся.

### 0.1 Write contract
Что collector пишет на локальный RWO PV и в S3.
- Формат WAL словарей: какие записи (method/param/tag), как сериализуются, когда fsync, ротация.
- Формат spill-SQLite: схема таблиц, когда запись туда выдавливается (таймер, memory pressure, explicit flush), как читается обратно при закрытии корневого вызова.
- Parquet schema: колонки, типы, какие агрегаты, как кодируются call-tree (reuse существующий формат из `backend/libs/storage/parquet/`).
- Папочная структура на локальном PV (`wal/`, `spill/`, `parquet-pending/`) и в S3 (`parquet/<namespace>/<service>/<duration-bucket>/<ts>.parquet` или аналог).
- Именование файлов: префикс, суффикс, правила уникальности при восстановлении.
- Семантика flush: что делает реплика, когда parquet-окно закрылось по времени (загружает в S3, удаляет локально, обновляет метаиндекс).

### 0.2 Read contract
Как query получает данные.
- Endpoint-ы collector read API: `/query/calls`, `/query/calltree`, `/query/pods`, `/query/stats` — с полным описанием параметров и формата ответа. JSON или protobuf — решить на этапе 0.
- S3 read path: как query понимает, какие parquet-файлы покрывают временной диапазон; нужен ли метаиндекс в S3 (manifest-файл) или достаточно LIST по префиксу.
- Правила hot/cold границы: query берёт из collector для `[now - flushInterval, now]`, из S3 для старшего; флажок для запрошенной «absolute freshness» (запросить и из collector при перекрытии с S3, ради гарантии полноты).
- PK для дедупликации: `(pod_id, restart_time, trace_file_index, buffer_offset, record_index)`. Откуда берётся в новой схеме, сохраняется ли из агентского протокола.

### 0.3 Lifecycle
- Readiness probe collector: возвращает Ready только после загрузки WAL и восстановления словарей.
- Что происходит при рестарте: последовательность «старт → mount PV → read WAL → restore dicts → read spill → open for agent connections → expose read API → Ready».
- Flush triggers: time (period), size (bytes), memory pressure (% of budget) — явные пороги и как они конфигурируются.
- Shutdown: как collector корректно закрывает агентские коннекты, дофlush’ит spill, отметит Not Ready для исключения из fan-out.

### 0.4 Storage layout & discovery
- Что идёт в локальный PV / в S3 — уже закрыто в 0.1, здесь сводим всё в одну диаграмму.
- k8s-манифесты: StatefulSet для collector с `volumeClaimTemplates` (RWO), Headless Service (`clusterIP: None`), Deployment для query (без PV), Deployment/CronJob для maintenance. Явные лимиты ресурсов.
- Helm-чарт: как меняются текущие `backend/charts/profiler-stack/values.yaml`. Целевая конфигурация — «только S3 (или filesystem-эмулятор)», без Postgres. Postgres-поля из values удаляются.
- Env vars: `COLLECTOR_HEADLESS_SVC`, `S3_ENDPOINT`/`S3_BUCKET`, `POD_NAMESPACE`.

### 0.5 Диаграммы
- Data flow: agent → collector (in-mem agg + WAL + spill) → parquet local → S3; query → collector fan-out + S3 → merge.
- Deployment: поды, PV, Service, сетевые границы.
- State diagram: жизнь корневого вызова (открыт в памяти → агрегируется → закрыт → попал в текущий parquet → flushed в S3).
- Включить в репо как `.md` с Mermaid-диаграммами рядом с контрактами (например, `backend/docs/design/`).

### 0.6 Что остаётся открытым после Stage 0
Если какие-то вопросы не закрываются на этапе проектирования — явно их перечислить и заложить в Stage 1 early spike, а не догадываться по ходу кодинга.

---

## Stage 1. Новый collector: write path (≈2–3 нед)

Цель: collector принимает поток от агента, агрегирует в памяти, пишет WAL + spill + parquet на локальный PV. Ничего про чтение, ничего про S3.

### 1.1 Что переиспользуем без изменений
- Парсер агентского протокола: `backend/apps/collector/src/main/java/com/netcracker/cdt/collector/tcp/ProfilerAgentReader.java` + `CollectorOrchestratorThread.java` + `ConnectionState.java`. Протокол не трогаем, формат байтов не меняем.
- Parquet writer из `backend/libs/storage/parquet/`. Если он в Go — нужен мост, либо Go-collector для write path отдельно. Посмотреть на этапе 0.

### 1.2 Что пишем заново
- **In-memory aggregator:** структура, накапливающая агрегаты per-(pod × restart × namespace × service × method × duration-bucket) на окне ротации. Бюджет памяти конфигурируется.
- **WAL dictionaries:** каждый новый словарный identifier — fsync в append-only файл сразу. Формат — простой length-prefixed, без парquet.
- **Spill-to-SQLite:** когда агрегат висит дольше порога или память под давлением — выдавить часть агрегатов в SQLite (`glebarez/sqlite` по аналогии с dumps-collector).
- **Parquet flush:** по горизонту времени закрывается parquet-файл на локальном PV. Удаление сырого agregator-state после успешного flush.
- **Restart recovery:** на старте читается WAL, восстанавливаются словари, читается spill, агрегаты возвращаются в память.

### 1.3 Интеграционный тест
- `load-generator` → in-process collector (без сети, прямой вызов) → проверка parquet-файлов на PV.
- Ассерты на семантику: «для профиля X методов N, топ-метод M с длительностью T±ε».
- Отдельно — тест на рестарт: убить процесс посреди обработки → перезапустить → убедиться, что WAL+spill восстанавливаются и финальный parquet корректен.

### 1.4 Что НЕ делаем на этом этапе
- S3 upload — Stage 2.
- Read API — Stage 3.
- Helm/StatefulSet — Stage 6.
- Retention / maintenance — Stage 2.

---

## Stage 2. Maintenance: flush в S3 и ретеншн (≈1–2 нед)

Цель: parquet-файлы уезжают с локального PV в S3; применяется retention по duration-bucket’ам.

### 2.1 S3 upload
- Процесс (в collector или отдельный sidecar/CronJob): по завершении parquet-файла закачивает в S3 по известному layout из Stage 0, удаляет локальную копию.
- Переиспользует `backend/libs/s3/` (файловый эмулятор в dev, MinIO в prod).

### 2.2 Retention
- В maintenance (новый код, не старый `temp_db_*`): обход S3 с удалением parquet-файлов старше TTL для данного duration-bucket’а. Пример: `duration < 100ms` — 1 сутки; `100ms–1s` — 7 дней; `> 1s` — 30 дней.
- Переиспользует логику из `backend/apps/maintenance/pkg/maintenance/s3_file_remove_job.go`, но с новой политикой per-bucket.

### 2.3 Тест
- Загружаем известный набор parquet в test S3 с датами разной давности → прогоняем maintenance → проверяем, что осталось по ожидаемому набору правил.

---

## Stage 3. Collector read API (≈1–2 нед)

Цель: на collector появляется HTTP endpoint, отдающий агрегаты для hot-окна (in-flight + spill).

### 3.1 Endpoint-ы
- Из Stage 0.2 (Read contract): `/query/calls`, `/query/calltree`, `/query/pods`, `/query/stats`.
- Читает **только** локальное состояние (in-memory + spill SQLite). Ничего из S3.
- Образец реализации — `backend/apps/dumps-collector/pkg/server/http_server.go` (endpoint поверх SQLite + PV).

### 3.2 Тест
- `load-generator` → collector → ждём flush period/2 → запрос к read API → проверяем, что видны актуальные вызовы.
- Интеграция с Stage 1/2: когда данные уезжают в parquet (local) / S3, из read API они исчезают (это нормально — hot cutoff).

---

## Stage 4. Query: fan-out + merge (≈2–3 нед)

Цель: stateless сервис, обслуживающий HTTP-запросы пользователя. Fan-out к collector-репликам + чтение S3 + дедупликация.

### 4.1 Discovery
- Читает env `COLLECTOR_HEADLESS_SVC`, через `net.LookupHost` получает IP всех Ready реплик. В dev — одна реплика, в prod — N.
- Перезапрашивает DNS на каждый запрос (Go stdlib не кеширует на уровне процесса, OS кеш ≤ CoreDNS TTL).

### 4.2 Fan-out
- Параллельные HTTP-запросы ко всем collector-репликам за hot-окно. Таймаут per-replica. Partial result с пометкой при падении отдельной реплики.

### 4.3 S3 read
- Чтение parquet из S3 за cold-окно. Использовать `backend/libs/storage/parquet/` reader-ы. Возможно, нужен manifest-файл — решается в Stage 0.2.

### 4.4 Merge & dedup
- Мерджим hot + cold, дедуплицируем по PK из Stage 0.2. Сортировка по времени.

### 4.5 Тест
- `load-generator` к N collector-репликам → ждём частичный flush в S3 → запросы к query на диапазон, покрывающий hot+cold → убеждаемся, что нет пропусков и нет дублей.

---

## Stage 5. UI (≈2–3 нед)

Цель: query UI работает поверх нового API и собирается без архивного npm-реестра.

### 5.1 Миграция зависимостей
- Только сейчас — заменяем `@netcracker/ux-react`, `@netcracker/cse-ui-components`, `@netcracker/ux-assets` на чистый AntD 4.24 (уже в зависимостях).
- По фичам в `backend/apps/query/src/features/cdt/`: начать с `calls` + `sidebar` (главный сценарий), далее `pods-info`, `controls`, `heap-dumps`.
- Фирменную палитру — через AntD theme customization, не через форк компонентов.

### 5.2 Подключение к новому query API
- Update API client в query на эндпоинты из Stage 0.2.
- Убрать все запросы, которые раньше шли в старый Postgres-based query backend.

### 5.3 Новые дизайны из Figma
- Выгрузить фигма-дизайны, которые уже были у Алексея (calls-таблица с меньшими паддингами, макапы других вью).
- Подключить MCP-интеграцию Figma к Claude для ускорения разметки.

### 5.4 CallTree — отложен в бэклог
- `backend/apps/query/src/features/cdt/calls-tree/` на jQuery-стайле. Отдельный большой эпик на потом.

---

## Stage 6. Деплой: StatefulSet + Headless Service + Helm (≈1–2 нед)

Цель: чарт, который разворачивает всё в k8s.

### 6.1 Манифесты
- StatefulSet для collector с `volumeClaimTemplates` (RWO), readiness probe, env `COLLECTOR_HEADLESS_SVC`.
- Headless Service (`clusterIP: None`) рядом со StatefulSet.
- Deployment для query, без PV.
- Deployment/CronJob для maintenance.
- MinIO — либо зависимость из внешнего values, либо ставим вместе.

### 6.2 Helm
- Обновить `backend/charts/profiler-stack/values.yaml`: убрать `INFRA_POSTGRES_*`, переконфигурировать под новую схему.
- Оставить только S3 (и S3 опционально, если в dev используется filesystem-эмулятор).

### 6.3 Readiness & HPA
- Readiness collector возвращает Ready только после восстановления WAL+spill.
- HPA на collector по CPU/memory (осторожно — sticky TCP означает, что новые реплики не сразу получат нагрузку; в первую итерацию можно без HPA).

---

## Cross-cutting концерны

Работы, которые идут параллельно основным стадиям и не укладываются в один этап.

### C1. Фильтрация на уровне агента
- Агент уже фильтрует одиночные вызовы ниже порога.
- Новая возможность: не писать целый call-tree, если корневой занял меньше порога. Владимир справедливо отметил, что если есть разные retention-политики по duration (Stage 2.2), то фильтрация на агенте может быть не нужна — короткие трейсы сами быстро удалятся. Делать, только если сетевой трафик агент→collector станет узким местом.

### C2. Runtime-конфигурация агента
- Менять фильтры/пороги/включение-выключение на лету через центральный endpoint и пушить в агент через существующий TCP-канал.
- Расширить `backend/apps/collector/src/main/java/com/netcracker/common/ProtocolConst.java` и `ProfilerAgentReader.java`.
- Можно делать независимо от Stages 1–6.

### C3. MCP / skills
- **Skill на скачивание диагностических дампов** через `dumps-collector` (endpoint’ы уже есть в `backend/apps/dumps-collector/pkg/server/http_server.go`). Быстрый win, можно сделать параллельно Stage 0–1.
- **MCP к данным профайлера** — поверх query API из Stage 0.2. Делать после Stage 4.
- Панкратов: «хороший машинно-читаемый API».

### C4. Метрики / Grafana
- Maintenance считает агрегаты (top-N медленных методов, p95 latency, счётчики по тегам) и экспортирует Prometheus-метрики.
- Инфра готова: `backend/libs/metrics/`.
- Grafana-дашборд поверх этих метрик — альтернатива отдельному analytics-экрану в UI. Проще и встраивается в существующий стек.

### C5. Dumps-collector + heap-дампы через S3
- Сейчас dumps-collector читает с RWX-PV. Переход на схему «агент по TCP → collector → S3 → dumps-collector читает из S3» снимает RWX. Менять только когда доберёмся до этого пункта — пока `dumps-collector` живёт независимо и работает.

---

## Принятые решения

1. **Contracts-first, без реанимации Postgres-пути.** Не делаем walking skeleton / golden output. Сначала контракты и диаграммы (Stage 0), потом сервисы по одному с интеграционным тестом.
2. **Rewriting Java collector на Go — откладываем.** Прямых выгод нет, работы много. Парсинг агентского TCP-протокола в `ProfilerAgentReader.java` переиспользуем без изменений.
3. **Raw-поток в Postgres в прод не пишем никогда.** Архитектура: in-memory aggregation + WAL словарей + spill-to-SQLite + parquet в S3.
4. **Два UI (профайлер и dumps-collector) — допустимо.** Унификация — не приоритет.
5. **RWX PV — не требуется.** Разделение состояния: общее → S3 (многочитательское, не RWX), per-replica hot state → RWO PV через StatefulSet, query — stateless без PV. Discovery — Headless Service + DNS.
6. **Query читает из двух слоёв (hot collector + cold S3).** Fan-out + merge + dedup по PK. Hot cutoff = flush interval.
7. **Миграция `@netcracker/*` → AntD — только в Stage 5.** До этого UI не трогаем.

---

## Приложение A. Карта соответствия «идея из обсуждения → код»

| Идея из митинга | Файлы/директории |
|---|---|
| Коллектор принимает поток от агента по TCP | `backend/apps/collector/src/main/java/com/netcracker/cdt/collector/tcp/ProfilerAgentReader.java`, `.../CollectorOrchestratorThread.java` |
| Коллектор пишет в Postgres «сырые» Call-3 + метаданные | `backend/apps/collector/src/main/java/com/netcracker/persistence/adapters/cloud/` |
| Temp-таблицы по 5 минут с дропом целиком | `backend/libs/pg/resources/schema/{calls,traces,suspend}_tables_template.gosql` и `backend/libs/pg/db.go` (`Granularity`, `TempTableLifetime`) |
| Maintenance-джоба: агрегация → parquet | `backend/apps/maintenance/pkg/maintenance/maintenance_job.go` + соседние |
| Разные диапазоны duration в parquet | `backend/libs/storage/parquet/file_map_calls.go` |
| Dumps-collector как пример all-in-one на SQLite | `backend/apps/dumps-collector/cmd/run.go`, `pkg/client/sqlite/`, `pkg/server/http_server.go` |
| UI на AntD + `@netcracker/ux-react` | `backend/apps/query/package.json`, `backend/apps/query/src/features/cdt/` |
| Helm-стек, требующий Postgres + S3 | `backend/charts/profiler-stack/values.yaml` |
| Сборка `collector`/`query` выключена | `backend/Makefile` — `APPS := dumps-collector maintenance` |

