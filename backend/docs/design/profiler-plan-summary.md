План записан в profiler-plan.md. Ниже — главные находки из анализа кода, которые могут скорректировать вашу картину (некоторые из них расходятся с тем, что обсуждалось на созвоне):

1. «Все три на Go» — неверно для текущего состояния
- backend/apps/collector — Java 21 / Quarkus (Maven, pom.xml). Он и принимает TCP-трафик от агента на порт 1715.
- backend/apps/maintenance и backend/apps/dumps-collector — Go.
- backend/apps/query — TypeScript/React + AntD 4.24.

2. collector и query уже выключены из общей сборки
   В backend/Makefile явно: APPS := dumps-collector maintenance. То есть make-таргет build-all сейчас собирает только половину стека. Это нужно чинить первым шагом.

3. dumps-collector — готовый образец «all-in-one» идеи
   Он использует glebarez/sqlite (встроенная SQLite), только PV, никакого Postgres. Паттерн oklog/run в cmd/run.go (parallel cron-задачи + HTTP-сервер в одном процессе) — шаблон для всех остальных.

4. Что именно пишется в Postgres (отвечает libs/pg/resources/schema/traces_tables_template.gosql)
   В traces_<ts> лежит trace bytea — то есть сырые байты трейса, не только метаданные. Плюс декодированные calls_<ts>. Это подтверждает опасения Владимира — такой объём сырых данных нельзя держать на одном
   нешардированном Postgres под продовой нагрузкой.

5. Разные диапазоны duration в parquet — частично уже сделано
   backend/libs/storage/parquet/file_map_calls.go создаёт файлы по диапазонам duration. Не сделаны только разные ротации/TTL — это небольшая доработка в maintenance.

6. Sticky sessions TCP → можно безопасно агрегировать в памяти
   Алексей прав: TCP-коннект живёт до часа без сессии, агент стабильно попадает в один и тот же коллектор. Значит, in-memory агрегация реально применима — при условии WAL для словарей, иначе рестарт пода теряет
   контекст.

7. Пакеты @netcracker/* в query/package.json
   Реальные зависимости: @netcracker/cse-ui-components 2.1, @netcracker/ux-react 4.5, @netcracker/ux-assets 1.1. Все из архивированного репозитория. Плюс в Dockerfile коллектора стоит
   CP_UI=https://github.com/pd.sandbox.npm.staging/... — наверняка битая ссылка.

В плане явно выделены 5 открытых вопросов, ответы на которые нужны, прежде чем начинать двигаться (переписывать ли collector на Go сейчас; объединять ли UI профайлера и dumps-collector; что делать с архивным
npm-реестром и т.д.). Хорошо бы пройтись по ним прежде, чем раскладывать задачи по неделям.
