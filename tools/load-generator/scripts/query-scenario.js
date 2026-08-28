// T6 background query load (load-testing-plan.md §7.6, runbook
// doc/soak-runs.md): read-side traffic against the query service's /api/v1,
// on the standard k6/http module of the same custom binary.
//
// Three profiles, each its own scenario, sized by env:
//   ui       — UI_VUS users looping the UI journey: list the last hour, open
//              a random call (trace + tree; there is no bare /calls/{pk}).
//   incident — INCIDENT_VUS users hammering wide ranges for INCIDENT_DURATION
//              out of every INCIDENT_PERIOD, idle in between.
//   cold     — COLD_VUS users issuing ranges just under the wide-range guard
//              and paging through every next_cursor; every page re-lists S3
//              by design.
//
// Windows are computed here and sent as integer Unix milliseconds — the API
// accepts nothing else. Guard rejections (HTTP 400) are the point of probing
// wide ranges: they land in the query_guard_rejected counter, not in
// http_req_failed.
//
// Profile knobs have NO defaults, same as scenario.js: every knob must be set
// in the deployment env (the k6query.workload map in the helm values), an
// unset knob fails the scenario at init, and setup() exports the resolved
// knobs as workload_info{knob,value} samples
// (doc/run-orchestration.md, "Workload wiring"). QUERY_URL, TESTID, and
// DURATION are plumbing and keep defaults.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Gauge } from 'k6/metrics';

// Guard rejections (400) and read-budget rejections (503) are probe results
// counted by classify(); without this callback k6 would also fold them into
// http_req_failed and taint the run verdict.
http.setResponseCallback(http.expectedStatuses(200, 400, 503));

const WORKLOAD_KNOBS = [
    'UI_VUS',
    'INCIDENT_VUS',
    'COLD_VUS',
    'UI_RANGE_MINUTES',
    'INCIDENT_PERIOD_MINUTES',
    'INCIDENT_DURATION_MINUTES',
    'WIDE_RANGE_MINUTES',
    'LIST_LIMIT',
    'THINK_SECONDS',
    'COLD_MAX_PAGES',
];

function knob(name) {
    const v = __ENV[name];
    if (v === undefined || v === '') {
        throw new Error(
            `workload knob ${name} is not set; the stand must pin every knob ` +
            `via k6query.workload in the helm values — silent defaults are ` +
            `forbidden (doc/run-orchestration.md)`);
    }
    return v;
}

function knobNum(name) {
    const v = Number(knob(name));
    if (Number.isNaN(v)) {
        throw new Error(`workload knob ${name}=${knob(name)} is not a number`);
    }
    return v;
}

function str(name, dflt) {
    const v = __ENV[name];
    return v === undefined || v === '' ? dflt : v;
}

const workload = {};
for (const name of WORKLOAD_KNOBS) {
    workload[name] = knob(name);
}

const BASE = str('QUERY_URL', 'http://profiler-backend-query.profiler-load.svc:8080');
const LIST_LIMIT = knobNum('LIST_LIMIT');
const THINK_SECONDS = knobNum('THINK_SECONDS');
// Wide-range probes sit just under PROFILER_WIDE_RANGE_LIMIT (default 6h)
// unless pushed over it on purpose.
const WIDE_RANGE_MINUTES = knobNum('WIDE_RANGE_MINUTES');
const COLD_MAX_PAGES = knobNum('COLD_MAX_PAGES');

const guardRejected = new Counter('query_guard_rejected');
const budgetRejected = new Counter('query_budget_rejected');
const partialResponses = new Counter('query_partial_responses');
const coldPages = new Counter('query_cold_pages');
const workloadInfo = new Gauge('workload_info');

// A profile with 0 VUs is omitted entirely (constant-vus rejects vus: 0).
// The soak companion runs ui (+ incident where the stand survives it); the
// cold-heavy profile is a dedicated probe run (doc/soak-runs.md).
function buildScenarios() {
    const out = {};
    const add = (name, exec, vus) => {
        if (vus > 0) {
            out[name] = {
                executor: 'constant-vus',
                exec,
                vus,
                duration: str('DURATION', '2h'),
            };
        }
    };
    add('ui', 'ui', knobNum('UI_VUS'));
    add('incident', 'incident', knobNum('INCIDENT_VUS'));
    add('cold', 'cold', knobNum('COLD_VUS'));
    return out;
}

export const options = {
    scenarios: buildScenarios(),
    tags: { testid: str('TESTID', 'query-dev') },
};

// The fingerprint: one sample per knob, the raw env string as the value
// label (doc/run-orchestration.md, "Workload wiring").
export function setup() {
    for (const name of WORKLOAD_KNOBS) {
        workloadInfo.add(1, { knob: name, value: workload[name] });
    }
}

function getCalls(fromMs, toMs, extra, tags) {
    let url = `${BASE}/api/v1/calls?from=${fromMs}&to=${toMs}&limit=${LIST_LIMIT}`;
    if (extra) {
        url += extra;
    }
    const res = http.get(url, { tags });
    classify(res);
    return res;
}

// classify folds the expected non-200s into the custom counters: a guard
// rejection (400) and a read-budget rejection (503, 02-read-contract.md
// §7.5) are probe results, not failures.
function classify(res) {
    if (res.status === 400) {
        guardRejected.add(1);
        return null;
    }
    if (res.status === 503) {
        budgetRejected.add(1);
        return null;
    }
    if (res.status !== 200) {
        return null;
    }
    const body = res.json();
    if (body && body.partial) {
        partialResponses.add(1);
    }
    return body;
}

// ui: list the trailing UI_RANGE_MINUTES (default: the last hour), open a
// random row (trace, then tree), think, loop. Accelerated-timer stands pack
// an hour's guard budget into minutes — shrink the range with the timers.
export function ui() {
    const now = Date.now();
    const res = getCalls(now - knobNum('UI_RANGE_MINUTES') * 60 * 1000, now, '', { profile: 'ui' });
    const body = res.status === 200 ? res.json() : null;
    const calls = body && body.calls ? body.calls : [];
    if (calls.length > 0) {
        const call = calls[Math.floor(Math.random() * calls.length)];
        openCall(call, { profile: 'ui' });
    }
    sleep(THINK_SECONDS);
}

function pkPath(pk) {
    const parts = [pk.pod_namespace, pk.pod_service, pk.pod_name,
        pk.restart_time_ms, pk.trace_file_index, pk.buffer_offset, pk.record_index];
    return encodeURIComponent(parts.join(':'));
}

function openCall(call, tags) {
    const hint = `ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`;
    const trace = http.get(`${BASE}/api/v1/calls/${pkPath(call.pk)}/trace?${hint}`, { tags });
    classify(trace);
    const tree = http.get(`${BASE}/api/v1/calls/${pkPath(call.pk)}/tree?${hint}`, { tags });
    classify(tree);
}

// incident: for INCIDENT_DURATION out of every INCIDENT_PERIOD, all VUs
// hammer wide ranges back to back; outside the burst they idle. Phase is
// wall-clock-based so every VU bursts together.
export function incident() {
    const periodMs = knobNum('INCIDENT_PERIOD_MINUTES') * 60 * 1000;
    const burstMs = knobNum('INCIDENT_DURATION_MINUTES') * 60 * 1000;
    const now = Date.now();
    if (now % periodMs >= burstMs) {
        sleep(5);
        return;
    }
    const wide = WIDE_RANGE_MINUTES * 60 * 1000;
    getCalls(now - wide, now, '', { profile: 'incident' });
    sleep(1);
}

// cold: a wide range paged to the end — every page re-resolves the fan-out
// and re-lists S3 (02 §2.3.1), which is exactly the cost this profile
// measures. A guard 400 on page one is a valid probe outcome.
export function cold() {
    const now = Date.now();
    const wide = WIDE_RANGE_MINUTES * 60 * 1000;
    let cursor = '';
    for (let page = 0; page < COLD_MAX_PAGES; page++) {
        const extra = cursor ? `&cursor=${encodeURIComponent(cursor)}` : '';
        const res = getCalls(now - wide, now, extra, { profile: 'cold' });
        if (res.status !== 200) {
            break;
        }
        coldPages.add(1);
        const body = res.json();
        if (!body || !body.next_cursor) {
            break;
        }
        cursor = body.next_cursor;
    }
    sleep(THINK_SECONDS);
}
