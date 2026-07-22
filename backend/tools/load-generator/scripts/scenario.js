// Fleet scenario for the ceiling campaign (load-testing-plan.md §7).
//
// The externally-controlled executor starts at 0 VUs; the run orchestrator
// (tools/load-generator/runner) scales VUs over the k6 REST API, so ramp
// steps keep existing connections alive. Each VU holds one fleet of
// PODS_PER_VU virtual dumpers until it is scaled away: 1 pod per VU for the
// T2 throughput runs, ~100 idle pods per VU for the T3 connection runs.
//
// Workload knobs have NO defaults: every knob must be set in the deployment
// env (the k6.workload map in the helm values), and an unset knob fails the
// scenario at init — a misconfigured stand crash-loops instead of sending a
// silently different load (doc/run-orchestration.md, "Workload wiring").
// setup() exports the resolved knobs as workload_info{knob,value} samples so
// the runner can verify the frozen spec against the running deployment.
// Plumbing (endpoints, TESTID, MAX_VUS, DURATION) keeps defaults — it caps or
// labels the run but does not shape the traffic. Explicit zeros are honored —
// T3 sets THREADS_PER_POD=0 for keep-alive-only pods.
import cdt from 'k6/x/cdt';
import { Gauge } from 'k6/metrics';

// Every traffic-shaping knob (load-testing-plan.md §4). Order matters only
// for readability; the runner compares the set, not the order.
const WORKLOAD_KNOBS = [
    'PODS_PER_VU',
    'THREADS_PER_POD',
    'CALLS_PER_SEC',
    'DICT_INITIAL',
    'DICT_GROWTH_PER_MIN',
    'DURATION_THRESHOLDS',
    'DURATION_SHARES',
    'STACK_DEPTH',
    'SQL_SHARE',
    'SQL_BYTES',
    'SQL_DEDUP',
    'XML_SHARE',
    'XML_BYTES',
    'SUSPEND_RATE',
    'ERROR_SHARE',
    'REQUEST_ID_SHARE',
    'CPU_FRACTION',
    'WAIT_FRACTION',
    'MEMORY_BYTES',
    'SEED',
    'START_SPREAD',
    'RESTART_INTERVAL',
    'CHURN_INTERVAL',
];

function knob(name) {
    const v = __ENV[name];
    if (v === undefined || v === '') {
        throw new Error(
            `workload knob ${name} is not set; the stand must pin every knob ` +
            `via k6.workload in the helm values — silent defaults are forbidden ` +
            `(doc/run-orchestration.md)`);
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

function num(name, dflt) {
    const v = __ENV[name];
    return v === undefined || v === '' ? dflt : Number(v);
}

function str(name, dflt) {
    const v = __ENV[name];
    return v === undefined || v === '' ? dflt : v;
}

// Validate at init: a missing knob must abort the k6 process, not the first
// iteration.
const workload = {};
for (const name of WORKLOAD_KNOBS) {
    workload[name] = knob(name);
}

const workloadInfo = new Gauge('workload_info');

export const options = {
    scenarios: {
        fleet: {
            executor: 'externally-controlled',
            vus: 0,
            maxVUs: num('MAX_VUS', 600),
            duration: str('DURATION', '2h'),
        },
    },
    // The run label: keeps this run's series apart in VictoriaMetrics
    // (doc/run-orchestration.md).
    tags: { testid: str('TESTID', 'dev') },
};

// The fingerprint the runner verifies against the frozen spec: one sample per
// knob, the raw env string as the value label. setup() runs once per test,
// including at 0 VUs under the externally-controlled executor.
export function setup() {
    for (const name of WORKLOAD_KNOBS) {
        workloadInfo.add(1, { knob: name, value: workload[name] });
    }
}

export default function () {
    const summary = cdt.runFleet({
        addr: `${str('COLLECTOR_HOST', 'localhost')}:${num('COLLECTOR_PORT', 1715)}`,
        pods: knobNum('PODS_PER_VU'),
        namespace: str('EMULATOR_NAMESPACE', 'load'),
        service: str('EMULATOR_SERVICE', 'load-svc'),
        podPrefix: str('EMULATOR_POD_PREFIX', ''),
        seed: knobNum('SEED'),
        startSpread: knob('START_SPREAD'),
        restartInterval: knob('RESTART_INTERVAL'),
        churnInterval: knob('CHURN_INTERVAL'),

        threadsPerPod: knobNum('THREADS_PER_POD'),
        callsPerSec: knobNum('CALLS_PER_SEC'),
        dictInitial: knobNum('DICT_INITIAL'),
        dictGrowthPerMin: knobNum('DICT_GROWTH_PER_MIN'),
        durationThresholds: knob('DURATION_THRESHOLDS'),
        durationShares: knob('DURATION_SHARES'),
        stackDepth: knobNum('STACK_DEPTH'),
        sqlShare: knobNum('SQL_SHARE'),
        sqlBytes: knobNum('SQL_BYTES'),
        sqlDedup: knobNum('SQL_DEDUP'),
        xmlShare: knobNum('XML_SHARE'),
        xmlBytes: knobNum('XML_BYTES'),
        suspendRate: knobNum('SUSPEND_RATE'),
        errorShare: knobNum('ERROR_SHARE'),
        requestIdShare: knobNum('REQUEST_ID_SHARE'),
        cpuFraction: knobNum('CPU_FRACTION'),
        waitFraction: knobNum('WAIT_FRACTION'),
        memoryBytes: knobNum('MEMORY_BYTES'),
    });
    console.log(`fleet done: ${JSON.stringify(summary)}`);
}
