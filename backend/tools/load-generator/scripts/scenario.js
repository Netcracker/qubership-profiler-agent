import {scenario, vu} from "k6/execution";
import { TestOpts, suite } from "./common.js";

// use functions from other files
export { tcp_communication } from "./scenario.tcp.js";
export { tcp_long_communication } from "./scenario.tcp-long.js";
export { dumps_sending } from "./scenario.dumps.js";

// k6 options for test
export const options = {
    // run two different scenarios simultaneously
    scenarios: {
        // N pods send top+td dumps every minute
        sending_dumps: {
            executor: 'constant-vus',     // emulating N agent at start and keep in this way until end of test
            exec: 'dumps_sending',        // use function from 'scenario.dumps.js'

            vus: TestOpts.pods,           // VUs (virtual users) -- N parallel agents
            duration: TestOpts.duration,

            startTime: '15s',             // run this scenario not immediately at start, but after `15s` pause
        },
        tcp_communication: {
            executor: 'constant-vus',

            // We have two different versions of sending TCP data:
            // 1. `tcp_long_communication` - A more realistic scenario is when each vu (agent) connects to the collector
            //at the beginning of the test and sends calls repeatedly from within ONE connection
            //throughout the entire duration of the test.
            // 2. `tcp_communication` - Each vu (agent) connects to the collector, sends calls once,
            // closes the connection, and so on in a loop until the test ends.
            exec: 'tcp_long_communication',    // use function from 'scenario.tcp-long.js'

            vus: TestOpts.pods,
            duration: TestOpts.duration,

            startTime: '2s',             // run this scenario not immediately at start, but after `2s` pause

            gracefulStop: '10s',
        }
    },

    // Do not use the system "name" and "url" tags because they overload the Prometheus database
    systemTags: [
        'proto', 
        'subproto', 
        'status', 
        'method', 
        'group', 
        'check', 
        'error', 
        'error_code', 
        'tls_version', 
        'scenario', 
        'service', 
        'expected_response'
    ],

    tags: {
        'namespace': __ENV.K8S_NAMESPACE || 'localhost',
        'pod': __ENV.K8S_POD || 'localhost' 
    }
};

// print config data in generator console at start
export function setup() {
    // configuration
    console.log('vu: ' + vu.idInTest)
    console.log(`Collector host:    ${TestOpts.host}`);
}

// Useful linux utilities to check performance in terminal:
// CPU and memory: htop
// Network:        iftop
// See also:       nmon   ( https://nmon.sourceforge.net )

