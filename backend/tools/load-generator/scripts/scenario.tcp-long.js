import { group, check, sleep } from "k6";
import collector from 'k6/x/cdt';
import {vu} from "k6/execution";
import { TestOpts, suite } from "./common.js";

const testStart = Date.now();
const testDurationMs = collector.parseDuration(TestOpts.duration);

// tcp protocol
export function tcp_long_communication() {
    const pod = suite.pod(vu.idInTest, "tcp");
    
    let ok = true;
    const client = collector.prepare(TestOpts);

    const tcpFile = pod.dumps.tcp;

    group("tcp. init connect", function () {
        // connect to CDT Collector
        ok = client.pass(client.connect(pod.pod_name))
        console.log('tcp vu: ' + vu.idInTest + ", pod " + pod.pod_name)
        check(ok, {
            "connected": (r) => r,
        });
        if (ok) {
            ok = client.pass(client.commandGetProtocolVersion(tcpFile.protocol_version,
                pod.namespace, pod.service, pod.pod_name))
        }
    })
    if (!ok) return;

    group("tcp. send meta", function () {
        check(client.pass(client.sendChunk(0, tcpFile.paramsChunk())), {
            "send pod params": (r) => r,
        });

        check(client.pass(client.sendChunk(0, tcpFile.dictionaryChunk())), {
            // client.sendDictionary(0, bDict, 1000, 10000);
            "send pod dictionary": (r) => r,
        });

        // client.sendChunk(0, bXml);
        // client.sendChunk(0, bSql);
    })

    group("tcp. send calls", function () {
        // check(client.pass(client.sendTraces(0, tcpFile.latestTraceChunk(), '30s')), {
        //     "send pod traces": (r) => r,
        // });
        // sleep(5)
        //check(client.pass(client.sendCallsAsNow(0, tcpFile.latestCallsChunk(), '5m')), {

        let requestedSeqId = 0;
        while (true) {
            const elapsed = Date.now() - testStart;
            const timeLeft = testDurationMs - elapsed;

            if (timeLeft <= 0) {
                break;
            }

            check(client.pass(client.sendCallsAsNow(requestedSeqId, tcpFile.latestCallsChunk(), '5m')), {
                "send pod calls": (r) => r,
            });
            requestedSeqId += 1;
        }
    })

    group("tcp. flush", function () {
        check(client.pass(client.commandRequestFlush()), {
            "send cmd flush": (r) => r,
        });
        check(client.pass(client.commandClose()), {
            "send close command": (r) => r,
        });

        // After sending COMMAND_REQUEST_ACK_FLUSH, the server attempts to send a response to the agent.
        // In the current version of the commandRequestFlush function, we do not wait for a response,
        // so an error on the side of server occurs when we subsequently attempt to close the connection.
        // To fix this, there is a 100 ms delay here, which allows the server to send data.
        // TODO: If errors occur related to this in the future, it may be necessary to change commandRequestFlush
        //  so that it waits for a response from the server.
        sleep(0.1)
        check(client.pass(client.close()), {
            "close pod connection": (r) => r,
        });
    })
}
