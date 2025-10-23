import http from 'k6/http';
import {group, check, sleep, fail} from "k6";
import {vu} from "k6/execution";
import { TestOpts, suite } from "./common.js";

// http headers
export const uploadDumpRequestParams = {
    headers: {
        'Content-Type': 'application/octet-stream',
    }
};

// top & td dumps
//   emulating http requests:
//     PUT http://${__ENV.COLLECTOR_HOST}:8080/diagnostic/{NAMESPACE}/2023/08/04/16/46/32/{POD}_{RESTART}/20230804T164632.top.txt
export function dumps_sending() {
    
    sleep(20)
    const pod = suite.pod(vu.idInTest, "dumps");

    group("files. thread dump", function () {
        const path = pod.prepareDumpPath(new Date(), "td") // construct link (with pod params and current time)
        const url = `http://${TestOpts.host}:8080/diagnostic/${path}`;

        const arrayBuffer = new Uint8Array(pod.dumps.td.data)
        const resp = http.put(url, arrayBuffer, uploadDumpRequestParams);

        check(resp, { // collector should reply with 200 OK
            "no err": (r) => r.status === 200,
        });
    })
    sleep(20)

    group("files. top dump", function () {
        const path = pod.prepareDumpPath(new Date(), "top")
        const url = `http://${TestOpts.host}:8080/diagnostic/${path}`;

        const arrayBuffer = new Uint8Array(pod.dumps.top.data)
        const resp = http.put(url, arrayBuffer, uploadDumpRequestParams);

        check(resp, {
            "no err": (r) => r.status === 200,
        });
    })
    sleep(20)

    // emulator calls this function in loop until the end of the test
}

