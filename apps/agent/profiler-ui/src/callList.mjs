import 'slickgrid/dist/styles/css/slick.grid.css';

import { CL } from './profiler.mjs';
import './callPodFilter.mjs';
import './activePodPopup.mjs';
import './downloadDump.mjs';

function callsdata(id, calls, params){
    const data = {data: params, response: calls};
    if (app.inited) {
        CL.firstData(data);
    } else {
        app.data = data;
    }
}

window.callsdata = callsdata;

export { callsdata };

const head = document.getElementsByTagName("head")[0] || document.documentElement;
const script = document.createElement("script");
script.src = 'js/calls.js?' + location.hash.replace(/^#/,'') + '&callback=callsdata&id=0&clientUTC='+new Date().getTime();
head.insertBefore(script, head.firstChild);
