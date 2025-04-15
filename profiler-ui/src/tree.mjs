import { CT } from './profiler.mjs';
import './activate_ide.mjs';
import 'code-prettify/styles/desert.css';
import 'code-prettify/loader/prettify.js';
import 'code-prettify/loader/lang-sql.js';

function treedata(id, tree){
    if (app.inited) {
        CT.render(tree());
    } else {
        app.data = tree;
    }
}

window.treedata = treedata;

export { treedata };

const paramUrl = window.location.hash.replace(/^#/, '');
const d = document.getElementById('download');
app.dn = new Date().getTime() + '_tree.zip';
if (paramUrl && (paramUrl.indexOf('i=') !== -1 || paramUrl.indexOf('i[]=') !== -1 || paramUrl.indexOf('i%5B%5D=') !== -1)) {
    app.du = paramUrl + '&callback=treedata';
    d.href = 'tree/' + app.dn + '?' + app.du;
} else {
    d.style.display = 'none';
}

if (location.href.indexOf('tree.html') !== -1) {
    const head = document.getElementsByTagName("head")[0] || document.documentElement;
    const script = document.createElement("script");
    script.src = 'js/tree.js?' + location.hash.replace(/^#/, '') + '&callback=treedata&id=0&clientUTC=' + new Date().getTime();
    head.insertBefore(script, head.firstChild);
}
