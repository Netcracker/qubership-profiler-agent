/*

 * Copyright 2000-2008 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function Activation_bind(func, scope) {
    return function() {
        func.apply(scope, arguments);
    }
}

function XSS(id, url) {
    this.id = id;
    this.url = url + "&noCache=" + (new Date()).getTime();
}

XSS.prototype.done = function(color) {
    this.indicator.style.backgroundColor = color;
    if (this.oncomplete) this.oncomplete(this.status);
};

function dump(o) {
    var s = "";
    for(var p in o) {
        if(!/on.*|inner.*|outer.*|src/.test(p))
            s+=p+"="+o[p]+"\t";
    }
    alert(s);
}

XSS.prototype.doneR = function() {
    if(navigator.userAgent.indexOf('Opera')>=0 || /loaded|complete/.test(this.transport.readyState)) {
        this.status = 1;
        this.doneL();
    }
};

XSS.prototype.doneL = function() {
    this.status = this.transport.width;
    if(this.status>1) {
        this.done('green');
    } else {
        this.done('orange');
    }
};

XSS.prototype.doneE = function() {
    this.status = -1;
    this.done('silver');
};

XSS.prototype.go = function() {
    this.transport = document.getElementById(this.id);
    this.parent = document.getElementById(Activator.PANELID);
    if (this.transport) {
        this.dispose();
    }
    this.transport = new Image();
    this.transport.id = this.id;
    if(navigator.userAgent.indexOf('Opera')>=0 || navigator.userAgent.indexOf('MSIE')>=0 ) {
        this.transport.onreadystatechange = Activation_bind(this.doneR, this);
    } else {
        this.transport.onload = Activation_bind(this.doneL, this);
    }
    this.transport.onerror = Activation_bind(this.doneE, this);
    this.transport.onabort = Activation_bind(this.doneE, this);
    this.transport.style.display = 'none';
    this.indicator = document.createElement("div");
    this.indicator.className='activationIndicator';
    this.indicator.appendChild(this.transport);
    this.parent.appendChild(this.indicator);

    this.transport.src = this.url; // start loading image
};

XSS.prototype.dispose = function() {
    if(navigator.userAgent.indexOf('Opera')>=0 || navigator.userAgent.indexOf('MSIE')>=0 ) {
        this.transport.onreadystatechange = function() {};
    } else {
        this.transport.onload = function() {};
    }
    this.transport.onerror = function() {};
    this.transport.onabort = function() {};
    if (this.transport.parentNode) {
        this.transport.parentNode.removeChild(this.transport);
    }
    if (this.indicator && this.indicator.parentNode) {
        this.indicator.parentNode.removeChild(this.indicator);
    }
};

function XSSBroadcast(command) {
    this.panel = document.getElementById(Activator.PANELID);
    if(!this.panel) {
        this.parent = document.getElementsByTagName("body").item(0);
        this.panel = document.createElement("div");
        this.panel.className = Activator.PANELID;
        this.panel.setAttribute("id", Activator.PANELID);
        this.parent.appendChild(this.panel);
        this.panel.style.display = 'block';
    }

    this.sequental = false;// (navigator.userAgent.indexOf('WebKit')>0);
    this.id = "id_" + Math.random();

    var START_PORT = 63340;
    var END_PORT = START_PORT + 9;
    var HOST = "http://127.0.0.1";

    this.responseCount = 0;
    this.successes = 0;
    this.connects = 0;
    this.requests = new Array(END_PORT - START_PORT);
    var oncomplete = Activation_bind(this.done, this);
    for (var port = START_PORT; port <= END_PORT; port++) {
        var uri = HOST + ":" + port + "/api/" + command;
        var xss = new XSS("r_" + this.id + "_" + port, uri);
        xss.oncomplete = oncomplete;
        this.requests[port - START_PORT] = xss;
        if(!this.sequental) xss.go();
    }
    this.index = 0;
    if(this.sequental) this.go();
}

XSSBroadcast.prototype.go = function() {
    this.requests[this.index++].go();
};

XSSBroadcast.prototype.done = function(status) {
    this._processingResponse = true;
    if (this._timeout) {
        clearTimeout(this._timeout);
    }
    this.responseCount++;
    if (status>=0) {
        this.connects++;
    }
    if (status>1) {
        this.successes++;
    }

    if(this.responseCount >= this.requests.length) {
        this._timeout = setTimeout(Activation_bind(this.notify, this), 1);
    }
    this._processingResponse = false;
    if(this.sequental && (this.index < this.requests.length)) {
        this.go();
    }
};

XSSBroadcast.prototype.notify = function() {
    if (this._processingResponse) return;

    if(this.successes==0) {
        if (this.connects == 0) {
            // img.onload does not work in Chrome for some reason
            // app.notify.notify('create', 'jqn-error', {title: 'Open in IDE',
            //         text:"No IDE responded.\n\nMake sure REST API is enabled: https://habrahabr.ru/post/315690/"},
            //     {expires: 15000, custom: true});
        } else {
            app.notify.notify('create', 'jqn-error', {title: 'Open in IDE',
                    text: "IDE cannot locate the requested file."},
                {expires: 5000, custom: true});
        }
    } else {
        app.notify.notify('create', 'jqn-notice', {title: 'Open in IDE',
                text: "File was successfully located" + (this.successes > 1 ? " in " + this.successes + " IDEs" : "")},
            {expires: 5000, custom: true});
    }
    this.panel.parentNode.removeChild(this.panel);
    this._timeout = null;

    for (var i=0; i<this.requests.length; i++) {
        this.requests[i].oncomplete = function() {};
        this.requests[i].dispose();
    }
};

var Activator = {};
Activator.PANELID = "activationPanel";
Activator.doOpen = function (command) {
    new XSSBroadcast(command);
};

export { Activator };
