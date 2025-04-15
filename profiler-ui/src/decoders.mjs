import { ESCConstants } from "./ESCConstants.mjs";
import { escapeHTML } from "./utils.mjs";

const ESCDecoders = new function() {
    var C_PARAMS = ESCConstants.C_PARAMS;
    var T_TYPE_LIST = ESCConstants.C_PARAMS;
    var T_TYPE_ORDER = ESCConstants.C_PARAMS;

    //todo: check that now p comes as map of integer -> param, and
    function decoder_genericAddParams(callParams, tags, hidden, no_links) {
        var params = [], name;
        for (var k in callParams) {
            var info = tags.y[k];
            if (!info) continue;
            if (!info[T_TYPE_LIST]) continue;

            name = tags.strByIndex(k);
            if (hidden[name]) continue;
            params[params.length] = [info[T_TYPE_ORDER], name, k];
        }

        if (params.length === 0) return params;

        params.sort(function (a, b) {
            if (a[0] !== b[0]) return a[0] - b[0];
            return a[1] > b[1];
        });

        var res = [];
        for (var i = 0; i < params.length; i++) {
            var orderNameIndex = params[i];
            name = orderNameIndex[1];

            var paramValue = callParams[orderNameIndex[2]];
            var stringParamValue;
            if (paramValue instanceof Array) {
                stringParamValue = '['
                for (var i = 0; i < paramValue.length; i++) {
                    stringParamValue += tags.strByIndex(paramValue)
                    if (i < paramValue.length - 1) {
                        stringParamValue += ', '
                    }
                }
                stringParamValue += ']'
            } else {
                stringParamValue = tags.strByIndex(paramValue);
            }

            if (name === 'wf.process' && !no_links)
                stringParamValue = '<a target=_blank href="/tools/wf/wf_info.jsp?run=Run&id=' + stringParamValue + '">' + stringParamValue + '</a>';
            else if (name === 'po.process' && !no_links)
                stringParamValue = '<a target=_blank href="/ncobject.jsp?id=' + stringParamValue + '">' + stringParamValue + '</a>';
            else
                stringParamValue = escapeHTML(stringParamValue);
            res[i] = name + ': ' + stringParamValue;
        }

        return res;
    }
    this.decoder_genericAddParams = decoder_genericAddParams;

    var qrtzJob = (function () {
        var hiddenParams = {};
        var types = {
            '7020873015013388039': 'JMS',
            '7020873015013388040': 'URL',
            '7020873015013388043': 'SOAP',
            '7020873015013388041': 'EJB',
            '7020873015013388042': 'Class'
        };

        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;
            var r;
            var type = tags.r['job.action.type'];
            if (type) type = p[type];
            var x;
            if (type) {
                if (x = types[type])
                    type = x;
                r = type + ' quartz job';
            } else r = 'Quartz job';

            var id, name;
            if (id = tags.r['job.id']) id = p[id];
            if (name = tags.r['job.name']) name = p[name];
            if (id) {
                if (!name) name = 'Name unknown';
                if (no_links)
                    r += escapeHTML(name);
                else
                    r += ' <a href="/ncobject.jsp?id=' + id + '">' + escapeHTML(name) + '</a>';
            }

            if (type === 'Class')
                r += ' (' + p[tags.r['job.class']].replace(/(?:[^.]+\.)+/, '') + '.' + p[tags.r['job.method']] + ')';

            var params = decoder_genericAddParams(p, tags, hiddenParams, no_links);
            return r + ', ' + params.join('; ');
        }

    })();

    function qrtzTrigger() {
        return "Quartz triger";
    }

    function decoder_addParam(r, name, id, p, tags) {
        var tagId = tags.r[id];
        if (!tagId) return false;
        var val = p[tagId];
        if (!val) return false;
        if (val instanceof Array) {
            if (val.length === 0) return;
            val = '[' + val.join(', ') + ']';
        }
        r[r.length] = name;
        r[r.length] = escapeHTML(val);
        return true;
    }



    var http = (function () {
        var hiddenParams = {'jmeter.step': 1};
        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;
            var r = '';
            var jmeter = p[tags.r['jmeter.step']];
            if (jmeter)
                r = 'JMeter: <b>' + jmeter + '</b>, ';

            var uiComponent = p[tags.r['ui.component']];
            var url = p[tags.r['web.url']];
            if (uiComponent) {
                if (uiComponent instanceof Array) {
                    r += uiComponent.length + ' CBTUI actions ';
                    for (var i = 0; i < uiComponent.length; i++) {
                        r += escapeHTML(uiComponent[i].replace(/\/ \S+ (?=\/)/g, '')).replace(/localValue=((?:\S|(?!\s(?:\/|\S+=))\s)+)/g, '<b>$1</b>');
                    }
                } else
                    r = escapeHTML(uiComponent.replace(/\/ \S+ (?=\/)/g, '')).replace(/localValue=((?:\S|(?!\s(?:\/|\S+=))\s)+)/g, '<b>$1</b>');
            } else if (url) {
                var query = p[tags.r['web.query']];

                function trimUrl(url) {
                    return url.substr(url.indexOf('/', url.indexOf('://') + 3));
                }

                if (!(url instanceof Array) && !(query instanceof Array)) {
                    var method = p[tags.r['web.method']];
                    if (!(method instanceof Array)) {
                        r += method + ' ';
                    }
                    if (query)
                        url = trimUrl(url) + '?' + query;
                    r += escapeHTML(url)
                } else {
                    // multiple urls (e.g. server-side redirects)
                    r += url.length + ' pages: ';
                    for (var u = 0; u < url.length; u++)
                        url[u] = trimUrl(url[u]);
                    r += escapeHTML(url.join(', '));
                    if (query) {
                        if (query instanceof Array)
                            r += ', ' + query.length + ' query strings: ' + escapeHTML(query.join(', '));
                        else
                            r += ', query ' + escapeHTML(query);
                    }
                }
            }

            var remote = p[tags.r['web.remote.addr']];
            if (remote) {
                r += ', client: ';
                if (!(remote instanceof Array)) {
                    r += escapeHTML(remote);
                } else {
                    var seen = {};
                    for (var re = 0; re < remote.length; re++) {
                        var rmt = remote[re];
                        if (seen[rmt]) continue;
                        seen[rmt] = true;
                        if (re > 0) r += ', ';
                        r += escapeHTML(rmt);
                    }
                }

                var params = decoder_genericAddParams(p, tags, hiddenParams, no_links);
                return r + ' ' + params.join('; ');
            }
        }
    })();

    var orchestratorDecoder = (function () {
        var hiddenParams = {};
        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;
            var r = ['Orchestrator: '];
            decoder_addParam(r, '<b>', 'po.process.name', p, tags);
            r[r.length] = '</b>';

            var params = decoder_genericAddParams(p, tags, hiddenParams, no_links);
            if (params && params.length > 0) {
                r[r.length] = ' ';
                r[r.length] = params.join(', ');
            }
            decoder_addParam(r, ', text: ', 'jms.text.fragment', p, tags);
            return r.join('');
        }
    })();

    var jmsDecoder = (function () {
        var hiddenParams = {};
        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;

            var cons = tags.r['jms.consumer'];
            if (cons && p[cons] === 'OrchestrationQueueInvokerBean')
                return orchestratorDecoder(dataContext);

            var r = ['JMS: '];
            decoder_addParam(r, '<b>', 'jms.consumer', p, tags);
            r[r.length] = '</b>';

            var params = decoder_genericAddParams(p, tags, hiddenParams, no_links);
            if (params && params.length > 0) {
                r[r.length] = ' ';
                r[r.length] = params.join(', ');
            }
            decoder_addParam(r, ', destination: ', 'jms.destination', p, tags);
            decoder_addParam(r, ', text: ', 'jms.text.fragment', p, tags);
            return r.join('');
        }
    })();

    var dataflowDecoder = (function () {
        function renderDfSession(r, s) {
            if (!s) {
                return;
            }
            if (s.toString() === '::other') {
                r[r.length] = s;
                return;
            }
            var val = JSON.parse('{' + s + '}');
            var stealthMode = !(val.i && val.i.length === 19);
            if (!stealthMode || val.c) {
                r[r.length] = '<a target=_blank href="/ncobject.jsp?id=';
                r[r.length] = stealthMode ? val.c : val.i;
                r[r.length] = '">';
            }
            r[r.length] = val.n ? escapeHTML(val.n) : (val.i + ":" + val.c);
            if (!stealthMode || val.c) {
                r[r.length] = ' (';
                r[r.length] = stealthMode ? 'configuration' : 'instance';
                r[r.length] = ')</a>';
            }
            if (!stealthMode && val.c) {
                r[r.length] = ', <a target=_blank href="/ncobject.jsp?id=';
                r[r.length] = val.c;
                r[r.length] = '">open configuration</a>';
            }
        }

        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;

            var r = ['DataFlow: '];
            var sessions = p[tags.r['dataflow.session']];
            if (sessions instanceof Array) {
                for (var i = 0; i < sessions.length; i++) {
                    if (i !== 0) {
                        r[r.length] = ', ';
                    }
                    renderDfSession(r, sessions[i]);
                }
            } else {
                renderDfSession(r, sessions);
            }
            return r.join('');
        }
    })();

    var aiDecoder = (function () {
        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;

            var r = ['AutoInstaller: '];
            decoder_addParam(r, '<b>', 'ai.package', p, tags);
            r[r.length] = '</b>';
            decoder_addParam(r, ', patch: ', 'ai.zip', p, tags);
            return r.join('');
        }
    })();

    var brockerDecoder = (function () {
        return function (dataContext, no_links, tags) {
            var p = dataContext[C_PARAMS];
            if (!p) return;
            var queue = p[tags.r['queue']];
            var url = p[tags.r['rabbitmq.url']];
            return '<b>RabbitMQ Url:</b> ' + url + ' , queue: ' + queue;
        }
    })();

    var decoders = []

    decoders['NioEventLoop.runAllTasks'] = [http];
    decoders['has.url'] = [http];
    decoders['NioEventLoop.processSelectedKeys'] = [http];
//apache + spring.webapplicationtype=reactive
    decoders['SocketProcessorBase.run'] = [http];
//async threads of apache + spring.webapplicationtype=servlet
    decoders['TraceRunnable.run'] = [http];

    decoders['WebAppServletContext$ServletInvocationAction.run'] = [http];
    decoders['ServletRequestImpl.run'] = [http];
    decoders['ServletRequestImpl.execute'] = [http];
    decoders['StandardEngineValve.invoke'] = [http];
    decoders['FilterHandler$FilterChainImpl.doFilter'] = [http];
    decoders['Connectors.executeRootHandler'] = [http]; // Undertow
    decoders['ServletHandler.doHandle'] = [http]; // Jetty


    decoders['NCJobStore.triggerFired'] = [qrtzTrigger];

    decoders['JobRunShell.run'] = [qrtzJob];

    decoders['MDListener.run'] = decoders['MDListener.execute'] = decoders['MDListener.onMessage'] =
        decoders['JMSSession.onMessage'] = decoders['JMSSession$UseForRunnable.run'] = [jmsDecoder];
    decoders['ClientConsumerImpl.callOnMessage'] = [jmsDecoder];
    decoders['AbstractMessageListenerContainer.doExecuteListener'] = [jmsDecoder];
    decoders['ActiveMQMessageHandler.onMessage'] = decoders['JMSMessageListenerWrapper.onMessage'] = [jmsDecoder];
    decoders['DataFlowAwareRunnable.run'] = [dataflowDecoder];
    decoders['Main.runBuild'] = [aiDecoder];
    decoders['MessagingMessageListenerAdapter'] = [brockerDecoder];

    this.decoders = decoders;
}

export { ESCDecoders };
