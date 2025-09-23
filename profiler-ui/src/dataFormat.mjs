import { default as moment } from 'moment';
import { escapeHTML } from './utils.mjs';
import { ESCConstants } from "./ESCConstants.mjs";
import { ESCDecoders } from "./decoders.mjs";
import { ESCProfilerSettings } from "./profilerSettings.mjs";
import { default as jQuery } from 'jquery';
import './jquery-browser.mjs';

const ESCDataFormat = new function() {
    var self = this;

    var C_TIME = ESCConstants.C_TIME;
    var C_DURATION = ESCConstants.C_DURATION;
    var C_QUEUE_WAIT_TIME = ESCConstants.C_QUEUE_WAIT_TIME;
    var C_SUSPENSION = ESCConstants.C_SUSPENSION;
    var C_FOLDER_ID = ESCConstants.C_FOLDER_ID;
    var C_ROWID = ESCConstants.C_ROWID;
    var C_PARAMS = ESCConstants.C_PARAMS;
    var C_TITLE_HTML = ESCConstants.C_TITLE_HTML;
    var C_TITLE_HTML_NOLINKS = ESCConstants.C_TITLE_HTML_NOLINKS;

    var T_CLASS = ESCConstants.T_CLASS;
    var T_METHOD = ESCConstants.T_METHOD;

    var decoder_genericAddParams = ESCDecoders.decoder_genericAddParams;
    var decoders = ESCDecoders.decoders;

    var profiler_settings = ESCProfilerSettings.profiler_settings;
    var browser = jQuery.browser;

    self.idleTags = {};
    self.idleTags['com.netcracker.ejb.cluster.messages.MessageThread.run'] = true;
    self.idleTags['com.netcracker.ejb.cluster.DatabaseThread.run'] = true;
    self.idleTags['com.netcracker.ejb.cluster.NodeManagerThread.run'] = true;
    self.idleTags['com.netcracker.ejb.cluster.NotificationThread.run'] = true;
    self.idleTags['com.netcracker.ejb.cluster.RecoveryThread.run'] = true;
    self.idleTags['com.netcracker.platform.scheduler.impl.ncjobstore.NCJobStore$RecoveryLockManager.run'] = true;
    self.idleTags['com.netcracker.mediation.dataflow.impl.util.trigger.socket.SocketListenerThread.run'] = true;
    self.idleTags['com.netcracker.mediation.dataflow.impl.util.recovery.RecoveryThread.run'] = true;
    self.idleTags['netscape.ldap.LDAPConnThread.run'] = true;
    self.idleTags['org.quartz.impl.jdbcjobstore.JobStoreSupport$MisfireHandler.run'] = true;
    self.idleTags['org.quartz.impl.jdbcjobstore.JobStoreSupport$ClusterManager.run'] = true;
    self.idleTags['org.quartz.core.QuartzSchedulerThread.run'] = true;
    self.idleTags['oracle.jms.AQjmsConsumer.receiveFromAQ'] = true;
    self.idleTags['org.apache.tools.ant.taskdefs.StreamPumper.run'] = true;
    self.idleTags['weblogic.jms.bridge.internal.MessagingBridge.run'] = true;

    var GRAY_START, GRAY_END;
    var RED_START, RED_END;
    if (browser['msie'] || browser['mozilla']) {
        GRAY_START = '<font color=gray>';
        GRAY_END = '</font>';
        RED_START = '<font color=red style="font-weight:bold;">';
        RED_END = '</font>';
    } else {
        // GRAY_START = '<s>';
        // GRAY_END = '</s>';
        // RED_START = '<ins>';
        // RED_END = '</ins>';
        GRAY_START = '<span class=\\"GrayNumber\\">';
        GRAY_END = '</span>';
        RED_START = '<span class=\\"RedNumber\\">';
        RED_END = '</span>';
    }

    function lpad2(num) {
        return num < 10 ? '0' + num : num;
    }

    self.Duration__formatTimeHMS = function(time) {
        if (time < Number(profiler_settings.omit_ms))
            return profiler_settings.millis_format === '0_400s' ? (time / 1000).toFixed(3) + 's' : Math.round(time) + 'ms'
        if (time < 1000000) return Math.round(time / 1000) + "s";
        if (time < 6000000) {
            time = Math.round(time / 1000);
            var ss = time % 60;
            time = (time - ss) / 60;
            return time + "m " + ss + "s";
        }
        var mm, hh;
        time = Math.round(time / 60000);
        if (time < 6000) {
            mm = time % 60;
            time = (time - mm) / 60;
            return time + "h " + mm + "m";
        }
        mm = time % 60;
        time = (time - mm) / 60;
        hh = time % 24;
        time = (time - hh) / 24;
        return time + "d " + lpad2(hh) + ":" + lpad2(mm);
    }

//when viewing a tree, duration needs to be displayed in percents of full duration.
// where full duration of a call T is 100%
// hence time_k = 100 / T
// this field will be set by the tree when viewing a call when percent mode is enabled. otherwise no need
    self.time_k = undefined;

    self.updateFormatFromPersonalSettings = function() {
        if (window.app && window.app.durationFormat === 'BYTES') {
            self.Duration__formatTime = self.AllocBytes__format;
        } else if (window.app && window.app.durationFormat === 'SAMPLES') {
            if (profiler_settings.threaddump_format === 'pct')
                self.Duration__formatTime = function (time) {
                    return (time * self.time_k).toFixed(4) + '%';
                }
            else if (profiler_settings.threaddump_format === 'cnt')
                self.Duration__formatTime = self.BigInteger__formatCalls;
            else { // format threaddump duration with seconds
                var millis_per_dump = Number(profiler_settings.thr_stack_duration);
                self.Duration__formatTime = function (time) {
                    return self.Duration__formatTimeHMS(time * millis_per_dump);
                }
            }
        } else {
            self.Duration__formatTime = self.Duration__formatTimeHMS;
        }
    }

    self.updateFormatFromPersonalSettings();

    function generateIntFormattingFunction(colorize, magnitude, unit, maxNonRedValue) {
        if (!unit) unit = '';
        var jsCode;
        if (profiler_settings.int_format === '1_234') {
            jsCode = "value = Math.round(value);" +
                "if (value < 1000) return " + (colorize ? "'" + GRAY_START + "'+" : "") + "value" + (colorize || unit ? "+' " : "") + unit + (colorize ? GRAY_END : "") + (colorize || unit ? "'" : "") + ";\n" +
                "var ms = value%1000; value = (value - ms) / 1000; " +
                (maxNonRedValue && colorize ? " if (value < " + (maxNonRedValue / 1000).toFixed(0) + ") " +
                        "     return value.toFixed(0) + \"'\" + profiler_lpad(ms, 3) " + (unit ? "+' " + unit + "'" : "") + " ; " : ""
                ) +
                " if (value < 100) " +
                "     return value.toFixed(0) + \"'\" + profiler_lpad(ms, 3) " + (unit ? "+' " + unit + "'" : "") + ";\n" +
                " if (value < 1000) " +
                "     return " + (colorize ? "'" + RED_START + "'+" : "") + "value.toFixed(0) + \"'\" + profiler_lpad(ms, 3) " + (colorize || unit ? "+' " : "") + unit + (colorize ? RED_END : "") + (colorize || unit ? "'" : "") + ";\n" +
                "var s = value%1000; value = (value - s) / 1000; " +
                " if (value < 1000) " +
                "     return " + (colorize ? "'" + RED_START + "'+" : "") + "value.toFixed(0) + \"'\" + profiler_lpad(s, 3) + \"'\" + profiler_lpad(ms, 3)" + (colorize || unit ? "+' " : "") + unit + (colorize ? RED_END : "") + (colorize || unit ? "'" : "") + ";\n" +
                "return " + (colorize ? "'" + RED_START + "'+" : "") + "(value/1000).toFixed(0) + \"'\" + profiler_lpad(value%1000, 3) + \"'\" + profiler_lpad(s, 3) + \"'\" + profiler_lpad(ms, 3)" + (colorize || unit ? "+' " : "") + unit + (colorize ? RED_END : "") + (colorize || unit ? "'" : "") + "; ";
        } else {
            var i = magnitude === 1024 ? 'i' : '';
            jsCode = "value = Math.round(value); " +
                " if (value < " + 100 * magnitude + ") " +
                "     return " + (colorize ? "'" + GRAY_START + "'+" : "") + "value" + (colorize || unit ? "+' " : "") + unit + (colorize ? GRAY_END : "") + (colorize || unit ? "'" : "") + ";\n" +
                (maxNonRedValue && colorize ? " if (value < " + maxNonRedValue + ") " +
                        "     return Math.round(value / " + magnitude + ") + ' K" + unit + "'; " : ""
                ) +
                " if (value < " + 10 * magnitude * magnitude + ") " +
                "     return " + (colorize ? "'" + RED_START + "'+" : "") + "Math.round(value / " + magnitude + ") + ' K" + i + unit + (colorize ? RED_END : "") + "';\n" +
                " if (value < " + 10 * magnitude * magnitude * magnitude + ") " +
                "     return " + (colorize ? "'" + RED_START + "'+" : "") + "Math.round(value / " + magnitude * magnitude + ") + ' M" + i + unit + (colorize ? RED_END : "") + "'\n" +
                " return " + (colorize ? "'" + RED_START + "'+" : "") + "Math.round(value / " + magnitude * magnitude * magnitude + ") + ' G" + i + unit + (colorize ? RED_END : "") + "';";
        }
        return new Function("value", jsCode);
    }

    self.Integer__format = generateIntFormattingFunction(false, 1000);
    self.BigInteger__format = generateIntFormattingFunction(true, 1000);
    self.BigInteger__formatCalls = generateIntFormattingFunction(true, 1000, '', 500000);
    self.Bytes__format = generateIntFormattingFunction(true, 1024, 'B', 200 * 1024);
    self.NetBytes__format = generateIntFormattingFunction(true, 1024, 'B', 1024 * 1024);
    self.AllocBytes__format = generateIntFormattingFunction(true, 1024, 'B', 1024 * 1024 * 1024);
    self.Bytes__formatNoColor = generateIntFormattingFunction(false, 1024, 'B');

    function titleFormatter(row, cell, value, columnDef, dataContext) {
        return row + " " + cell + " " + JSON.stringify(columnDef)
    }

    self.format_pod_name = function (podInfo) {
        return function ( row, cell, value, columnDef, dataContext) {
            var podName = podInfo[dataContext[C_FOLDER_ID]].name;
            if (typeof podName === 'undefined' || podName === null) {
                return podName;
            }
            //if it's like 'clust1_1989/2021/10/07/1633613813589'
            if (podName.match(/^[^\/]+\/\d{4}\/(0[1-9]|1[0-2])\/(0[1-9]|[12][0-9]|3[01])\/\d{13}$/) != null) {
                var idx = podName.indexOf('/');
                return podName.substring(0, idx);
            }
            return podName;
        }
    }

    self.format_service_name = function (podInfo) {
        return function (row, cell, value, columnDef, dataContext) {
            return podInfo[dataContext[C_FOLDER_ID]].serviceName;
        }
    }

    self.format_namespace = function (podInfo) {
        return function (row, cell, value, columnDef, dataContext) {
            return podInfo[dataContext[C_FOLDER_ID]].namespace;
        }
    }

    self.formatDateByMask = function(value, cutOutMask, fullMask) {
        var curDateStr = moment().format(cutOutMask);
        var valueStr = moment(value).format(fullMask)
        if (!valueStr.startsWith(curDateStr)) {
            return valueStr
        } else {
            return valueStr.substr(cutOutMask.length);
        }
    }

    self.format_date = function(row, cell, value/*, columnDef, dataContext*/) {
        if (value == null || value === "")
            return "";
        return self.formatDateByMask(value, "YYYY/MM/DD ", "YYYY/MM/DD HH:mm:ss.SSS")
    }

    var format_duration_with_red_mark = function (podInfo) {
        return function(row, cell, value, columnDef, dataContext, redBoundary) {
            if (value == null || value === "")
                return "";

            var formatted = self.Duration__formatTime(value);
            if (value > redBoundary)
                formatted = '<ins>' + formatted + '</ins>';

            var folderId = dataContext[C_FOLDER_ID];
            var callHandle = dataContext[C_ROWID];
            if(podInfo[folderId].callHandle) {
                callHandle = podInfo[folderId].callHandle(dataContext);
            }
            // var callHandle = "" + folderId + "_" + rowId + "_0_0"
            var folderName = podInfo[folderId].name;
            var suspTime = dataContext[C_SUSPENSION];
            var queueTime = dataContext[C_QUEUE_WAIT_TIME];
            var res = '<a target="_blank" href="tree.html#params-trim-size=15000&f%5B_' + folderId + '%5D=' + encodeURIComponent(folderName) + '&i=' + callHandle +
            '&s=' + dataContext[C_TIME] + '&e=' + (dataContext[C_TIME] + dataContext[C_DURATION]) + '" title="' +
                self.Duration__formatTime(value - suspTime - queueTime) + ' execution';
            if (suspTime > 0)
                res += ' + ' + self.Duration__formatTime(suspTime) + ' gc/swap';
            if (queueTime > 0)
                res += ' + ' + self.Duration__formatTime(queueTime) + ' waited in queue';

            res += '">' + formatted + ' <div class="uc ui-button"><span class="ui-icon ui-icon-newwin"></span></div></a>';
            return res;
        }
    }

    function format_duration_with_red_mark_nolink(row, cell, value, redBoundary) {
        if (value == null || value === "")
            return "";

        var formatted = self.Duration__formatTime(value);
        if (value > redBoundary)
            formatted = '<ins>' + formatted + '</ins>';

        return formatted;
    }

    self.format_duration = function (podInfo) {
        return function (row, cell, value, columnDef, dataContext) {
            return format_duration_with_red_mark(podInfo)(row, cell, value, columnDef, dataContext, 10000);
        }
    }

    self.format_cpu_time = function(row, cell, value/*, columnDef, dataContext*/) {
        return format_duration_with_red_mark_nolink(row, cell, value, 10000);
    }

    self.format_suspension = function(row, cell, value, columnDef, dataContext) {
        return format_duration_with_red_mark_nolink(row, cell, value, Math.max(dataContext[C_DURATION] * 0.15, 2000));
    }

    self.format_queue_wait = function(row, cell, value, columnDef, dataContext) {
        return format_duration_with_red_mark_nolink(row, cell, value, Math.max(dataContext[C_DURATION] * 0.15, 500));
    }

    self.format_memory = function(row, cell, value, columnDef, dataContext) {
        if (value == null || value === "")
            return "";
        var formatted = self.BigInteger__format(value);
        if (value > 10 * 1024 * 1024)
            formatted = '<ins>' + formatted + '</ins>';
        return formatted;
    }

    self.format_transactions = function(row, cell, value/*, columnDef, dataContext*/) {
        if (value == null || value === "")
            return "";
        var str = self.Integer__format(value);
        return value > 10 ? '<ins>' + str + '</ins>' : str;
    }

    self.format_calls = function(row, cell, value/*, columnDef, dataContext*/) {
        if (value == null || value === "")
            return "";
        return self.BigInteger__formatCalls(value);
    }

    self.format_io = function(row, cell, value, columnDef, dataContext) {
        if (value == null || value === "")
            return "";
        var written = dataContext[columnDef.field + 1];
        return "<span title='" + self.Bytes__formatNoColor(value - written) + " read, " +
            self.Bytes__formatNoColor(written) + " written'>" +
            self.Bytes__format(value) + "</span>";
    }

    self.format_net_io = function(row, cell, value, columnDef, dataContext) {
        if (value == null || value === "")
            return "";
        var written = dataContext[columnDef.field + 1];
        return "<span title='" + self.Bytes__formatNoColor(value - written) + " read, " +
            self.Bytes__formatNoColor(written) + " written'>" +
            self.NetBytes__format(value) + "</span>";
    }

    var METHOD_REGEX = /^(\S+) ((?:[^(.]+\.)*)([^(.]+)\.([^(.]+)(\([^)]*\)) (\([^)]*\))(?: (\[[^\]]*]))?/

//classMethod = m[2] + m[3] + '.' + m[4];
    self.decodeMethod = function(tag) {
        var m = METHOD_REGEX.exec(tag);
        if (m) {
            return [tag, m[1], m[2], m[3], m[4], m[5], m[6], m[7]];
        } else {
            return [tag]
        }
    }

    self.isTagIdle = function(methodTag) {
        if (methodTag.length > 1) {
            return self.idleTags[methodTag[2] + methodTag[3] + '.' + methodTag[4]]
        } else {
            return self.idleTags[methodTag[0]]
        }
    }

    var toShortHTML = function (titleIndexId, tags) {
        var decodedMethod = tags.t[titleIndexId];
        return '<span title="' + escapeHTML(decodedMethod[0]) + '">' + decodedMethod[T_CLASS] + '.<b>' + escapeHTML(decodedMethod[T_METHOD]) + '</b></span>';
    }

    function paramValuesByParamName(call, paramName, tags) {
        var paramIdx = tags.r[paramName];
        if (!call || !call[C_PARAMS] || !call[C_PARAMS][paramIdx]) {
            return [];
        }
        var result = [];
        var resultIDs = call[paramIdx];
        if (!Array.isArray(resultIDs)) {
            resultIDs = [resultIDs]
        }
        for (var i = 0; i < resultIDs.length; i++) {
            var id = resultIDs[i];
            result.push(tags.strByIndex(id))
        }
        return result;
    }

    function escapeHtmlString(string) {
        // return string;
        return string.replace(/"/g, "&#34;");
    }

    function formatTraceIDs(dataContext, tags) {
        var traceIds = paramValuesByParamName(dataContext, 'brave.trace_id', tags);
        var spanIds = paramValuesByParamName(dataContext, 'brave.span_id', tags);
        var xreqIds = paramValuesByParamName(dataContext, 'cloud.x.request.id', tags);
        xreqIds = xreqIds.concat(paramValuesByParamName(dataContext, 'x-request-id', tags));

        if (!traceIds.length && !spanIds.length && !xreqIds.length) {
            return "";
        }

        var metadata = {
            traceIds: traceIds,
            spanIds: spanIds,
            xreqIds: xreqIds
        };
        metadata = JSON.stringify(metadata);

        return '<span class="reactiveButton" reactiveIDs="' + escapeHtmlString(metadata) + '">reactive ids</span>';
    }

    var resolveDecoder = function (tag, params, tags) {
        var decoder = decoders[tag[T_CLASS] + '.' + tag[T_METHOD]];
        if (decoder) {
            return decoder;
        } else if (params) {
            var url = params[tags.r['web.url']];
            if (url) {
                return decoder = decoders['has.url'];
            }
            var queue = params[tags.r['queue']];
            if (queue) {
                return decoders['MessagingMessageListenerAdapter'];
            }
        }
    }

    self.format_title = function (podInfo) {
        return function(row, cell, methodNameIndex, columnDef, dataContext, no_links) {
            if (methodNameIndex == null || methodNameIndex === "")
                return "";
            var cachelet = no_links ? C_TITLE_HTML_NOLINKS : C_TITLE_HTML;
            if (dataContext[cachelet])
                return dataContext[cachelet];
            var tags = podInfo[dataContext[C_FOLDER_ID]].tags;
            var tag = tags.t[methodNameIndex];
            var traceIDs = formatTraceIDs(dataContext, tags);
            var callParams = dataContext[C_PARAMS];

            if (callParams) {
                var title = tags.strByIndex(callParams[tags.r['profiler.title']]);
                if (title) {
                    dataContext[cachelet] = traceIDs + title;
                    return dataContext[cachelet];
                }
            }

            var decoder = resolveDecoder(tag, callParams, tags);

            if (decoder) {
                for (var i = 0; i < decoder.length; i++) {
                    var rr;
                    try {
                        rr = decoder[i](dataContext, no_links, tags);
                    } catch (e) {
                        console.log('Exception while trying decoder for ' + tag[T_CLASS] + '.' + tag[T_METHOD] + ' ' + e);
                    }
                    if (!rr) continue;
                    if (rr.charAt(rr.length - 1) !== ' ') rr += ' ';
                    dataContext[cachelet] = traceIDs + rr;
                    return dataContext[cachelet];
                }
            }

            traceIDs = traceIDs + toShortHTML(methodNameIndex, tags);
            if (callParams) {
                var params = decoder_genericAddParams(callParams, tags, ['j2ee.xid', 'common.started', 'java.thread'], no_links);
                traceIDs += ' ' + params.join('; ');
            }
            traceIDs += ' ';
            return dataContext[cachelet] = traceIDs;
        }
    }

    self.humanSize = function(size) {
        var i = Math.max(0, Math.floor(Math.log(size) / Math.log(1024)));
        return (size / Math.pow(1024, i)).toFixed(2) + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
    }

    self.formatMinutes = function(value) {
        return self.formatDateByMask(value, "", "YYYY/MM/DD HH:mm")
    }
}

export { ESCDataFormat };
