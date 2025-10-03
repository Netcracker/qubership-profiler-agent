import { default as $, jQuery } from './leaked-jquery.mjs';
import 'jquery-bbq';
import 'jquery-ui-themes/base/jquery-ui.css';
import '../styles/jquery-ui.theme.css';
import 'jquery-ui';
import 'jquery-notify/ui.notify.css';
import 'jquery-notify/src/jquery.notify.js';
import 'timepicker/jquery.timepicker.css';
import 'timepicker/jquery.timepicker.js';
import 'jquery.event.drag';
import 'bootstrap-datepicker/dist/css/bootstrap-datepicker.standalone.css';
import 'bootstrap-datepicker';
import 'datepair.js';
import 'datepair.js/dist/jquery.datepair.js';
import { ESCConstants } from './ESCConstants.mjs';
import 'url-search-params-polyfill';
import { ESCProfilerSettings } from './profilerSettings.mjs'
import { ESCDataFormat } from './dataFormat.mjs';
import { ESCDecoders } from './decoders.mjs';
import { escapeHTML, escapeRegExp } from "./utils.mjs";
import { default as links } from 'chap-timeline';
import './leaked-sortable.mjs';
import { SlickGrid, SlickDataView } from 'slickgrid';
import { default as moment } from 'moment';
import 'moment-timezone';
import { default as vkbeautify } from 'vkbeautify';
import 'dygraphs/dist/dygraph.css';
import { default as Dygraph } from 'dygraphs';
import { Activator } from './activate_ide.mjs';
import './jquery-browser.mjs';
import { default as FNV1a128 } from './fnv1a128.mjs';
import '../styles/prof.css';

jQuery.fn.firstParents = function(n) {
    var matched = [];
    var t = this[0].parentNode;
    for (var i = 0; i < n && t; i++) {
        if (t.nodeType === 9) break;
        if (t.nodeType === 1)
            matched.push(t);
        t = t.parentNode;
    }
    return this.pushStack(matched, 'firstParents(' + n + ')', '');
};

var csrf;

if (window.location.protocol !== 'file:') {
    jQuery.get('api/csrf-token')
        .then(data => {
            csrf = data;

            function csrfSafeMethod(method) {
                // these HTTP methods do not require CSRF protection
                return (/^(GET|HEAD|OPTIONS)$/.test(method));
            }

            jQuery.ajaxSetup({
                beforeSend: function (xhr, settings) {
                    if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
                        xhr.setRequestHeader(data.header, data.token);
                    }
                }
            })
        });
}

(function ($) {
    $.extend({
        doGet:function (url, params) {
            document.location = url + '?' + $.param(params);
        },
        doPost:function (url, params) {
            var $div = $('<div>').css('display', 'none');
            var $form = $('<form method="POST">').attr('action', url).attr('target', '_blank');
            function addHiddenField(name, value) {
                $('<input type="hidden">')
                    .attr('name', name)
                    .attr('value', value)
                    .appendTo($form);
            }
            addHiddenField(csrf.header, csrf.token);

            $.each(params, function (name, value) {
                if (value instanceof Array) {
                    for(var i= 0, len = value.length; i<len; i++)
                        addHiddenField(name + '[]', value[i]);
                } else addHiddenField(name, value);
            });
            $form.appendTo($div);
            $div.appendTo('body');
            $form.submit();
        }
    });
})(jQuery);

var CL = {}, CT = {}, profiler_lpad, isDump = {};

export { CL, CT, isDump };

window.CL = CL;
window.CT = CT;
window.isDump = isDump;

(function() {
    /** @const */
    var undefined;

    /** @const */
    var T_FULL_NAME  = ESCConstants.T_FULL_NAME ;
    /** @const */
    var T_RETURN_TYPE  = ESCConstants.T_RETURN_TYPE ;
    /** @const */
    var T_PACKAGE  = ESCConstants.T_PACKAGE ;
    /** @const */
    var T_CLASS  = ESCConstants.T_CLASS ;
    /** @const */
    var T_METHOD  = ESCConstants.T_METHOD ;
    /** @const */
    var T_ARGUMENTS  = ESCConstants.T_ARGUMENTS ;
    /** @const */
    var T_SOURCE  = ESCConstants.T_SOURCE ;
    /** @const*/
    var T_JAR  = ESCConstants.T_JAR ;
    /** @const */
    var T_HTML  = ESCConstants.T_HTML ;
    /** @const */
    var T_CATEGORY  = ESCConstants.T_CATEGORY ;
    /** @const */
    var T_CATEGORY_ACTIVE  = ESCConstants.T_CATEGORY_ACTIVE ;

    /** @const */
    var T_TYPE_LIST  = ESCConstants.T_TYPE_LIST ;
    /** @const */
    var T_TYPE_ORDER  = ESCConstants.T_TYPE_ORDER ;
    /** @const */
    var T_TYPE_INDEX  = ESCConstants.T_TYPE_INDEX ;
    /** @const */
    var T_TYPE_SIGNATURE  = ESCConstants.T_TYPE_SIGNATURE ;

    /** @const */
    var getState = $.bbq.getState;
    /** @const */
    var pushState = $.bbq.pushState;

    var GRAY_START, GRAY_END;
    var RED_START, RED_END;
    if ($.browser.msie || $.browser.mozilla) {
        GRAY_START = '<font color=gray>';
        GRAY_END = '</font>';
        RED_START = '<font color=red style="font-weight:bold;">';
        RED_END = '</font>';
    } else {
        GRAY_START = '<s>';
        GRAY_END = '</s>';
        RED_START = '<ins>';
        RED_END = '</ins>';
    }

    var settings_cookie_domain = location.hostname;
    if (/.*?([^.]+\.[^.]+)/.test(settings_cookie_domain))
        settings_cookie_domain = settings_cookie_domain.match(/.*?([^.]+\.[^.]+)$/)[1];
    else
        settings_cookie_domain = undefined;

    var profiler_settings = ESCProfilerSettings.profiler_settings;

    var lpad = profiler_lpad = function(num, digits) {
        num = num.toString();
        var padding = digits - num.length;
        if (padding <= 0) return num;
        return '0000000000'.substr(0, padding) + num;
    };

    function lpad2(num) {
        return num < 10 ? '0' + num : num;
    }

    isDump.addProperty = function (isCallsFromFile){
            window.isFromDump = isCallsFromFile;

            if(isCallsFromFile === "false"){
                $('#cmd-show-cloud-features').attr('checked', true);
                $('#cmd-show-cloud-features').css("display", "none");
                $('label[for=cmd-show-cloud-features]').css("display", "none");
            }else{
                if(!$('#cmd-show-cloud-features').attr('checked')){
                    pushState({hideCloudFeatures: 'yes'});
                }
            }
        return isCallsFromFile;
    };

    var Integer__format, BigInteger__format, BigInteger__formatCalls, Bytes__format, NetBytes__format, Bytes__formatNoColor;
    var AllocBytes__format;

    function Date__format(date) {
        return date.getFullYear() + '-' + lpad(date.getMonth() + 1, 2) + '-' + lpad(date.getDate(), 2) + '_' + lpad(date.getHours(), 2) + lpad(date.getMinutes());
    }

    function Date__formatWithMillis(date) {
        return date.toString().replace(" GMT", "." + lpad(date.getMilliseconds(), 3) + " GMT");
    }

    function guid() {
        function s4() {
            return Math.floor((1 + Math.random()) * 0x10000)
                .toString(16)
                .substring(1);
        }
        return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
    }

    function invokeLater(caller, runnable, timeout) {
        clearTimeout(caller._invTimeout);
        caller._invRunnable = runnable;
        caller._invTimeout = setTimeout(runnable, timeout);
    }

    function invokeLaterRun(caller) {
        invokeLaterCancel(caller, 1);
    }

    function invokeLaterCancel(caller, run) {
        var runnable = caller._invRunnable;
        if (caller._invTimeout) {
            clearTimeout(caller._invTimeout);
            delete caller._invTimeout;
            delete caller._invRunnable;
        }
        if (run && runnable)
            runnable();
    }

    function getScrollDimensions() {
        var div = $("<div style='position:absolute; top:-10000px; left:-10000px; width:100px; height:100px; overflow:scroll;'></div>").appendTo("body");
        var scrollDim = { width: div[0].offsetWidth - div[0].clientWidth, height: div[0].offsetHeight - div[0].clientHeight };
        div.remove();
        return scrollDim;
    }

    function valueDiffers(a, b) {
        if (typeof a != typeof b){
            if (typeof a == 'string' && typeof b == 'number')
                return Number(a) != b;
            else if (typeof b == 'string' && typeof a == 'number')
                return a != Number(b);
            else
                return true;
        }

        if (typeof a === 'object') {
            var i;
            for (i in a)
                if (valueDiffers(a[i], b[i])) return true;

            for (i in b)
                if (!(i in a)) return true;
            return false;
        }
        if (typeof a == 'array') {
            if (a.length != b.length) return true;
            for (i = 0; i < a.length; i++)
                if (valueDiffers(a[i], b[i]))
                    return true;
        }
        return a != b;
    }

    /** @const */
    var TAGS_ROOT  = ESCConstants.TAGS_ROOT ;
    /** @const */
    var TAGS_HOTSPOTS  = ESCConstants.TAGS_HOTSPOTS ;
    /** @const */
    var TAGS_PARAMETERS  = ESCConstants.TAGS_PARAMETERS ;
    /** @const */
    var TAGS_CALL_ACTIVE  = ESCConstants.TAGS_CALL_ACTIVE ;
    /** @const */
    var TAGS_CALL_ACTIVE_STR  = ESCConstants.TAGS_CALL_ACTIVE_STR ;
    /** @const */
    var TAGS_JAVA_THREAD  = ESCConstants.TAGS_JAVA_THREAD ;

    var idleTags = ESCDataFormat.idleTags;

    var Tags = (function() {
        /** @constructor */
        function T() {
            this.t = [];
            this.r = {};
            this.y = {};
            this.i = {}; // idle tags
            this.strByIndex =  function (index) {
                if(!index) {
                    return undefined
                }
                return this.t[index] ? this.t[index][0] : undefined
            }
            this.a(TAGS_ROOT, 'Root');
            this.a(TAGS_HOTSPOTS, 'Hotspots');
            this.a(TAGS_PARAMETERS, 'Parameters');
            this.a(TAGS_CALL_ACTIVE, 'call.active');
            this.a(TAGS_JAVA_THREAD, 'java.thread');
        }

        var T_TAG_REGEXP = /^(\S+) ((?:[^(.]+\.)*)([^(.]+)\.([^(.]+)(\([^)]*\)) (\([^)]*\))(?: (\[[^\]]*\]))?/;
        var T_REGEXP_TAG_TO_HTML_CLASS_NAME = /(?:[^(.,]+\.)*(\w+)/g;
        var T_REGEXP_COMMA = /,/g;
        var T_REGEXP_WORD_BOUNDARY = /((?:[^A-Z](?=[A-Z])))/g;

        /**
         * @param {Number} id
         * @param {String} tag
         */
        T.prototype.a = function(id, tag) {
            var m = T_TAG_REGEXP.exec(tag);
            var classMethod;
            if (!m) {
                this.t[id] = [tag];
                this.r[tag] = id;
                classMethod = tag;
            } else {
                this.t[id] = [tag, m[1], m[2], m[3], m[4], m[5], m[6], m[7]];
                classMethod = m[2] + m[3] + '.' + m[4];
                this.r[classMethod] = id;
            }
            if (idleTags[classMethod])
                this.i[id] = true;
            if (tag == 'calls.idle')
                this.idle_id = id;
        };

        /**
         * @param {String} name
         * @param {Number} index
         * @param {Number} list
         * @param {Number} order
         */
        T.prototype.b = function(name, list, order, index, signature) {
            var id = this.r[name];
            if (id)
                this.y[id] = [list, order || 100, index, signature];
        };

        /**
         * @param {Number} id
         */
        T.prototype.toHTML = function(id) {
            var t = this.t[id];
            if (!t) {
                return 'tag ' + id;
            }
            var r = t[T_HTML];
            if (r) return r;
            if (t.length == 1) return t[0];
            var method = t[T_METHOD];
            var klass = t[T_CLASS];
            if (method == '<allocate>') {
                klass = '<b>' + klass + '</b>';
                method = GRAY_START + escapeHTML(method) + GRAY_END;
            } else {
                method = '<b>' + escapeHTML(method) + '</b>';
            }
            var returnType = t[T_RETURN_TYPE] == 'void' ? '' : (' : ' + t[T_RETURN_TYPE].replace(T_REGEXP_TAG_TO_HTML_CLASS_NAME, '$1'));

            return t[T_HTML] = '<span class=p>' + t[T_PACKAGE] + '</span><span ad=' + id + '>' + klass + '.' + method + '</span>' + GRAY_START +
                    t[T_ARGUMENTS].replace(T_REGEXP_TAG_TO_HTML_CLASS_NAME, '$1').replace(T_REGEXP_COMMA, ', ') +
                    GRAY_END + returnType;
        };

        /**
         * @param {Number} id
         */
        T.prototype.toHighlightHTML = function (id, klass) {
            var t = this.t[id];
            if (!t) {
                return 'tag ' + id;
            }
            if (t.length == 1) return t[0];
            return this.toHTML(id).replace('span ad=', 'span class="' + klass + '" ad=');
        };

        /**
         * @param {Number} id
         */
        T.prototype.toShortHTML = function (id) {
            var t = this.t[id];
            return '<span title="' + escapeHTML(t[0]) + '">' + t[T_CLASS] + '.<b>' + escapeHTML(t[T_METHOD]) + '</b></span>';
        };

        /**
         * @param {Number} id
         */
        T.prototype.toWrapHTML = function(id) {
            var t = this.t[id];
            var pkg = t[T_PACKAGE];
            var clazz = t[T_CLASS];
            var method = t[T_METHOD];
            if (!pkg || !clazz || !method)
                return escapeHTML(t[0].replace(T_REGEXP_WORD_BOUNDARY, '$1<wbr>'));
            return '<font color=gray>' + pkg.replace(/\./g, '.<wbr>') + '</font><br>' +
                    clazz.replace(T_REGEXP_WORD_BOUNDARY, '$1<wbr>') + '.<b>' +
                    escapeHTML(method).replace(T_REGEXP_WORD_BOUNDARY, '$1<wbr>') + '</b>';
        };

        /**
         * @param {Number} id
         */
        T.prototype.toMethodName = function(id) {
            var t = this.t[id];
            if (!t) {
                return 'tag ' + id;
            }
            if (t.length == 1)
                return t[0];
            return t[T_PACKAGE] + t[T_CLASS] + '.' + t[T_METHOD] + t[T_ARGUMENTS] + ' ' + t[T_SOURCE];
        };

        return T;
    })();

    function JQueryRemove() {
        $(this).remove();
    }


    var Loader_onLoadStart, Loader_onLoadComplete;

    var Loader = (function() {
        var active = 0;
        var queue = {};
        var id = 0;
        var minId = 1;
        var onStart = [], onComplete = [];

        function processQueue() {
            var i;
            for (i = minId; i <= id && queue[i].done; i++) {
                minId = i;
                var qi = queue[i];
                qi.callback(qi);
                active--;
                delete queue[i];
            }
            minId = i;
            if (active > 0) return;
            for (i = 0; i < onComplete.length; i++)
                onComplete[i]();
        }

        Loader_onLoadStart = function(handler) {
            onStart.push(handler);
        };

        Loader_onLoadComplete = function(handler) {
            onComplete.push(handler);
        };

        window.dataReceived = function(id, response) {
            var qi = queue[id];
            if (!qi) {
                var msg = 'Unexpected ajax response received. Id == ' + id + '; response == ' + response;

                if (app.notify)
                    app.notify.notify('create', 'jqn-error', {title: 'Server connection failure', text: msg}, {expires: false, custom:true});
                else
                    alert(msg);
                return;
            }

            qi.done = true;
            qi.response = response;
            processQueue();
        };

        return function(params) {
            // Extract from JQuery
            id++;
            active++;
            for (var i = 0; i < onStart.length; i++)
                onStart[i]();

            params.data.id = id;
            // Send client's UTC time, so server can compensate difference in clocks
            params.data.clientUTC = new Date().getTime();
            queue[id] = params;

            if (params.method === undefined) {
               params.method = 'GET';
            };

            $.ajax({
            	url : params.url,
            	type: params.method,
            	data : params.data
            });
        };
    })();

    /** @constructor
     * @param {Number} id
     * @param {String} folderName */
    function ProfilingFolder(id, folderName, serviceName, namespace) {
        this.id = id;
        this.name = folderName;
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.tags = new Tags();
    }

    function TimeRange_new(a, b) {
        return {min: a, max: b};
    }

    function TimeRange_toString(self) {
        return new Date(self.min) + " .. " + (self.autoUpdate == '1' ? 'now' : new Date(self.max));
    }

    function TimeRange_getExpr(self) {
        return new Function('start', 'duration', "return start+duration >= " + self.min + ' && start <= ' + self.max);
    }

    function TimeRange_includedInto(a, b) {
        return a.min >= b.min && a.max <= b.max;
    }

    var TimeRange$REGEXP = /^\s*(.*\S)\s*(\.{2,}|\+\s*-)\s*(.*\S)\s*$/;

    function date$parse(a) {
        var now = new Date().getTime();
        var maxDate = now + 1000 * 3600 * 24 * 365 * 2, minDate = now - 1000 * 3600 * 24 * 365 * 7;
        var d, i;
        if (a.toLowerCase() == 'now')
            d = now;
        else if (/^\d+$/.test(a)) {
            d = Number(a);
            // Accept "up to 3 ditits missing" or "up to 3 extra digits" in unix timestamp format
            for (i = 0; i < 3; i++)
                if (Math.abs(now - d) > Math.abs(now - d * 10))
                    d *= 10;
                else break;
            for (i = 0; i < 3; i++)
                if (Math.abs(now - d) > Math.abs(now - d / 10))
                    d /= 10;
                else break;
        } else
            d = new Date(a).getTime();
        if (d < 0 || isNaN(d)
            || d > maxDate
            || d < minDate
            )
            return undefined; // isNaN(undefined) == true
        return d;
    }

    function TimeRange$parse(src) {
        var m = TimeRange$REGEXP.exec(src);
        if (!m) return undefined;
        var a = date$parse(m[1]);
        var b = date$parse(m[3]);

        var autoUpdate = m[3].toLowerCase() == 'now';

        var res = {
            min: a,
            max: b
        };

        if (isNaN(res.min) || res.min < 0 || isNaN(res.max) || res.max < 0) return undefined;

        if (autoUpdate) res.autoUpdate = 1;

        return res;
    }

    function Duration_new(a, b) {
        return {min: a, max: b};
    }

    var Duration__formatSmallTime;

    var Duration__formatTimeHMS = ESCDataFormat.Duration__formatTimeHMS;

    function Duration__formatPercents(time) {
        return (time * ESCDataFormat.time_k).toFixed(4) + '%';
    }

    var Duration__formatTime;

    function generateIntFormattingFunction(colorize, magnitude, unit, maxNonRedValue) {
        if (!unit) unit = '';
        var jsCode;
        if (profiler_settings.int_format == '1_234') {
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
            var i = magnitude == 1024 ? 'i' : '';
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

    function updateFormatFromPersonalSettings() {
        ESCDataFormat.updateFormatFromPersonalSettings();
        Duration__formatSmallTime = new Function("time", "if (time < " + profiler_settings.omit_ms + ") return " +
                (profiler_settings.millis_format == '0_400s' ? "(time/1000).toFixed(3)+'s'" : "Math.round(time)+'ms'")
        );
        Integer__format = ESCDataFormat.Integer__format;
        BigInteger__format = ESCDataFormat.BigInteger__format;
        BigInteger__formatCalls = ESCDataFormat.BigInteger__formatCalls;
        Bytes__format = ESCDataFormat.Bytes__format;
        NetBytes__format = ESCDataFormat.NetBytes__format;
        AllocBytes__format = ESCDataFormat.AllocBytes__format;
        Bytes__formatNoColor = ESCDataFormat.Bytes__formatNoColor;
        Duration__formatTime = ESCDataFormat.Duration__formatTime;
    }

    updateFormatFromPersonalSettings();

    function Duration_toString(self) {
        if (self.max == undefined)
            return '>= ' + Duration__formatTime(self.min);
        return Duration__formatTime(self.min) + ' .. ' + Duration__formatTime(self.max);
    }

    function Duration_getExpr(self) {
        if (self.max === undefined)
            return new Function("x", "return x >= " + self.min);
        return Function("x", "return x >=" + self.min + " && x < " + self.max);
    }

    function Duration_includedInto(a, b) {
        var a_min = a.min, b_min = b.min;
        var a_max = a.max, b_max = b.max;

        return ((b_min === undefined) || (a_min !== undefined && b_min <= a_min)) &&
                ((b_max === undefined) || (a_max !== undefined && b_max >= a_max));
    }

    var Duration$SIMPLE_CRITERION = /^(>|>=|<|<=|=)?([0-9.]+)(s|ms)?$/;
    var Duration$RANGE_CRITERION = /^([0-9.]+)(s|ms)?\.{2,}([0-9.]+)(s|ms)?$/;

    function Duration$parse(val) {
        var m, a, b;
        if (!val) return undefined;
        val = val.replace(/\s+/g, '');
        if (m = Duration$SIMPLE_CRITERION.exec(val)) {
            var value = Number(m[2]);
            if (isNaN(value)) return undefined;
            if (m[3] != 'ms') value *= 1000;
            var sign = m[1];
            if (sign == '=')
                a = b = value;
            else {
                if (!sign || sign.length == 0 || sign.charAt(0) == '>')
                    return {min: value};
                return {max: value};
            }
        } else
        if (m = Duration$RANGE_CRITERION.exec(val)) {
            a = Number(m[1]),b = Number(m[3]);
            if (isNaN(a) || isNaN(b)) return undefined;
            if (m[2] != 'ms') a *= 1000;
            if (m[4] != 'ms') b *= 1000;
            if (a > b) {
                var c = a;
                a = b;
                b = c;
            }
        } else return undefined;

        return {
            min: a,
            max: b
        };
    }

    function getServerName() {
        var location = window.location;
        var windowTitle = '';
        if (location.port && location.port.length > 0)
            windowTitle = location.port + ':';
        windowTitle += location.hostname.match(/(?:www\.)?(\d+\.\d+\.\d+\.\d+|\w*)/)[1];
        return windowTitle;
    }

    document.title = getServerName() + ' Profiler';

    if (app.name == 'CallList')
        (function() {
            /** @const */
            var C_TIME  = ESCConstants.C_TIME ;
            /** @const */
            var C_DURATION  = ESCConstants.C_DURATION ;
            /** @const */
            var C_NON_BLOCKING  = ESCConstants.C_NON_BLOCKING ;
            /** @const */
            var C_CPU_TIME  = ESCConstants.C_CPU_TIME ;
            /** @const */
            var C_QUEUE_WAIT_TIME  = ESCConstants.C_QUEUE_WAIT_TIME ;
            /** @const */
            var C_SUSPENSION  = ESCConstants.C_SUSPENSION ;
            /** @const */
            var C_CALLS  = ESCConstants.C_CALLS ;
            /** @const */
            var C_FOLDER_ID  = ESCConstants.C_FOLDER_ID ;
            /** @const */
            var C_ROWID  = ESCConstants.C_ROWID ;
            /** @const */
            var C_METHOD  = ESCConstants.C_METHOD ;
            /** @const */
            var C_TRANSACTIONS  = ESCConstants.C_TRANSACTIONS ;
            /** @const */
            var C_MEMORY_ALLOCATED  = ESCConstants.C_MEMORY_ALLOCATED ;
            /** @const */
            var C_LOG_GENERATED  = ESCConstants.C_LOG_GENERATED ;
            /** @const */
            var C_LOG_WRITTEN  = ESCConstants.C_LOG_WRITTEN ;
            /** @const */
            var C_FILE_TOTAL  = ESCConstants.C_FILE_TOTAL ;
            /** @const */
            var C_FILE_WRITTEN  = ESCConstants.C_FILE_WRITTEN ;
            /** @const */
            var C_NET_TOTAL  = ESCConstants.C_NET_TOTAL ;
            /** @const */
            var C_NET_WRITTEN  = ESCConstants.C_NET_WRITTEN ;
            /** @const */
            var C_PARAMS  = ESCConstants.C_PARAMS ;
            /** @const */
            var C_TITLE_HTML  = ESCConstants.C_TITLE_HTML ;
            /** @const */
            var C_TITLE_HTML_NOLINKS  = ESCConstants.C_TITLE_HTML_NOLINKS ;

            /** @const */
            var C_NAMESPACE  = ESCConstants.C_NAMESPACE ;
            /** @const */
            var C_SERVICE_NAME = ESCConstants.C_SERVICE_NAME;
            var C_TRACE_ID = ESCConstants.C_TRACE_ID;
            var C_SPAN_ID = ESCConstants.C_SPAN_ID;


            var dataView;

            var grid, gridId, gridColumns, gridOptions;

            var selectedRowIds = [];

            var folderNames = {};
            var folderContents = [];
            var data = [];
            var dataRowidIndex = undefined;
            var filter_string = '';
            var filter_string_included, filter_string_excluded, filter_string_mandatory;
            var filters_timerange_pending, filters_timerange_str, filters_duration_str;
            var timezone = profiler_settings.timezone;
            var timezone_pending = timezone;
            var timezoneLabels = [], timezoneLabelsForDate;
            var filters_timerange_is_pending = false;
            var filters_duration_is_pending = false;
            var ignore_timerange_update;
            var filters_timerange, filters_duration;
            var filters_timeline_range;
            var sort_criteria = '';
            var hide_filters = '';
            var show_proxy_requests = '';
            var timelineBottom = '';
            var hideCloudFeatures = '';
            var defaultHiddenRows = [
                "socketinputstream.read",
                "/actuator/ready",
                "/actuator/health",
                "/actuator/metrics",
                "/actuator/prometheus",
                "/probes/live",
                "/probes/ready"
            ];

            var loaded_timerange, loaded_duration;

            var filterDuration, filterTimeRange;
            var filterProxyRequests;
            var $notify;

            function Date__toUTC(date) {
                var tzName = filters_timerange_is_pending ? timezone_pending : timezone;
                var tz = moment.tz.zone(tzName);
                var value = date.getTime();
                var lab = Timezone__label(tzName, value);
                value -= lab.offs * 60000;
                return {date: new Date(value), shortLabel: lab.shortLabel};
            }

            links.Timeline.StepDate.prototype.getLabelMajor = function(options, date) {
                if (date == undefined) {
                    date = this.current;
                }
                var v = Date__toUTC(date);
                date = v.date;

                switch (this.scale) {
                    case links.Timeline.StepDate.SCALE.MILLISECOND:
                        return lpad2(date.getUTCHours())+ ":" +
                            lpad2(date.getUTCMinutes()) + ":" +
                            lpad2(date.getUTCSeconds()) + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.SECOND:
                        return  date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            lpad2(date.getUTCHours()) + ":" +
                            lpad2(date.getUTCMinutes()) + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.MINUTE:
                        return  options.DAYS[date.getUTCDay()] + " " +
                            date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear() + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.HOUR:
                        return  options.DAYS[date.getUTCDay()] + " " +
                            date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear() + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.WEEKDAY:
                    case links.Timeline.StepDate.SCALE.DAY:
                        return  options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear();
                    case links.Timeline.StepDate.SCALE.MONTH:
                        return String(date.getUTCFullYear());
                    default:
                        return "";
                }
            };

            links.Timeline.StepDate.prototype.getLabelMinor = function(options, date) {
                if (date == undefined) {
                    date = this.current;
                }
                var v = Date__toUTC(date);
                date = v.date;

                switch (this.scale) {
                    case links.Timeline.StepDate.SCALE.MILLISECOND:  return String(date.getUTCMilliseconds());
                    case links.Timeline.StepDate.SCALE.SECOND:       return String(date.getUTCSeconds());
                    case links.Timeline.StepDate.SCALE.MINUTE:
                        return lpad2(date.getUTCHours()) + ":" + lpad2(date.getUTCMinutes());
                    case links.Timeline.StepDate.SCALE.HOUR:
                        return lpad2(date.getUTCHours()) + ":" + lpad2(date.getUTCMinutes());
                    case links.Timeline.StepDate.SCALE.WEEKDAY:      return options.DAYS_SHORT[date.getUTCDay()] + ' ' + date.getUTCDate();
                    case links.Timeline.StepDate.SCALE.DAY:          return String(date.getUTCDate());
                    case links.Timeline.StepDate.SCALE.MONTH:        return options.MONTHS_SHORT[date.getUTCMonth()];   // month is zero based
                    case links.Timeline.StepDate.SCALE.YEAR:         return String(date.getUTCFullYear());
                    default:                                         return "";
                }
            };

            filterDuration = filterTimeRange = function() {
                return true;
            };

            function filterOutProxyRequests(item, tags, folder) {
                if (tags.i[item[C_METHOD]]) return false;
                var params = item[C_PARAMS];
                if (params && tags.idle_id in params) {
                    return false;
                }
                var proxyTo = tags.r['proxy.to'];
                if (proxyTo && params)
                    return params[proxyTo];

                let str = tags.t[item[C_METHOD]][0].toLowerCase();
                var urlString = "";
                if(params && tags && tags.r && tags.r['web.url']){
                    urlString = params[tags.r['web.url']];
                }

                for (let i = 0; i < defaultHiddenRows.length; i++){
                    if (str.includes(defaultHiddenRows[i]) || (urlString && urlString.includes(defaultHiddenRows[i]))) {
                       return false;
                   }
               }
                //the call is a reactor-type async subcall. it only means something when displayed along with its caller call
                if(params && tags && tags.r && tags.r['async.absorbed'] !== undefined && params[tags.r['async.absorbed']] ){
                    return false;
                }

                return !(folderContents.length > 1 && item[C_CALLS] < 200 && folder.name.indexOf('Srv/') > 0
                    && item[C_SUSPENSION] < 500
                    && item[C_QUEUE_WAIT_TIME] < 500
                    && item[C_CPU_TIME] < 500);
            }

            filterProxyRequests = filterOutProxyRequests;

            var decoders = CL.decoders = ESCDecoders.decoders;
            CL.idleCalls = idleTags;

            (function() { // create
                dataView = new SlickDataView();

                dataView.setFilter(filter);

                dataView.setItems(data, C_ROWID);

                dataView.sort(function(a, b) {
                    var x = a[0], y = b[0];
                    if (x < y) return -1;
                    return x > y;
                }, false);

                dataView.onRowCountChanged.subscribe(function() {
                    grid.updateRowCount();
                    updateFilterStatus();
                });

                dataView.onRowsChanged.subscribe(function(e, args) {
                    // TODO: migrate to dataview.syncGridSelection
                    grid.invalidateRows(args.rows);
                    grid.render();
                    if (selectedRowIds.length == 0) return;
                    // since how the original data maps onto rows has changed,
                    // the selected rows in the grid need to be updated
                    var selRows = [];
                    for (var i = 0; i < selectedRowIds.length; i++) {
                        var idx = dataView.getRowById(selectedRowIds[i]);
                        if (idx !== undefined)
                            selRows.push(idx);
                    }

                    grid.setSelectedRows(selRows);
                });

                gridOptions = {
                    enableCellNavigation: true,
                    forceFitColumns: false,
                    secondaryHeaderRowHeight: 25,
                    rowCssClasses: format_row_css,
                    rowHeight: 30
                };

                Loader_onLoadStart(CallList_onLoadStart);
                Loader_onLoadComplete(CallList_onLoadComplete);

                var timeRange = getState('timerange');
                var now = new Date().getTime();
                var newState = {};
                if (!/^esc\./.test(window.name)) {
                    window.name = 'esc.' + guid();
                }

                if (timeRange) {
                    if (Number(timeRange.autoUpdate) == 1) {
                        if (getState('wname') != window.name) {
                            timeRange.autoUpdate = 0;
                        } else {
                            var dt = now - Number(timeRange.max);
                            timeRange.max = now;
                            timeRange.min = Number(timeRange.min) + dt;
                        }
                        newState.timerange = timeRange;
                    }
                } else
                    newState.timerange = {min: now - 15 * 60 * 1000, max: now, autoUpdate: 1};
                if (!getState('duration'))
                    newState.duration = {min: 500};
                if (!getState('tz'))
                    newState.tz = profiler_settings.timezone;

                newState.wname = window.name;

                if (newState.duration || newState.timerange) {
                    pushState(newState);
                    skipPostLoad = true;
                } else if (!getState('wname')) {
                    pushState(newState);
                }

                $(init);
            })();

            function sumbitTimerangeDurationFilters(skip_timelineRange) {
                var timerange = filters_timerange_pending;
                var duration = Duration$parse(filters_duration_str);
                if (!!timerange && !!duration) {
                    var diff = (timerange.max - timerange.min) * 0.05;
                    var newState = {
                        timerange: timerange, duration: duration
                        , tz: timezone_pending
                    };
                    if (!skip_timelineRange)
                        newState.timelineRange = {
                            min: timerange.min - diff, max: timerange.max + diff
                        };
                    pushState(newState);
                }
            }

            window.sumbitTimerangeDurationFilters = sumbitTimerangeDurationFilters;

            function init() {
                app.inited = 1;
                updateFilterStatus();

                $notify = app.notify = $("#jqn-container").notify();
                filter_string = getState('q') || '';
                initGrid();

                $('#qry').val(filter_string);

                const resizeHandle = {};
                $(window).resize(function() {
                    invokeLater(resizeHandle, function() {
                        resizeCallList(true);
                        grid.resizeCanvas();
                        grid.onColumnsResized.notify();
                    }, 50);
                });

                var $timeline = $('#timeline');
                var $filter_config = $('#calls-list-configuration');
                var timelineTop = $filter_config.offset().top + $filter_config.outerHeight() + 5;
                $timeline.css({top: timelineTop});

                $('#vrs').show();

                function timeline_scroll(amount) {
                    timeline.move(amount);
                    invokeLater(timeline_scroll, function () {
                        timeline.trigger("rangechange");
                        timeline.trigger("rangechanged");
                    }, 50);
                }

                function timeline_zoom(amount) {
                    timeline.zoom(amount);
                    invokeLater(timeline_zoom, function () {
                        timeline.trigger("rangechange");
                        timeline.trigger("rangechanged");
                    }, 50);
                }

                function arrayToSet(a, s) {
                    if (!s) s = {};
                    for (var i = 0; i < a.length; i++)
                        s[a[i]] = true;
                    return s;
                }

                var searchKeys = arrayToSet([47 /* / */, 46 /* . */, 115 /* s */, 83 /* S */, 1099 /* ы */, 1067 /* Ы */]);
                var refreshKeys = arrayToSet([114 /* r */, 82 /* R */, 1082 /* к */, 1050 /* К */]);
                var zoomInKeys = arrayToSet([$.ui.keyCode.NUMPAD_ADD, 61 /* = in FF */, 187 /* = in Chrome */]);
                var zoomOutKeys = arrayToSet([$.ui.keyCode.NUMPAD_SUBTRACT, 173 /* - in FF */, 189 /* - in Chronme */]);

                $(document).keypress(function(e) {
                    if (document.activeElement && document.activeElement.tagName == 'INPUT' &&
                            document.activeElement.type != 'radio') return;
                    if (e.originalEvent.keyIdentifier == 'U+002F' /* / */
                        || searchKeys[e.keyCode]
                        || searchKeys[e.charCode] /* FF sometimes has 0 keyCode and normal charCode */
                        ) {
                        $('#qry').focus();
                        return false;
                    } else
                    if (refreshKeys[e.keyCode]
                        || refreshKeys[e.charCode]) {
                        $('#tr_15min').trigger('change');
                        return false;
                    }
                });

                $(document).keydown(function (e) {
                    if (document.activeElement && document.activeElement.tagName == 'INPUT' &&
                        document.activeElement.type != 'radio') return;
                    if (e.keyCode == $.ui.keyCode.LEFT) {
                        timeline_scroll(-0.2);
                        return false;
                    }
                    if (e.keyCode == $.ui.keyCode.RIGHT) {
                        timeline_scroll(0.2);
                        return false;
                    }
                    if (e.ctrlKey) {
                        if (e.keyCode == $.ui.keyCode.UP) {
                            if (timelineBottom / 1.2 > 50)
                                pushState({timelineBottom: Number(timelineBottom) / 1.2});
                            return false;
                        }
                        if (e.keyCode == $.ui.keyCode.DOWN) {
                            if (timelineBottom * 1.2 < $(window).height())
                                pushState({timelineBottom: Number(timelineBottom) * 1.2});
                            return false;
                        }
                        return false;
                    }
                    if (e.keyCode == $.ui.keyCode.UP || zoomInKeys[e.keyCode]) {
                        timeline_zoom(0.4);
                        return false;
                    }
                    if (e.keyCode == $.ui.keyCode.DOWN || zoomOutKeys[e.keyCode]) {
                        timeline_zoom(-0.4);
                        return false;
                    }
                });

                $('#cmd-config').click(Configuration__open);
                $('#cmd-ana-thr').click(ThreadDumps__open);

                $('#group').button().click(function() {
                    var folders = {}, i, idx, row, file;
                    var maxSupportedUrl = $.browser.msie ? 1600 : 3500;

                    if (selectedRowIds.length * 50 > maxSupportedUrl) {
                        var $form = $('<form action="tree" method=POST target="_blank">');
                        $form.attr('action', 'tree/' + Date__format(new Date()) + '_group_' + selectedRowIds.length + 'calls.zip');
                        $('<input>').attr({type: 'hidden', name: 'callback', value: 'treedata'}).appendTo($form);
                        $('<input>').attr({type: 'hidden', name: csrf.header, value: csrf.token}).appendTo($form);
                        for (i = 0; i < selectedRowIds.length; i++) {
                            idx = dataView.getRowById(selectedRowIds[i]);
                            if (idx === undefined) continue;
                            row = dataView.rows[idx];
                            if (!folders[row[C_FOLDER_ID]]) {
                                $('<input>').attr({
                                    type: 'hidden',
                                    name: 'f[_' + row[C_FOLDER_ID] + ']',
                                    value: folderContents[row[C_FOLDER_ID]].name
                                }).appendTo($form);
                                folders[row[C_FOLDER_ID]] = 1;
                            }
                            $('<input>').attr({
                                type: 'hidden',
                                name: 'i',
                                value: row[C_ROWID]
                            }).appendTo($form);
                        }
                        $form.appendTo(document.body).submit();
                        return false;
                    }
                    var url = ["tree.html#params-trim-size=15000"];

                    folders = {};
                    var minBeginTime = Number.MAX_VALUE;
                    var maxEndTime = Number.MIN_VALUE;
                    for (i = 0; i < selectedRowIds.length; i++) {
                        idx = dataView.getRowById(selectedRowIds[i]);
                        if (idx === undefined) continue;
                        row = dataView.rows[idx];

                        var beginTime = row[C_TIME];
                        var endTime = row[C_TIME] + row[C_DURATION];
                        if(beginTime < minBeginTime) {
                            minBeginTime = beginTime;
                        }
                        if(endTime >  maxEndTime) {
                            maxEndTime = endTime;
                        }

                        if (!folders[row[C_FOLDER_ID]]) {
                            file = folderContents[row[C_FOLDER_ID]].name;
                            folders[row[C_FOLDER_ID]] = 1;
                            url[url.length] = '&f%5B_';
                            url[url.length] = row[C_FOLDER_ID];
                            url[url.length] = '%5D=';
                            url[url.length] = encodeURIComponent(file);
                        }
                        url[url.length] = '&i=';
                        url[url.length] = row[C_ROWID];
                    }
                    url[url.length] = '&s=';
                    url[url.length] = minBeginTime;
                    url[url.length] = '&e=';
                    url[url.length] = maxEndTime;

                    window.open(url.join(''), '_blank');
                    return false;
                });

                $('#cmd-show-filter').change(function(e) {
                    if ($(e.target).attr('checked'))
                        $.bbq.removeState('hidefilters');
                    else
                        pushState({hidefilters: 'yes'});
                });

                $('#cmd-hide-proxy-requests').change(function (e) {
                    skipPostLoad = true;
                    if ($(e.target).attr('checked'))
                        $.bbq.removeState('showproxy');
                    else
                        pushState({showproxy: 'yes'});
                });

                $('#cmd-hide-timeline').change(function (e) {
                                    if ($(e.target).attr('checked'))
                                        pushState({timelineBottom: 0});
                                    else
                                        pushState({timelineBottom: $('#timeline').offset().top + 165});
                                });


                $('#cmd-show-cloud-features').change(function (e) {
                    if ($(e.target).attr('checked'))
                        $.bbq.removeState('hideCloudFeatures');
                    else
                        pushState({hideCloudFeatures: 'yes'});
                });


                $('#podName').on('change', function(e){
                    sumbitTimerangeDurationFilters();
                });

                $('#timerange-buttons').buttonset();
                $('#tr_selector .time').timepicker({
                    'showDuration': true,
                    'timeFormat': 'H:i:s',
                    'typeaheadHighlight': false
                });

                $('#tr_selector .date').datepicker({
                    'format': 'yyyy-mm-dd',
                    'autoclose': true
                });

                // initialize datepair
                $('#tr_selector').datepair();

                $('#timerange-buttons input:radio').on('change click', function (e) {
                    if (e.target.id == 'tr_custom') {
                        filters_timerange_pending.autoUpdate = 0;
                        setTimerangePending();
                        return;
                    }
                    var now = new Date().getTime();
                    var min = now - e.target.value * 60 * 1000;
                    var wid
                    filters_timerange_pending = {
                        min: min
                        , max: now
                        , autoUpdate: 1
                    };
                    sumbitTimerangeDurationFilters();
                });
                $('#tr_custom').change(function () {
                    $('#tr_selector').focus();
                });

                $('#tr_timezone').autocomplete({
                    source: getTimezonesForAutocomplete
                    , minLength: 0
                    , focus: function(ev, ui) {
                        $('#tr_timezone').val(Timezone__humanName(ui.item.value));
                        return false;
                    }
                    , select: function (ev, ui) {
                        $('#tr_timezone').val(Timezone__humanName(ui.item.value));
                        Timezone__select(ev, ui);
                        return false;
                    }
                }).click(function () {
                    $(this).autocomplete('search', '');
                }).change(Timezone__select);

                $('#tr_selector').on('rangeSelected', tr_selector__applyFilters);
                $('#tr_selector').keyup(function (e) {
                    if (e.keyCode == 13 && (e.ctrlKey || e.metaKey)) {
                        sumbitTimerangeDurationFilters();
                        return;
                    }
                });

                $('#duration-buttons').buttonset();
                $('#duration-buttons input:radio[value]').change(function () {
                    filters_duration_str = this.value;
                    sumbitTimerangeDurationFilters(true);
                });
                $('#dr_selector').keyup(function(e) {
                    if (e.keyCode == 13) {
                        sumbitTimerangeDurationFilters();
                        return;
                    }

                    var valStr = this.value;
                    if (valStr == filters_duration_str) return;
                    filters_duration_str = valStr;

                    var val = Duration$parse(valStr);
                    var ok = !!val;
                    $(this).toggleClass('ok', ok).toggleClass('err', !ok);
                    setDurationPending();
                    $('#apply_filters').button({disabled: !ok});
                    if (!ok) return;

                    updateDurationFilter(val, true);
                    refresh();
                });
                $('#apply_filters').button().click(function () {
                    sumbitTimerangeDurationFilters();
                });

                var timelineBottom = getState('timelineBottom');
                if (!timelineBottom) {
                    pushState({timelineBottom: 0}); // default value on first open
                } else if(timelineBottom > 0) {
                    $('#cmd-hide-timeline').prop('checked', false);
                }


                if (app.data) {
                    CallList_firstData(app.data, app.params);
                    delete app.data;
                }
                $(window).bind('hashchange', onHashChange);
                //data load is already requested in index.html by default
                skipPostLoad = true;
                $(window).trigger('hashchange');
            }

            CL.addFolder = function (folderName, serviceName, namespace) {
                var id = folderNames[folderName];
                if (id) return id;
                id = folderContents.length;
                return folderNames[folderName] = folderContents[id] = new ProfilingFolder(id, folderName, serviceName, namespace);
            };

            CL.append = function (newData) {
                if (newData.length == 0) {
                    return;
                }
                var k;
                if (dataRowidIndex == undefined) {
                    dataRowidIndex = {};
                    for(k = 0; k < data.length; k++) {
                        dataRowidIndex[data[k][C_ROWID]] = k;
                    }
                }
                for(k = 0; k < newData.length; k++) {
                    var row = newData[k];
                    var pk = row[C_ROWID];
                    var pos = dataRowidIndex[pk];
                    if (pos == undefined) {
                        data.push(row);
                        dataRowidIndex[pk] = data.length - 1;
                        continue;
                    }
                    var oldItem = data[pos];
                    var params = oldItem[C_PARAMS];
                    if (params && TAGS_CALL_ACTIVE_STR in params)
                        data[pos] = row;
                }
            };

            function toTz(millis, tz) {
                var zone = moment.tz.zone(tz);
                var bestMillis = -1, bestDiff = 100500100500;
                for (var i = 0; i <= 90; i++) {
                    for (var j = -1; j <= 1; j += 2) {
                        var z0 = millis + i * j * 60000;
                        var z1 = z0 + zone.utcOffset(z0) * 60000;
                        var z2 = z1 - zone.utcOffset(z1) * 60000;
                        var diff = Math.abs(z2 - millis);
                        if (bestDiff > diff) {
                            bestDiff = diff;
                            bestMillis = z1;
                        }
                    }
                }
                return bestMillis;
            }

            function tr_selector__applyFilters(skipFilter) {
                if (ignore_timerange_update) return;
                var $trSelector = $('#tr_selector');
                var sd = $trSelector.find('.date.start').datepicker('getUTCDate');
                if (sd == undefined) return;
                var st = $trSelector.find('.time.start').timepicker('getSecondsFromMidnight');
                if (st == undefined) return;
                var ed = $trSelector.find('.date.end').datepicker('getUTCDate');
                if (ed == undefined) return;
                var et = $trSelector.find('.time.end').timepicker('getSecondsFromMidnight');
                if (et == undefined) return;
                var newFilter = {
                    min: toTz(sd.getTime() + st * 1000, timezone_pending)
                    , max: toTz(ed.getTime() + et * 1000, timezone_pending)
                    , autoUpdate: 0
                };

                if (!filters_timerange_is_pending
                    && Math.abs(Math.floor(filters_timerange_pending.min / 1000) - Math.floor(newFilter.min / 1000)) < 0.5
                    && Math.abs(Math.ceil(filters_timerange_pending.max / 1000) - Math.ceil(newFilter.max / 1000)) < 0.5
                ) {
                    // Tabbing over time edit field causes excessive "change" events
                    return;
                }
                filters_timerange_pending = newFilter;
                setTimerangePending();
                updateTimeRangeFilter(filters_timerange_pending, true);

                if (skipFilter) return;
                refresh();
            }

            function Timezone__select(event, ui) {
                var $this = $('#tr_timezone');
                var tz = $this.val();
                var shiftTime = event.ctrlKey | event.metaKey;
                if (tz.charAt(0) == '!') {
                    tz = tz.substr(1);
                    $this.val(tz);
                    shiftTime = 1;
                }
                var ok = !!moment.tz.zone(tz);

                if (!ok) {
                    var newTz;
                    getTimezonesForAutocomplete({term: tz}, function (res) {
                        if (res.length > 0) newTz = res[0].value;
                    });
                    if (newTz && moment.tz.zone(newTz)) {
                        tz = newTz;
                        $this.val(Timezone__humanName(tz));
                        ok = true;
                        $('#tr_timezone').autocomplete('close');
                    }
                }
                $this.toggleClass('err', !ok);
                if (!ok) return;
                filters_timerange_pending = true;
                if (shiftTime)
                    timezone_pending = tz;
                tr_selector__applyFilters();
                timezone_pending = tz;
                profiler_settings.timezone = tz;
                ESCProfilerSettings.ProfilerSettings__save();
                drawTimerangeEditors();
                grid.invalidate();
                timeline.render();
            }

            var etcGmt = new RegExp('^Etc/GMT.');
            function Timezone__humanName(tz) {
                var uiTz = tz;
                if (etcGmt.test(tz)) {
                    uiTz = uiTz.substring(8);
                    if (uiTz.length === 1) {
                        uiTz = '0' + uiTz;
                    }
                    if (tz.charAt(7) === '-')
                        uiTz = '+' + uiTz;
                    else
                        uiTz = '-' + uiTz;
                    uiTz += ':00';
                }
                return uiTz;
            }

            function Timezone__label(tz, now) {
                var zone = moment.tz.zone(tz);
                var uiTz = Timezone__humanName(tz);
                var offs = zone.utcOffset(now);
                var sign = offs < 0 ? '+' : '-';
                var absOffs = Math.abs(offs);
                var mm = absOffs % 60;
                var shortLabel = sign + lpad2(Math.round((absOffs - mm) / 60)) + ':' + lpad2(mm);
                return {
                    shortLabel: shortLabel
                    , label: shortLabel !== uiTz ? (shortLabel + ' ' + uiTz) : shortLabel
                    , offs: offs
                }
            }

            function getTimezonesForAutocomplete(request, response) {
                var dt = filters_timerange_pending && filters_timerange_pending.min ? filters_timerange_pending.min : new Date().getTime();
                if (!timezoneLabelsForDate || Math.abs(timezoneLabelsForDate - dt) > 3600 * 1000) {
                    timezoneLabelsForDate = dt;
                    var goldTz = {
                        'Europe/Moscow': 2
                        , 'Europe/Samara': 2
                        , 'Europe/Kiev': 2
                        , 'America/New_York': 2
                    };

                    goldTz[profiler_settings.timezone] = 1;

                    timezoneLabels = moment.tz.names();
                    for (var t = 0; t < timezoneLabels.length; t++) {
                        var tz = timezoneLabels[t];
                        var v = Timezone__label(tz, timezoneLabelsForDate);
                        v.value = tz;
                        v.sortKey = (goldTz[tz] || (/^Etc\/GMT[+-][^0]/.test(tz) || tz == "UTC" ? 3 : 4)) + lpad(1440 - v.offs, 5) + tz;
                        timezoneLabels[t] = v;
                    }
                    timezoneLabels.sort(function (a, b) {
                        var va = a.sortKey;
                        var vb = b.sortKey;
                        if (va < vb) return -1;
                        return va > vb ? 1 : 0;
                    });
                }

                var res = [];
                if (request.term == '') {
                    res = timezoneLabels;
                } else {
                    var matcher = new RegExp($.ui.autocomplete.escapeRegex(request.term), 'i');
                    for (var i = 0; i < timezoneLabels.length; i++) {
                        var item = timezoneLabels[i];
                        if (matcher.test(item.label)) {
                            res.push(item);
                        }
                    }
                }

                var t1 = new Date();
                response(res);
            }

            function getLocalDate(utc) {
                var millis = utc;
                var bestMillis = -1, bestDiff = 100500100500;
                for (var i = 0; i <= 90; i++) {
                    for (var j = -1; j <= 1; j += 2) {
                        var z0 = millis + i * j * 60000;
                        var z1 = z0 + new Date(z0).getTimezoneOffset() * 60000;
                        var z2 = z1 - new Date(z1).getTimezoneOffset() * 60000;
                        var diff = Math.abs(z2 - utc);
                        if (bestDiff > diff) {
                            bestDiff = diff;
                            bestMillis = z0;
                        }
                    }
                }
                return bestMillis;
            }

            function drawTimerangeEditors() {
                ignore_timerange_update = true;
                var ui_filter = filters_timerange_is_pending ? filters_timerange_pending : filters_timerange;
                var ui_timezone = filters_timerange_is_pending ? timezone_pending : timezone;
                var minTimeUtc = ui_filter.min - moment.tz.zone(ui_timezone).utcOffset(ui_filter.min) * 60000;
                var minTime = minTimeUtc % 86400000;
                var minTimeLocal = getLocalDate(minTimeUtc - minTime);
                var maxTimeUtc = ui_filter.max - moment.tz.zone(ui_timezone).utcOffset(ui_filter.max) * 60000;
                var maxTime = maxTimeUtc % 86400000;
                var maxTimeLocal = getLocalDate(maxTimeUtc - maxTime);
                var $trSelector = $('#tr_selector');
                $trSelector.find('.date.start').datepicker('setUTCDate', new Date(minTimeLocal));
                $trSelector.find('.time.start').timepicker('setTime', Math.floor(minTime / 1000));
                $trSelector.find('.date.end').datepicker('setUTCDates', new Date(maxTimeLocal));
                $trSelector.find('.time.end').timepicker('setTime', Math.ceil(maxTime / 1000));
                $trSelector.datepair('refresh');
                $('#tr_timezone').val(Timezone__humanName(ui_timezone));
                var autoUpdate = ui_filter.autoUpdate == 1;
                $trSelector.find('input').css('color', autoUpdate ? 'gray' : 'black');
                var $bestBtn;
                if (!autoUpdate) {
                    $bestBtn = $("#tr_custom");
                } else {
                    var diff = (ui_filter.max - ui_filter.min) / 1000 / 60;
                    var sorted = $('#timerange-buttons input[type=radio][value]')
                        .map(function (i, e) {
                            return [[Math.abs(Number(e.value) - diff), e]];
                        })
                        .sort(function (a, b) {
                            return a[0] - b[0];
                        });
                    $bestBtn = $(sorted[0][1]);
                }
                if (!$bestBtn.attr('checked')) {
                    $bestBtn.attr('checked', true);
                    $('#timerange-buttons').buttonset('refresh');
                }
                drawTimerangePendingStatus();
                ignore_timerange_update = false;
            }

            function setTimerangePending() {
                if (filters_timerange_is_pending) return;
                filters_timerange_is_pending = true;
                drawTimerangeEditors();
            }

            function setDurationPending() {
                if (filters_duration_is_pending) return;
                filters_duration_is_pending = true;
                drawTimerangePendingStatus();
            }

            function drawTimerangePendingStatus() {
                $('#apply_filters').toggle(filters_timerange_is_pending || filters_duration_is_pending);
            }

            var CallList_firstData = CL.firstData = function(data) {
                dispatchNewData(data);
                CallList_onLoadComplete();
                app.firstLoadDone = 1;
            };

            function updateFilterStatus() {
                var s;
                var rows = dataView.rows;
                if (data.length == 0)
                    s = 'No data to display';
                else {
                    s = 'Displaying ';
                    s += rows.length;
                    s += ' of ' + data.length + ' calls';
                    if (rows.length > 0 && rows.length < 1000) {
                        s += ', ';
                        var totalDuration = 0, totalQueueing = 0, totalSuspension = 0, totalTransactions = 0;
                        var i, item, dur;
                        for (i = 0; item = rows[i]; i++) {
                            totalDuration += item[C_DURATION];
                            totalQueueing += item[C_QUEUE_WAIT_TIME];
                            totalSuspension += item[C_SUSPENSION];
                            totalTransactions += item[C_TRANSACTIONS];
                        }
                        s += Duration__formatTime(totalDuration);
                        if (sort_criteria == '' || sort_criteria.column == 'Start Timestamp') {
                            var maxWallClock = 0, totalWallClock = 0;
                            var rev = sort_criteria.reverse == 'true';
                            var di = rev ? 1 : -1;
                            for (i = rev ? 0 : rows.length - 1; item = rows[i]; i += di) {
                                dur = item[C_DURATION];
                                var start = item[C_TIME], end = start + dur;
                                if (maxWallClock < start) {
                                    totalWallClock += dur;
                                    maxWallClock = end;
                                } else if (maxWallClock < end) {
                                    totalWallClock += end - maxWallClock;
                                    maxWallClock = end;
                                }
                            }

                            s += ' (' + Duration__formatTime(totalWallClock) + ' server busy) ';
                        }
                        s += ' (' + Duration__formatTime((totalDuration / rows.length).toFixed(3)) + '/call) ';
                        s += ', ' + Duration__formatTime(totalQueueing) + ' queueing ';
                        s += ' (' + Duration__formatTime((totalQueueing / rows.length).toFixed(3)) + '/call)';
                        s += ', ' + Duration__formatTime(totalSuspension) + ' suspend ';
                        s += ' (' + Duration__formatTime((totalSuspension / rows.length).toFixed(3)) + '/call)';
                        s += ', ' + Integer__format(totalTransactions) + ' transactions';
                        s += ' (' + Integer__format((totalTransactions / rows.length).toFixed(3)) + '/call)';
                    }
                }

                $('#filter-status').text(s);
                updateTimeline();
            }

            var timeline, timelineItemRowids = [], timelineRowId2Idx = {};

            function Timeline__rangeChanged() {
                var r = timeline.getVisibleChartRange();
                pushState({timelineRange: {
                    min: r.start.getTime()
                    , max: r.end.getTime()
                }});
            }

            function updateTimeline() {
                var order = dataView.rows, MAX_ROWS = 1000;
                if (order.length > MAX_ROWS) { // get top MAX_ROWS
                    order = order.slice(0, order.length); // make copy
                    order.sort(function (a, b) {
                        var x = b[C_DURATION] - a[C_DURATION];
                        if (x != 0) return x;
                        x = b[C_CPU_TIME] - a[C_CPU_TIME];
                        if (x != 0) return x;
                        x = b[C_TRANSACTIONS] - a[C_TRANSACTIONS];
                        if (x != 0) return x;
                        x = b[C_CALLS] - a[C_CALLS];
                        if (x != 0) return x;
                        x = b[C_SUSPENSION] - a[C_SUSPENSION];
                        if (x != 0) return x;
                        return b[C_QUEUE_WAIT_TIME] - a[C_QUEUE_WAIT_TIME];
                    });
                    if (order.length > MAX_ROWS) {
                        order = order.slice(0, MAX_ROWS);
                    }
                }

                var tl = [];
                timelineItemRowids = [];
                timelineRowId2Idx = {};
                for (var i = 0; i < order.length; i++) {
                    var o = order[i];
                    timelineItemRowids[i] = o[C_ROWID];
                    timelineRowId2Idx[o[C_ROWID]] = i;
                    var title = format_duration_for_timeline(o, 10000);
                    var start = o[C_TIME];
                    var duration = o[C_DURATION];
                    var item;
                    if (duration > 0)
                        item = {
                            start: start, end: start + duration, content: title
                        };
                    else
                        item = {
                            start: start, content: title
                        };

                    tl[tl.length] = item;
                }
                if (!timeline) {
                    var $timeline = $('#timeline');
                    timeline = new links.Timeline($timeline[0]);
                    links.events.addListener(timeline, 'rangechanged', function () {
                        invokeLater(Timeline__rangeChanged, Timeline__rangeChanged, 300);
                    });
                    links.events.addListener(timeline, 'select', function () {
                        var sel = timeline.getSelection();
                        if (!sel || sel.length == 0)
                            return;
                        var row = sel[0].row;
                        var rowId = timelineItemRowids[row];
                        var rowIdx = dataView.getRowById(rowId);
                        grid.scrollRowIntoView(rowIdx, false);
                        grid.setSelectedRows([rowIdx]);
                    });
                    $timeline.resizable({
                        handles: 's'
                        , resize: function (event, ui) {
                            invokeLater(resizeCallList, resizeCallList, 300);
                        }, stop: function (event, ui) {
                            invokeLaterCancel(resizeCallList);
                            if(ui.size.height <= 0) {
                                $('#cmd-hide-timeline').prop('checked', true);
                                pushState({timelineBottom: 0});
                            } else {
                                $('#cmd-hide-timeline').prop('checked', false);
                                pushState({timelineBottom: ui.position.top + ui.size.height});
                            }
                        }, minHeight: 0
                    }).find('.ui-resizable-handle').css('z-index', '8');
                }
                var intervalMin, intervalMax;
                if (filters_timerange) {
                    intervalMin = filters_timerange.min;
                    intervalMax = filters_timerange.max;
                } else {
                    intervalMin = new Date().getTime() - 1000 * 3600 * 24;
                    intervalMax = new Date().getTime() + 1000 * 3600 * 24
                }
                var diff = intervalMax - intervalMin;
                if (diff) {
                    intervalMax += diff*0.05;
                    intervalMin -= diff*0.05;
                }
                var options = {
                    width: '100%'
                    , height: '100%'
                    , eventMarginAxis: 7
                    , eventMargin: 7
                    , cluster: true
                    , axisOnTop: true
                    , animate: false
                    , animateZoom: false
                    , showNavigation: true
                    , min: intervalMin
                    , max: intervalMax
                    , customStackOrder: function (a, b) {
                        // Sort earliest to finish first
                        if ((a instanceof links.Timeline.ItemRange) && !(b instanceof links.Timeline.ItemRange)) {
                            return -1;
                        }

                        if (!(a instanceof links.Timeline.ItemRange) &&
                            (b instanceof links.Timeline.ItemRange)) {
                            return 1;
                        }
                        if (a.right != b.right)
                            return a.right - b.right;
                        return a.left - b.left;
                    }
                };
                timeline.draw(tl, options);
                var range = filters_timeline_range;
                if (!range && (filters_timerange || loaded_timerange)) {
                    range = filters_timerange || loaded_timerange;
                    var rdiff = (range.max - range.min)*0.05;
                    range = {
                        min: range.min - rdiff
                        , max: range.max + rdiff
                    }
                }
                if (!range)
                    range = {};
                timeline.setVisibleChartRange(range.min, range.max);
            }

            function trimDoubleQuote(str) {
                if (str.charAt(0) == '"' && str.charAt(str.length - 1) == '"') {
                    str = str.substr(1, str.length - 2);
                }
                return str;
            }

            function parseSearchParam(str) {
                var result = {};
                if(str.charAt(0) == '"' || str.charAt(0) != '$') {
                    result.value = trimDoubleQuote(str);
                } else {
                    var delimIdx = str.indexOf('=');
                    if(delimIdx == -1) {
                        result.value = trimDoubleQuote(str);
                    } else {
                        result.name = str.substring(1, delimIdx);
                        result.value = trimDoubleQuote(str.substring(delimIdx+1));
                    }
                }
                return result;
            }

            function refresh() {
                filter_string = filter_string.toLowerCase();
                filter_string_included = [];
                filter_string_excluded = [];
                filter_string_mandatory = [];

                if (filter_string.indexOf('/profiler/') == -1)
                    filter_string_excluded[filter_string_excluded.length] = {value: '/profiler/'};
                var m = filter_string.match(/[+-]?\S*?"[^"]*?"|\S+/g);
                if (!m)
                    filter_string_included[0] = {value: filter_string};
                else {
                    for (var i = 0; i < m.length; i++) {
                        var mi = m[i];
                        var dst;
                        var firstChar = mi.charAt(0);
                        if (firstChar == '-') {
                            dst = filter_string_excluded;
                            mi = mi.substr(1);
                        } else if (firstChar == '+') {
                            dst = filter_string_mandatory;
                            mi = mi.substr(1);
                        } else
                            dst = filter_string_included;
                        if (mi.length == 0) continue;
                        dst[dst.length] = parseSearchParam(mi);
                    }
                    if (filter_string_excluded.length == 0)
                        filter_string_excluded = null;
                    if (filter_string_included.length == 0)
                        filter_string_included = null;
                    if (filter_string_mandatory.length == 0)
                        filter_string_mandatory = null;
                }
                dataView.refresh();
                updateFilterStatus();
            }

            /** @const */
            var MATCH_STATE_UNKNOWN = 0;
            /** @const */
            var MATCH_STATE_MAYBE = 1;
            /** @const */
            var MATCH_STATE_OK = 2;
            /** @const */
            var MATCH_STATE_FAIL = 3;

            var all_mandatory_match;

            function stringMatchesArrays(paramName, str, state) {
                if (!filter_string_excluded && !filter_string_included && !filter_string_mandatory)
                    return MATCH_STATE_OK;

                var i;
                str = str.toLowerCase();

                if (filter_string_excluded)
                    for (i = 0; i < filter_string_excluded.length; i++) {
                        var param = filter_string_excluded[i];
                        if((param.name == paramName || (param.name == undefined && paramName.charAt(0) != '_')) && str.indexOf(param.value) > -1) {
                            return MATCH_STATE_FAIL;
                        }
                    }

                all_mandatory_match = true;
                if (filter_string_mandatory) {
                    for (i = 0; i < filter_string_mandatory.length; i++) {
                        var param = filter_string_mandatory[i];
                        if (param.match)
                            continue;
                        all_mandatory_match &= ((param.name == paramName || (param.name == undefined && paramName.charAt(0) != '_')) && (param.match = str.indexOf(param.value) > -1));
                    }
                }

                if (filter_string_included) {
                    for (i = 0; i < filter_string_included.length; i++) {
                        var param = filter_string_included[i];
                        if ((param.name == paramName || (param.name == undefined && paramName.charAt(0) != '_')) && str.indexOf(param.value) > -1)
                            return !filter_string_excluded && all_mandatory_match ? MATCH_STATE_OK : MATCH_STATE_MAYBE;
                    }
                    return state;
                }

                // filter_string_included is undefined, thus the case is similar to match of included pattern
                return !filter_string_excluded && all_mandatory_match ? MATCH_STATE_OK : MATCH_STATE_MAYBE;
            }

            function filter(item) {
                var callDuration = item[C_DURATION] + item[C_QUEUE_WAIT_TIME];
                if (!filterDuration(callDuration)) return false;
                if (!filterTimeRange(item[C_TIME], callDuration)) return false;

                var folder = folderContents[item[C_FOLDER_ID]];
                var tags = folder.tags;
                if (!filterProxyRequests(item, tags, folder)) return false;

                if (!filter_string_included && !filter_string_excluded && !filter_string_mandatory) return true;
                var matchState = MATCH_STATE_UNKNOWN;

                if (filter_string_mandatory) {
                    for (i = 0; i < filter_string_mandatory.length; i++) {
                        filter_string_mandatory[i].match = false;
                    }
                }

                matchState = stringMatchesArrays('method', tags.t[item[C_METHOD]][0], matchState);
                if (matchState > MATCH_STATE_MAYBE) return matchState == MATCH_STATE_OK;

                matchState = stringMatchesArrays('node.name', folder.name, matchState);
                if (matchState > MATCH_STATE_MAYBE) return matchState == MATCH_STATE_OK;

                var p = item[C_PARAMS];
                if (!p) return all_mandatory_match && matchState == MATCH_STATE_MAYBE;
                for (var i in p) {
                    matchState = stringMatchesArrays(tags.t[i][0], tags.t[i][0], matchState);
                    if (matchState > MATCH_STATE_MAYBE) return matchState == MATCH_STATE_OK;
                    var pi = p[i];
                    if (pi instanceof Array) {
                        for (var j = 0; j < pi.length; j++)
                            if (pi[j]) {
                                matchState = stringMatchesArrays(tags.t[i][0], pi[j], matchState);
                                if (matchState > MATCH_STATE_MAYBE) return matchState == MATCH_STATE_OK;
                            }
                    } else if (pi) {
                        matchState = stringMatchesArrays(tags.t[i][0], pi, matchState);
                        if (matchState > MATCH_STATE_MAYBE) return matchState == MATCH_STATE_OK;
                    }
                }
                return all_mandatory_match && matchState == MATCH_STATE_MAYBE;
            }

            /**
             * @param {Object} val
             * @param {boolean} changing
             */
            function updateTimeRangeFilter(val, changing) {
                if (!changing)
                    filters_timerange = val;
                if (filters_timeline_range)
                    val = {
                        min: Math.max(val.min, Math.round(filters_timeline_range.min))
                        , max: Math.min(val.max, Math.round(filters_timeline_range.max))
                    };
                filterTimeRange = TimeRange_getExpr(val);
            }

            /**
             * @param {Object} val
             * @param {boolean} changing
             */
            function updateDurationFilter(val, changing) {
                if (!changing)
                    filters_duration = val;
                filterDuration = Duration_getExpr(val);
            }

            function format_row_css(row) {
                var params = row[C_PARAMS];
                if (!params)
                    return '';
                if (TAGS_CALL_ACTIVE_STR in params)
                    return 'inf';
                var tags = folderContents[row[C_FOLDER_ID]].tags;
                if (tags.r['call.red'] in params)
                    return 'err';
                return '';
            }

            //this one is unique to the old UI since there's a requirement for a fresh moment.js in a new ui
            function format_date(row, cell, value/*, columnDef, dataContext*/) {
                if (value == null || value === "")
                    return "";
                var tzName = filters_timerange_is_pending ? timezone_pending : timezone;
                var tz = moment.tz.zone(tzName);
                var lab = Timezone__label(tzName, value);
                value -= lab.offs * 60000;
                var v = new Date(value);
                var now = new Date().getTime();
                now = new Date(now - lab.offs * 60000);
                var res;
                if (now.getTime() - value > 1000 * 3600 * 24 || now.getUTCDate() != v.getUTCDate())
                    res = lpad(v.getUTCFullYear(), 4) + '/' + lpad(v.getUTCMonth() + 1, 2) + '/' + lpad(v.getUTCDate()) + ' ';
                else
                    res = '';
                var hh = v.getUTCHours();
                var mm = v.getUTCMinutes();
                var ss = v.getUTCSeconds();
                var sss = v.getUTCMilliseconds();
                return res + lpad(hh, 2) + ":" + lpad(mm, 2) + ":" + lpad(ss, 2) + "." + lpad(sss, 3) + ' ' + lab.shortLabel;
            }

            function format_duration_for_timeline(dataContext, redBoundary) {
                var value = dataContext[C_DURATION];
                var formatted = Duration__formatTime(value);
                if (value > redBoundary)
                    formatted = '<ins>' + formatted + '</ins>';

                var folderId = dataContext[C_FOLDER_ID];
                var folderName = folderContents[folderId].name;
                var suspTime = dataContext[C_SUSPENSION];
                var queueTime = dataContext[C_QUEUE_WAIT_TIME];
                var res = '<a target="_blank" href="tree.html#params-trim-size=15000&f%5B_' + folderId + '%5D=' + encodeURIComponent(folderName) + '&i=' + dataContext[C_ROWID] +
                    '&s=' + dataContext[C_TIME] + '&e=' + (dataContext[C_TIME] + dataContext[C_DURATION]) + '" title="' +
                    Duration__formatTime(value - suspTime - queueTime) + ' execution';
                if (suspTime > 0)
                    res += ' + ' + Duration__formatTime(suspTime) + ' gc/swap';
                if (queueTime > 0)
                    res += ' + ' + Duration__formatTime(queueTime) + ' waited in queue';

                var title = format_title('', '', dataContext[C_METHOD], '', dataContext, true);
                res += ' ' + title.replace(/["\n]/g);
                res += '">' + formatted + ' ' + title + '</a>';
                return res;
            }

            var format_duration = ESCDataFormat.format_duration(folderContents);

            var format_cpu_time = ESCDataFormat.format_cpu_time;

            var format_suspension = ESCDataFormat.format_suspension;

            var format_queue_wait = ESCDataFormat.format_queue_wait;

            var format_memory = ESCDataFormat.format_memory;

            var format_transactions = ESCDataFormat.format_transactions;

            var format_calls = ESCDataFormat.format_calls;

            var format_io = ESCDataFormat.format_io;

            var format_net_io = ESCDataFormat.format_net_io;

            var format_pod_name = ESCDataFormat.format_pod_name(folderContents);

            var format_service_name = ESCDataFormat.format_service_name(folderContents);

            var format_namespace = ESCDataFormat.format_namespace(folderContents);

            var format_title = ESCDataFormat.format_title(folderContents);

            function getTimeRangeFromCalls(rows, diff_padding_pct) {
                var start = Number.MAX_VALUE, end = 0;
                for (var i = 0; i < rows.length; i++) {
                    var item = rows[i];
                    var itemStart = item[C_TIME];
                    var itemDuration = item[C_DURATION];
                    if (start > itemStart)
                        start = itemStart;
                    if (end < itemStart + itemDuration)
                        end = itemStart + itemDuration;
                }
                if (diff_padding_pct && end != 0) {
                    var diff = end - start;
                    start -= diff * diff_padding_pct;
                    end += diff * diff_padding_pct;
                }
                if (end == 0)
                    return {
                        start: new Date().getTime() - 1000 * 3600 * 24, end: new Date().getTime() + 1000 * 3600 * 24
                    };
                return {
                    start: start, end: end
                };
            }

            function initGrid() {
                var scrollDim = getScrollDimensions();

                gridColumns = [
                    {id:"date", name:"Start Timestamp", field:C_TIME, behavior:"select", width:135, resizable:true, formatter: format_date, sortable:true},
                    {id:"dur", name:"Duration", field:C_DURATION, width:90, resizable:true, cssClass:'nmbr', formatter: format_duration, sortable:true},
                    //{id:"non_block", name:"Idle Time", field:C_NON_BLOCKING, width:90, resizable:true, cssClass:'nmbr', formatter: format_duration, sortable:true},
                    {id:"cpu", name:"CPU time", field:C_CPU_TIME, width:70, resizable:true, cssClass:'nmbr', formatter: format_cpu_time, sortable:true},
                    {id:"susp", name:"Suspension", field:C_SUSPENSION, width:60, resizable:true, cssClass:'nmbr', formatter: format_suspension, sortable:true},
                    {id:"que", name:"Queue wait time", field:C_QUEUE_WAIT_TIME, width:60, resizable:true, cssClass:'nmbr', formatter: format_queue_wait, sortable:true},
                    {id:"calls", name:"Calls", field:C_CALLS, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_calls, sortable:true},
                    {id:"txs", name:"Transactions", field:C_TRANSACTIONS, behavior:"select", width:40, resizable:true, cssClass:'nmbr', formatter: format_transactions, sortable:true},
                    {id:"fileio", name:"Disk IO", field:C_FILE_TOTAL, behavior:"select", width:70, resizable:true, cssClass:'nmbr', formatter: format_io, sortable:true},
                    {id:"netio", name:"Network IO", field:C_NET_TOTAL, behavior:"select", width:70, resizable:true, cssClass:'nmbr', formatter: format_net_io, sortable:true},
                    {id:"mem", name:"Memory allocated, bytes", field:C_MEMORY_ALLOCATED, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_memory, sortable:true},
                    {id:"nms", name:"Namespace", field:C_NAMESPACE, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_namespace, sortable:false},
                    {id:"srv", name:"Service", field:C_SERVICE_NAME, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_service_name, sortable:false},
                    {id:"pod", name:"POD", field:C_FOLDER_ID, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_pod_name, sortable:true},
                    {id:"title", name:"Title", field:C_METHOD, width:$(window).width() - 135 - 90 - 70 - 60 - 60 - 60 - 40 - 70 - 70 - 60 - 60 - 60 - 8*10  - scrollDim.width - 3, resizable:true, formatter: format_title}
                ];

                var $grid = $("#calls-list-raw");
                grid = new SlickGrid($grid[0], dataView, gridColumns, gridOptions);
                gridId = $grid[0].className.match(/\S+/)[0];

                grid.setSortColumn('date', false);

                grid.onSort = function(sortCol, sortAsc) {
                    pushState({sort: {column: sortCol.name, reverse: sortAsc}});
                };

                grid.onSelectedRowsChanged = function() {
                    selectedRowIds = [];
                    var rows = grid.getSelectedRows();
                    var time = 0;
                    var cpuTime = 0;
                    var selectedRows = [];
                    for (var i = 0, l = rows.length; i < l; i++) {
                        var item = dataView.rows[rows[i]];
                        if (item) {
                            var itemStart = item[C_TIME];
                            var itemDuration = item[C_DURATION];
                            var itemCpuTime = item[C_CPU_TIME];
                            selectedRowIds.push(item[C_ROWID]);
                            selectedRows.push(item);
                            time += itemDuration;
                            cpuTime += itemCpuTime;
                        }
                    }
                    $('#group').button({
                        disabled: selectedRowIds.length < 2,
                        label: 'Group selected ' + selectedRowIds.length + ' row' + (selectedRowIds.length > 1 ? 's' : '') + ' (Duration: ' + Duration__formatTime(time) + ', CPU Time: '+ Duration__formatTime(cpuTime) + ')'
                    }).show();
                    var range = getTimeRangeFromCalls(selectedRows, 2);
                    if (range.end != 0)
                        timeline.setVisibleChartRange(new Date(range.start), new Date(range.end));
                    if (selectedRowIds.length > 0) {
                        var timelineRow = timelineRowId2Idx[selectedRowIds[0]];
                        if (timelineRow != undefined)
                            timeline.setSelection([
                                {row: timelineRow}
                            ]);
                    }
                };

                function _doFilter($qry) {
                    $qry.focus();
                    var val = $qry.val();
                    filter_string = val;
                    refresh();
                    $qry.focus();
                }
                window._doFilter = _doFilter;
                function FilterString_onKeyUp(event) {
                    var $qry = $('#qry');
                    if (event && event.keyCode == 27) {
                        if ($qry.val() == '' && filter_string == '') {
                            invokeLaterCancel(FilterString_onKeyUp);
                            $qry.blur();
                            return false;
                        } else
                            $qry.val('');
                        if (dataView.rows.length >= 1000)
                            _doFilter($qry);
                    }

                    if (dataView.rows.length < 1000)
                        _doFilter($qry);

                    invokeLater(FilterString_onKeyUp, function() {
                        if (dataView.rows.length >= 1000)
                            _doFilter($qry);
                        pushState({q: $qry.val()});
                    }, 2000);
                }

                function FilterString_onBlur() {
                    invokeLaterRun(FilterString_onKeyUp);
                }

                $('#qry').keyup(FilterString_onKeyUp).blur(FilterString_onBlur).click(function () {
                                                            FilterString_onKeyUp.call(this);
                                                            invokeLaterCancel(FilterString_onKeyUp);
                                                        });

                grid.onColumnsReordered.subscribe(function() {
                    grid.onColumnsResized.notify();
                });

                grid.onColumnsResized.subscribe(function() {
                    grid.autosizeColumns();
                });

                grid.onColumnsReordered.notify();
                $('#calls-list-configuration').show();
            }

            var skipPostLoad;

            function resizeCallList(resizeTimeline) {
                var $timeline = $('#timeline');
                if (resizeTimeline) {
                    var $filter_config = $('#calls-list-configuration');
                    var timelineTop = $filter_config.offset().top + $filter_config.outerHeight() + 5;
                    $timeline.css({top: timelineTop, height: timelineBottom - timelineTop});
                }
                timeline.render();
                var tlBottom = $timeline.offset().top + $timeline.outerHeight();
                $('#calls-list-raw').css({top: Number(tlBottom) + 5});
            }

            function onHashChange(event) {
                var val, x, str, e, updateRequired = false;
                var duration, timeRange, timelineRange;
                if(callPodFilter && callPodFilter.length){
                    var filterFromHash = event.getState('callPodFilter');
                    if(filterFromHash){
                        if(callPodFilter.skipPostLoadRequested) {
                            skipPostLoad |= callPodFilter.skipPostLoadRequested();  //do not reload data based on callPodFilter events
                        }
                        callPodFilter.val(filterFromHash);
                    }
                }
                if (valueDiffers(duration = event.getState('duration'), filters_duration)) {
                    updateDurationFilter(duration);

                    e = $('#dr_selector');
                    str = filters_duration_str = Duration_toString(duration);
                    if (e.val() != str) e.val(str);
                    var currentButton = $('#duration-buttons input:radio:checked');
                    if (currentButton.val() != str) {
                        currentButton.attr('checked', false);
                        $('#duration-buttons input:radio[value="' + str + '"]').attr('checked', true);
                        $('#duration-buttons').buttonset('refresh');
                    }
                    e.removeClass('ok err');
                    filters_duration_is_pending = false;
                    updateRequired = true;
                }

                var tz;
                var updateTimerangeRequired = false;
                if (valueDiffers(tz = event.getState('tz'), timezone)) {
                    timezone_pending = timezone = tz;
                    profiler_settings.timezone = tz;
                    ESCProfilerSettings.ProfilerSettings__save();
                    updateTimerangeRequired = true;
                }

                timeRange = event.getState('timerange');
                if (!filters_timerange || String(timeRange.min) !== String(filters_timerange.min) ||
                    String(timeRange.min) !== String(filters_timerange.min) ||
                    String(timeRange.autoUpdate ==null ? '0' : timeRange.autoUpdate ) !== String(filters_timerange.autoUpdate == null ? '0' : filters_timerange.autoUpdate ) )
                {
                    timeRange.min = Number(timeRange.min);
                    timeRange.max = Number(timeRange.max);
                    timeRange.autoUpdate = Number(timeRange.autoUpdate || '0');
                    updateTimeRangeFilter(timeRange);
                    filters_timerange_pending = $.extend({}, timeRange);
                    updateTimerangeRequired = true;
                }

                if (updateTimerangeRequired) {
                    filters_timerange_is_pending = false;
                    updateRequired = true;
                    drawTimerangeEditors();
                }

                drawTimerangePendingStatus();

                if (valueDiffers(timelineRange = event.getState('timelineRange'), filters_timeline_range)) {
                    timelineRange.min = Number(timelineRange.min);
                    timelineRange.max = Number(timelineRange.max);
                    filters_timeline_range = timelineRange;
                    updateTimeRangeFilter(filters_timerange); // it will account for filters_timeline_range
                    updateRequired = true;
                }

                if (valueDiffers(val = (event.getState('q') || ''), filter_string)) {
                    e = $('#qry');
                    if (e.val() != val) {
                        e.val(val);
                        filter_string = val;
                    }
                    //never reload from server when filter query changes
                    // updateRequired = true;
                }

                if (valueDiffers(val = (event.getState('sort') || ''), sort_criteria)) {
                    sort_criteria = val;
                    var reverse = /true/i.test(val.reverse);
                    var columns = grid.getColumns();
                    var idx;
                    for (idx = 0; idx < columns.length && columns[idx].name != val.column; idx++) {
                    }
                    if (idx == columns.length) idx = 0;
                    var column = columns[idx].field;
                    grid.setSortColumn(columns[idx].id, reverse);

                    dataView.sort(function(a, b) {
                        var x = a[column], y = b[column];
                        if (x < y) return -1;
                        return x > y;
                    }, reverse);
                }

                var resizeRequired = false;
                if (valueDiffers(val = (event.getState('hidefilters') || ''), hide_filters)) {
                    hide_filters = val;
                    var doShow = !/yes|true/i.test(hide_filters);
                    $('#cl-cfg-flt').css('display', doShow ? 'block' : 'none');
                    $('#cmd-show-filter').attr('checked', !hide_filters);
                    resizeRequired = true;
                }

                if (valueDiffers(val = (event.getState('hideCloudFeatures') || ''), hideCloudFeatures)) {
                    var cols = grid.getColumns().slice(0);
                    hideCloudFeatures = val;

                    if(!hideCloudFeatures){
                         cols.splice(cols.length-2, 0,
                             {id:"nms", name:"Namespace", field:C_NAMESPACE, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_namespace, sortable:false},
                             {id:"srv", name:"Service", field:C_SERVICE_NAME, behavior:"select", width:60, resizable:true, cssClass:'nmbr', formatter: format_service_name, sortable:false}
                          );
                          $('#callPodFilterSpan').show();
                     }else{
                        cols.splice(cols.length-4, 2);

                        $('#callPodFilterSpan').hide();
                     }

                     grid.setColumns(cols);

                     grid.onColumnsReordered.notify();

                     resizeCallList(true);
                     $(window).triggerHandler('resize');

                     $('#cmd-show-cloud-features').attr('checked', !hideCloudFeatures);
                }


                if (valueDiffers(val = (event.getState('showproxy') || ''), show_proxy_requests)) {
                    show_proxy_requests = val;
                    var doShowProxy = /yes|true/i.test(show_proxy_requests);
                    $('#cmd-hide-proxy-requests').attr('checked', !doShowProxy);
                    if (!doShowProxy){
                        filterProxyRequests = filterOutProxyRequests;
                    }
                    else {
                        filterProxyRequests = function () {
                            return true;
                        }
                    }
                    updateRequired = true;
                    resizeRequired = true; // group selected button might move to second line
                }

                if (valueDiffers(val = (event.getState('timelineBottom') || ''), timelineBottom)) {
                    timelineBottom = val;
                    resizeRequired = true;
                }

                if (resizeRequired) {
                    resizeCallList(true);
                    $(window).triggerHandler('resize');
                }

                if (!updateRequired)
                    return;
                refresh();

                if (skipPostLoad) {
                    skipPostLoad = false;
                    return;
                }
                schedulePostLoad();
            }

            function applyAvailableServices(availableServices){
                if(!availableServices) {
                    return;
                }
                window.availableServices = availableServices;
                window.workingWithMultiplePODs = true;

                var cols = grid.getColumns().slice(0);
//                 var tags = folderContents[dataContext[C_FOLDER_ID]].tags;
                    var addInFilterAndRun = function(clickedId){
                        var qry = $('#qry');
                        var oldVal = qry.val();
                        if(!oldVal){
                            oldVal = "";
                        }
                        if(oldVal.indexOf(clickedId) < 0){
                            qry.val(oldVal + ' ' + clickedId);
                        }
                        pushState({q: qry.val()});
                        window._doFilter(qry);
                    };

                    grid.onClick = function(e) {
                        if(e.target.className.indexOf('reactiveButton') >= 0){
                            var reactiveIDs = e.target.getAttribute("reactiveIDs");
                            reactiveIDs = JSON.parse(reactiveIDs);
                            var popup = $('<div class="reactivePopup" id="reactivePopup"></div>');
                            var originPosition = $(e.target).offset();
                            popup.offset({
                                left: originPosition.left,
                                top: originPosition.top
                            });
                            var popupInner = $('<div class="reactivePopupInner"></div>');
                            popup.append(popupInner);
                            function printWithCaption(parent, caption, array){
                                if(array && array.length > 0){
                                    parent.append($("<span class='reactiveCaption'></span>").text(caption));
                                    for(var i = 0; i <  array.length; i++){
                                        var value = array[i];
                                        parent.append($("<span class='tracespanid'></span>").text(value));
                                        if(i < array.length-1){
                                            parent.append(' ');
                                        }
                                    }
                                    parent.append($("<br/>"));
                                }
                            }

                            printWithCaption(popupInner, "Trace IDs:\t", reactiveIDs.traceIds);
                            printWithCaption(popupInner, "Span IDs:\t", reactiveIDs.spanIds);
                            printWithCaption(popupInner, "X-request-IDs:\t", reactiveIDs.xreqIds);

                            popup.click(function(e){
                                if(e.target.className.indexOf('tracespanid') < 0){
                                    return;
                                }
                                var clickedText = e.target.innerText;
                                if (clickedText.indexOf("\n") < 0 ) {
                                    addInFilterAndRun(clickedText);
                                } else {
                                    var lines = clickedText.split("\n");
                                    for (index = 0, len = lines.length; index < len; ++index) {
                                        var line = lines[index];
                                        var toks = line.split(":");
                                        if (toks.length > 1){
                                            var value = toks[1].trim();
                                            addInFilterAndRun(value);
                                        }
                                    }
                                    clickedId = e.target.innerText;
                                }
                            });

                            var sb = $("<div class='reactiveScreenBlocker'>&nbsp;</div>");
                            sb.click(function(e){
                                popup.detach();
                                sb.detach();
                            });

                            $(document.body).append(popup);
                            $(document.body).append(sb);
                            // in chrome actual width is floating. And width() method only returns its integer part
                            popupInner.width(popupInner.width()+1);
                            popupInner.height(popupInner.height()+1);
                            popup.hide();
                            popup.animate({
                                width: "toggle"
                            },200);

                            e.preventDefault();
                            //there is a bug in slickgrid: after doFilter it loses the last row. Which is the row with the main HTTP call in case of Flux
                            //need to trigger resize manually to force grid to display it properly
                            // window.dispatchEvent(new Event('resize'));
                            e.preventDefault();
                        }
                    };
                    window.dispatchEvent(new Event('resize'));

                grid.setColumns(cols);
                grid.onColumnsReordered.notify();
            }

            function dispatchNewData(params) {
                var args = params.data;
                var src_duration = args.duration;
                var src_timerange = args.timerange;

                var availableServices = args.availableServices;
                applyAvailableServices(availableServices);

                if (dataView)
                    dataView.beginUpdate();

                if (!params.incremental) {
                    data.length = 0;
                    dataRowidIndex = undefined;
                    dataView.setItems(data, C_ROWID);
                    loaded_duration = src_duration;
                    loaded_timerange = src_timerange;
                }

                (params.response)();

                if (dataView) {
                    dataView.setItems(data, C_ROWID);
                    dataRowidIndex = undefined; // "sort" would break all indices
                    dataView.reSort();
                    dataView.endUpdate();
                    updateFilterStatus();
                }

                if (loaded_duration.min !== undefined && loaded_duration.min > src_duration.min)
                    loaded_duration.min = src_duration.min;
                if (loaded_duration.max !== undefined &&
                        (src_duration.max === undefined || loaded_duration.max < src_duration.max))
                    loaded_duration.max = src_duration.max;

                loaded_timerange.min = Math.min(src_timerange.min, loaded_timerange.min);
                loaded_timerange.max = Math.max(src_timerange.max, loaded_timerange.max);
            }

            function CallList_onLoadStart() {
                $('#loading').show();
            }

            function CallList_onLoadComplete() {
                $('#loading').hide();
                $('#tr_selector input').removeClass('ok');
                $('#dr_selector').removeClass('ok');
            }

            function postLoad(duration, timerange, incremental, searchConditions) {
                var url = 'js/calls.js';

                Loader({
                    url: url,
                    data: {
                        duration: duration,
                        timerange: timerange,
                        searchConditions: searchConditions
                    },
                    incremental: incremental,
                    callback: dispatchNewData
                });
            }

            function schedulePostLoad() {
                if (!app.firstLoadDone) {
                    setTimeout(schedulePostLoad, 100);
                    return;
                }
                if(window.workingWithMultiplePODs){
                    var searchConditions;
                    if(callPodFilter){
                        searchConditions = callPodFilter.getConditionsString();
                    }
                    if(!searchConditions){
                        searchConditions = '';
                    }
                    postLoad(filters_duration, filters_timerange, false, searchConditions);
                    return;
                }
                if (loaded_duration && loaded_timerange &&
                        Duration_includedInto(filters_duration, loaded_duration)) {

                    if (TimeRange_includedInto(filters_timerange, loaded_timerange)) {
                        CallList_onLoadComplete();
                        return; // both filters do not require loading
                    }

                    if (filters_timerange.max < loaded_timerange.min ||
                            filters_timerange.min > loaded_timerange.max) {
                        // New timerange does not intersect with loaded
                        // Will load new one and discard old
                        postLoad(loaded_duration, filters_timerange, false);
                        return;
                    }
                    // We need to extend timerange.

                    if (filters_timerange.min < loaded_timerange.min) {
                        // New timerange extends the low bound
                        postLoad(loaded_duration, {min: filters_timerange.min, max: loaded_timerange.min}, true);
                    }
                    if (filters_timerange.max > loaded_timerange.max) {
                        // New timerange extends the high bound
                        var newMin = loaded_timerange.max;
                        for (var k = 0; k < data.length && k < 300; k++) {
                            var row = data[k];
                            var params = row[C_PARAMS];
                            if (params && TAGS_CALL_ACTIVE_STR in params && row[C_TIME] > loaded_timerange.min) {
                                newMin = Math.min(newMin, row[C_TIME] - 10);
                            }
                        }
                        postLoad(loaded_duration, {min: newMin, max: filters_timerange.max}, true);
                    }
                    return;
                }

                postLoad(filters_duration, filters_timerange, false);
            }

            window.onSearchRequest = schedulePostLoad;

            /* Decoders(Deprecated, use ServerSide formatters instead) */
            var qrtzJob = (function() {
                var hiddenParams = {};
                var types = {
                    '7020873015013388039': 'JMS',
                    '7020873015013388040': 'URL',
                    '7020873015013388043': 'SOAP',
                    '7020873015013388041': 'EJB',
                    '7020873015013388042': 'Class'
                };

                return function (dataContext, no_links) {
                    var p = dataContext[C_PARAMS];
                    if (!p) return;
                    var tags = folderContents[dataContext[C_FOLDER_ID]].tags;
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

                    if (type == 'Class')
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
                    if (val.length == 0) return;
                    val = '[' + val.join(', ') + ']';
                }
                r[r.length] = name;
                r[r.length] = escapeHTML(val);
                return true;
            }

            var decoder_genericAddParams = ESCDecoders.decoder_genericAddParams;

            var Configuration$initDone;
            var Configuration$dumperRunning;

            /** @const */
            var RELOAD_RESULT_IS_DONE = 0;
            /** @const */
            var RELOAD_RESULT_TOTAL_COUNT = 1;
            /** @const */
            var RELOAD_RESULT_SUCCESS_COUNT = 2;
            /** @const */
            var RELOAD_RESULT_ERROR_COUNT = 3;
            /** @const */
            var RELOAD_RESULT_CONFIG_PATH = 4;
            /** @const */
            var RELOAD_RESULT_FILTER_CONFIG = 5;
            /** @const */
            var RELOAD_RESULT_MESSAGE = 6;

            /** @const */
            var DUMPER_STATUS_IS_RUNNING = 0;
            /** @const */
            var DUMPER_STATUS_ACTIVE_TIME = 1;
            /** @const */
            var DUMPER_STATUS_NUMBER_OF_RESTARTS = 2;
            /** @const */
            var DUMPER_STATUS_WRITE_TIME = 3;
            /** @const */
            var DUMPER_STATUS_WRITTEN_BYTES = 4;
            /** @const */
            var DUMPER_STATUS_WRITTEN_RECORDS = 5;
            /** @const */
            var DUMPER_STATUS_CURRENT_ROOT = 6;
            /** @const */
            var DUMPER_STATUS_CPU_TIME = 7;
            /** @const */
            var DUMPER_STATUS_MEMORY_USED = 8;
            /** @const */
            var DUMPER_STATUS_FILE_READ = 9;
            /** @const */
            var DUMPER_STATUS_ARCHIVE_SIZE = 10;

            function Configuration__configFile_reload() {
                Loader({
                    data: {},
                    url: 'config/reload_status',
                    callback: Configuration__configFile_reloadStatus
                });
            }

            function Configuration__dumper_reload() {
                Loader({
                    data: {},
                    url: 'config/dumper/status',
                    callback: Configuration__dumper_status
                });
            }

            function Configuration__configFile_reloadStatus(params) {
                var r = params.response;
                $('#cfg-rl-path').val(r[RELOAD_RESULT_FILTER_CONFIG]);
                var x = (r[RELOAD_RESULT_FILTER_CONFIG] || '').match(/_config.filters.(\w+).xml/);
                x = x ? x[1] : '';
                $('#cfg-rl-opts input:radio[value="' + x + '"]').attr('checked', true);
                $('#conf-reload').button({disabled: !r[RELOAD_RESULT_IS_DONE]});
                $('#conf-file-save').button('disable');
                var progress = r[RELOAD_RESULT_IS_DONE] ? 100 : (r[RELOAD_RESULT_SUCCESS_COUNT] + r[RELOAD_RESULT_ERROR_COUNT]) * 100 / r[RELOAD_RESULT_TOTAL_COUNT];

                $('#cfg-rl-pbar').progressbar({value: progress});
                var msg = r[RELOAD_RESULT_MESSAGE];
                var m = msg.match(/^([^\n\r]+)((?:\s\S)*)$/);
                if (msg.indexOf('does not support configuration reloading') >= 0)
                    $('#conf-reload').button('disable');
                if (m) {
                    msg = m[2];
                    m = m[1];
                } else {
                    m = '';
                }
                $('#cfg-rl-detail').text(msg);
                $('#cfg-rl-msg').text(progress.toFixed(1) + '% ' + m);
                if (r[RELOAD_RESULT_IS_DONE]) return;
                invokeLater(Configuration__configFile_reload, Configuration__configFile_reload, 2000);
            }

            function Configuration__dumper_status(params) {
                var r = params.response;
                if (r.length == 1) { // Exception
                    $('#cfg-dum-exc').text(r[0]);
                    return;
                }
                Configuration$dumperRunning = r[DUMPER_STATUS_IS_RUNNING];
                $('#cfg-dum-stat').text(Configuration$dumperRunning ? 'Active' : 'Inactive');
                $('#cfg-dum-switch').button({
                    disabled: false,
                    label: Configuration$dumperRunning ? 'Stop' : 'Start'
                });
                $('#cfg-dum-restarts').text(r[DUMPER_STATUS_NUMBER_OF_RESTARTS]);
                var pctBusy = r[DUMPER_STATUS_ACTIVE_TIME];
                if (pctBusy == 0 || !Configuration$dumperRunning)
                    pctBusy = 'n/a';
                else
                    pctBusy = (r[DUMPER_STATUS_WRITE_TIME] / pctBusy * 100 / 1000000).toFixed(3) + '%';
                $('#cfg-dum-pct').text(pctBusy);
                var bytes;
                if (Configuration$dumperRunning) {
                    bytes = Bytes__format(r[DUMPER_STATUS_WRITTEN_BYTES]) + ' (' + Bytes__format(r[DUMPER_STATUS_FILE_READ]) + ')';
                } else {
                    bytes = 'n/a';
                }
                $('#cfg-dum-bytes').html(bytes);
                $('#cfg-dum-rows').html(Configuration$dumperRunning ? Integer__format(r[DUMPER_STATUS_WRITTEN_RECORDS]) : 'n/a');
                $('#cfg-dum-time').html(Configuration$dumperRunning ? Duration__formatTime(r[DUMPER_STATUS_WRITE_TIME] / 1000000) : 'n/a');
                $('#cfg-dum-act-time').html(Configuration$dumperRunning ? Duration__formatTime(r[DUMPER_STATUS_ACTIVE_TIME]) : 'n/a');
                $('#cfg-dum-root').text(Configuration$dumperRunning ? r[DUMPER_STATUS_CURRENT_ROOT] : 'n/a');
                $('#cfg-dum-cpu-time').html(Configuration$dumperRunning ? Duration__formatTime(r[DUMPER_STATUS_CPU_TIME]) : 'n/a');
                var memoryUsed = r[DUMPER_STATUS_MEMORY_USED];
                $('#cfg-dum-mem-used').html(Configuration$dumperRunning && memoryUsed > 0 ? Bytes__format(memoryUsed) : 'n/a');

                $('#cfg-dum-arch-sz').html(Bytes__format(r[DUMPER_STATUS_ARCHIVE_SIZE]));
                $('#cfg-dum-frc-scn').button({
                    disabled: false
                });
            }

            function Configuration__scheduleRefresh(tab) {
                if (tab == 0)
                    Configuration__configFile_reload();
                if (tab == 1)
                    Configuration__dumper_reload();
            }

            function Configuration__configFile_updatePath() {
                $('#conf-file-save, #conf-reload').button('enable');
                var newPath = $('#cfg-rl-opts input:radio:checked').val();
                if (newPath && newPath.length > 0)
                    $('#cfg-rl-path').val('_config.filters.' + newPath + '.xml');
            }

            function Configuration__configFile_performReload() {
                $('#conf-file-save, #conf-reload').button('disable');
                Loader({
                    data: {
                        config: $('#cfg-rl-path').val() || '',
                        now: this.id == 'conf-reload'
                    },
                    url: 'config/reload',
                    method: 'POST',
                    callback: Configuration__configFile_reloadStatus
                });
            }

            function Configuration__open() {
                if (!Configuration$initDone) {
                    Configuration__scheduleRefresh(0);
                    $('#tabs').tabs({
                        beforeActivate: function(e, ui) {
                            Configuration__scheduleRefresh(ui.newTab.index());
                        }
                    });
                    $('#conf-file-save,#conf-reload').button({disabled:true}).click(Configuration__configFile_performReload);
                    $('#cfg-rl-pbar').progressbar();
                    $('#config-dialog').dialog({
                        title: 'Profiler configuration',
                        width: 900,
                        height: 500,
                        resizable: true,
                        close: function() {
                            invokeLaterCancel(Configuration__configFile_reload);
                        }
                    });
                    $('#cfg-dum-switch').button({
                        disabled: true
                    }).click(function() {
                        var that = $(this);
                        Loader({
                            data: {},
                            url: 'config/dumper/' + (Configuration$dumperRunning ? 'stop' : 'start'),
                            method: 'POST',
                            callback: Configuration__dumper_status
                        });
                        that.button({disabled: true});
                    });
                    $('#cfg-rl-opts input:radio').change(Configuration__configFile_updatePath);
                    $('#cfg-rl-path').bind('change keypress', function() {
                        $('#cfg-rl-cust').click();
                        $('#conf-file-save,#conf-reload').button('enable');
                    });
                    $('#cfg-rl-opts input:radio[value=""]').attr('checked', true);

                    $('#cfg-dum-frc-scn').button({
                        disabled: true
                    }).click(function() {
                        var that = $(this);
                        Loader({
                            data: {},
                            url: 'config/dumper/rescan',
                            method: 'POST',
                            callback: Configuration__dumper_status
                        });
                        that.button({disabled: true});
                    });
                    Configuration$initDone = true;
                }
                $('#config-dialog').dialog('open');
                return false;
            }

            var ThreadDumps$initDone;

            function ThreadDumps__open() {
                if (!ThreadDumps$initDone) {
                    $('#thr-dmp').dialog({
                                title: 'Analyze file',
                                width: 650,
                                height: 210,
                                resizable: true,
                                buttons: {
                                    Analyze: ThreadDumps__analyze,
                                    Cancel: function() {
                                        $(this).dialog('close');
                                    }
                                }
                            });
                    $('#thr-dmp-file').keyup(ThreadDumps__scheduleUpdateFileSize).blur(function() {
                        invokeLaterRun(ThreadDumps__scheduleUpdateFileSize);
                    });
                    $('#thr-dmp-fb,#thr-dmp-lb').change(ThreadDumps__analyzeFilePart).keypress(ThreadDumps__analyzeFilePart);
                    $('#thr-dmp-fs').click(ThreadDumps__updateFileSize);
                    ThreadDumps$initDone = true;
                    ThreadDumps__updateFileSize();
                }
                $('#thr-dmp').dialog('open');
                return false;
            }

            function ThreadDumps__scheduleUpdateFileSize() {
                $('#thr-dmp-st').css('color', 'gray');
                if ($(this).val() == '')
                    invokeLaterCancel(arguments.callee);
                else
                    invokeLater(ThreadDumps__updateFileSize, ThreadDumps__updateFileSize, 1000);
            }

            function ThreadDumps__analyzeFilePart() {
                $('#thr-dmp-sub').attr('checked', true);
            }

            function ThreadDumps__updateFileSize() {
                var fileName = $('#thr-dmp-file').val();
                Loader({
                            data: {file: fileName},
                            url: 'threaddump/file_size',
                            callback: functionThreadDumps__updateFileSize_callback
                        });
                return false;
            }

            function functionThreadDumps__updateFileSize_callback(params) {
                var r = params.response;
                var $file = $('#thr-dmp-file');
                var currentFile = $file.val();
                if (currentFile != params.data.file) return;
                if (currentFile != r[0]) $file.val(r[0]);
                var status, size = r[1];
                if (size == -2)
                    status = 'directory, will parse all contents recursively';
                else if (size == -1)
                    status = 'file does not exist';
                else if(size == -3)
                    status = 'Access denied. Edit applications/execution-statistics-collector/config/analyzer_white_list.cfg to grant access.';
                else
                    status = size + (size < 102400 ? ' bytes' : (' (' + Bytes__format(size) + ')'));
                $('#thr-dmp-st').html(status);
                $('#thr-dmp-st').css('color', size == -1 || size == -3 ? 'red' : '');
            }

            function ThreadDumps__analyze() {
                var fileName = $('#thr-dmp-file').val();
                if (!fileName) {
                    $('#thr-dmp-file').focus();
                    app.notify.notify('create', 'jqn-error', {title: 'File analysis', text: 'Please specify file name to analyze.<br>This is usually logs/console.log or servers/clustX/logs/clustX.log.'}, {expires: 2000, custom:true});
                    return;
                }
                var fileFormat = $('#thr-dmp input:radio[name=thr-dmp-format]:checked').val();
                if (/(\.gz|\.zip)$/.test(fileName) && $('#thr-dmp-auto').attr('checked')) {
                    app.notify.notify('create', 'jqn-error', {title: 'File analysis', text: 'Please specify file format.<br>File format is mandatory when parsing archived files.'}, {expires: 2000, custom:true});
                    return;
                }
                var params = {file:fileName, callback:'treedata', format: fileFormat};
                if ($('#thr-dmp-sub').attr('checked')) {
                    if ((params.firstByte = ThreadDumps__parseBytes('thr-dmp-fb', 'starting offset')) === undefined) return;
                    if ((params.lastByte = ThreadDumps__parseBytes('thr-dmp-lb', 'last byte to parse')) === undefined) return;
                }
                var type;
                if (fileFormat != 'auto') {
                    type = fileFormat;
                } else if (/\.trc$/.test(fileName)) {
                    type = "dbms_hprof";
                } else if (/collapsed\.log$/.test(fileName)) {
                    type = "stackcollapse";
                } else if (/\.jfr$/.test(fileName)) {
                    type = "jfr_allocation";
                } else {
                    type = "threaddump";
                }
                window.open('tree/' + Date__format(new Date()) + '_' + type + '.zip?' + $.param(params));
                $(this).dialog('close');
            }

            function ThreadDumps__parseBytes(id, name) {
                var $e = $('#' + id);
                var res = $e.val();
                if (res == '') return res;
                res = parseInt(res);
                if (!isNaN(res)) return res;
                $e.focus().css('color', 'red');
                app.notify.notify('create', 'jqn-error', {title: 'Threaddump analysis', text: 'Please specify valid number for ' + name}, {expires: 3000, custom:true});
                return undefined;
            }
        })();

    if (app.name == 'CallTree')
        (function() {
            var j = [];
            /** @const */
            var S_ID = 0;
            /** @const */
            var S_DURATION = 1;
            /** @const */
            var S_CHILDREN = 2;
            /** @const */
            var S_TAGS = 3;

            /** @const */
            var M_ID = 0;
            /** @const */
            var M_DURATION = 1;
            /** @const */
            var M_SELF_DURATION = 2;
            /** @const */
            var M_SUSPENSION = 3;
            /** @const */
            var M_SELF_SUSPENSION = 4;
            /** @const */
            var M_EXECUTIONS = 5;
            /** @const */
            var M_CHILD_EXECUTIONS = 6;
            /** @const */
            var M_START_TIME = 7;
            /** @const */
            var M_END_TIME = 8;
            /** @const */
            var M_CHILDREN = 9;
            /** @const */
            var M_COLLAPSE_LEVELS = 10;
            /** @const */
            var M_TAGS = 11;
            /** @const */
            var M_CATEGORY = 12;
            /** @const */
            var M_SIGNATURE = -2;
            /** @const */
            var M_NOT_COMPUTED = -3;
            /** @const */
            var M_COMPUTATOR = -4;
            /** @const */
            var M_PREV_SELF_DURATION = 13;
            /** @const */
            var M_PREV_SELF_SUSPENSION = 14;
            /** @const */
            var M_PREV_EXECUTIONS = 15;

            /** @const */
            var P_ID = 0;
            /** @const */
            var P_DURATION = 1;
            /** @const */
            var P_EXECUTIONS = 2;
            /** @const */
            var P_VALUE = 3; // Array of values
            /** @const */
            var P_CHILDREN = 4;
            /** @const */
            var P_ID2ITEM_MAP = -1;
            /** @const */
            var P_PREV_DURATION = 5;

            /** @const */
            var ATTR_BUTTON_ID = 'aa';
            /** @const */
            var ATTR_NODE_COLLAPSE_LEVELS = 'ab';
            /** @const */
            var ATTR_TREE_ID = 'ac';
            /** @const */
            var ATTR_NODE_ID = 'ad';
            /** @const */
            var ATTR_NODE_IDX = 'ae';
            /** @const */
            var ATTR_NODE_COLLAPSE_EXPANDED = 'af';
            /** @const */
            var ATTR_NODE_LAZY = 'ag';
            /** @const */
            var ATTR_NODE_NO_GRAND_CHILDREN = 'ah';
            /** @const */
            var ATTR_NODE_NOT_COMPUTED = 'ai';
            /** @const */
            var ATTR_SUBTREE_ID = 'aj';
            /** @const */
            var ATTR_NODE_UNFOLDED = 'ak';

            /** @const */
            var BUTTON_ID_FOLD_UNFOLD = 'a';
            /** @const */
            var BUTTON_ID_ZERO_METHOD = 'b';
            /** @const */
            var BUTTON_ID_COLLAPSE = 'c';
            /** @const */
            var BUTTON_ID_MENU = 'd';

            /** @const */
            var DOM_TREE_FILTER_INPUT = 'q';
            /** @const */
            var DOM_TREE_FILTERED_CONTAINER = 'f';
            /** @const */
            var DOM_TREE_CONTAINER = 't';
            /** @const */
            var DOM_TREE_FILTER_SUMMARY = 'g';

            /** @const */
            var DOM_CLASS_TOOLTIP = 'ca';
            /** @const */
            var DOM_CLASS_HELP_LINK = 'cb';
            /** @const */
            var DOM_CLASS_TREE_NUMBERS = 'cc';

            /** @const */
            var DOM_PARALLEL_CLASS_TREE_NUMBERS = 'cg';

            var TREE_GROUP_TAG = '<div class="q">', TREE_GROUP_TAG_HIDDEN = '<div class="q" style="display:none">', TREE_ITEM_TAG = '<var><div ';
            var TREE_GROUP_TAG_CLOSE = '</div>';
            var TREE_ITEM_TAG_CLOSE = '</div></var>';
            var PBAR_START = '<span class=pbar><img src="data:image/gif;base64,R0lGODlhAQABAID/AMDAwAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==" height=1 width=';

            if ($.browser.msie || $.browser.mozilla) { // otherwise copy&paste to Word does not work
                TREE_GROUP_TAG = '<div style="margin:0 0 0 20px" class="q">';
                TREE_GROUP_TAG_HIDDEN = '<div style="margin:0 0 0 20px;display:none" class="q">';
                TREE_GROUP_TAG_CLOSE = '</div>';
                TREE_ITEM_TAG = '<span style="margin:1px;display:block"><div';
                TREE_ITEM_TAG_CLOSE = '</div></span>';
                PBAR_START = '<span style="vertical-align:3px;font-size:5px;background:#8b0000;"><img src="data:image/gif;base64,R0lGODlhAQABAPABAP///wAAACH5BAEKAAAALAAAAAABAAEAAAICRAEAOw==" height=1 width=';
            }


            /** @const */
            var CallTree_FILTERED_TREE = 'a';
            /** @const */
            var CallTree_FILTERED_QUERY = 'b';
            /** @const */
            var CallTree_FILTERED_RESULTS = 'numberofresults';

            /** @const */
            var CallTree_SUBTREE_ID = 'c';

            /** @const */
            var CallTree_TYPE_OUT_CALLS = 0;
            /** @const */
            var CallTree_TYPE_IN_CALLS = 1;
            /** @const */
            var CallTree_TYPE_FIND_USAGES = 2;

            /** @constructor */
            function CallTree(id, fw, type, reversed) {
                this.id = id;
                this.fw = fw; // Forward order (top-down)
                this.ty = type; // function used to generate the tree
                this.rv = reversed;
            }

            function CallTree__getTree(id) {
                var tree_id, subtree_id;
                if (id instanceof Array) {
                    tree_id = id[0];
                    subtree_id = id[1];
                } else
                    tree_id = id;
                var tree = trees[tree_id];
                if (tree === undefined || subtree_id == undefined) return tree;
                return tree[subtree_id];
            }

            function CallTree_cleanCategories(node) {
                delete node[M_CATEGORY];

                var t = node[M_CHILDREN];
                if (!t || t.length == 0) return;

                for (var i = t.length - 1; i >= 0; i--)
                    CallTree_cleanCategories(t[i]);
            }

            function CallTree_refreshCategories(node, bc, prevCollapse) {
                var nodeId = node[M_ID];

                var tag = tags.t[nodeId];
                var newBc = tag[T_CATEGORY];
                if (newBc !== undefined && newBc !== bc && tag[T_CATEGORY_ACTIVE] == BC_CURRENT) {
                    node[M_CATEGORY] = newBc;
                    bc = newBc;
                }

                var t = node[M_CHILDREN];
                if (!t || t.length == 0) return;
                var i;

                var collapseLevels = node[M_COLLAPSE_LEVELS], child;
                if (newBc === undefined || newBc === bc) {
                    if (prevCollapse > 0 && collapseLevels == 0) {
                        // Node that introduces category might be hidden by collapse
                        // so we explicitly set category for each children
                        for (i = t.length - 1; i >= 0; i--) {
                            child = t[i];
                            child[M_CATEGORY] = bc;
                            CallTree_refreshCategories(child, bc, collapseLevels);
                        }
                    } else
                        for (i = t.length - 1; i >= 0; i--)
                            CallTree_refreshCategories(t[i], bc, collapseLevels);
                } else
                    for (i = t.length - 1; i >= 0; i--) {
                        child = t[i];
                        child[M_CATEGORY] = newBc;
                        CallTree_refreshCategories(child, newBc, collapseLevels);
                    }
            }

            var tags = CT.tags = new Tags();
            var sqls = CT.sqls = {}; // Hash of sql's (file index -> {file offset -> sql string})
            var xmls = CT.xmls = {}; // Hash of big params like xml (file index -> {file offset -> sql string})
            var trees = CT.trees = []; // tree id -> CallTree
            var res = []; // temporary array for innnerHTML
            var $notify;
            CT.defaultCategories = [
                '# Custom code from OSSJ',
                'solutions.ossj com.netcracker.applications.ossj.base.controller.AbstractBusinessController.invoke',
                '',
                '# Orchestrator',
                'orchestrator.mdb com.netcracker.platform.utils.tools.queueservice.QueueInvokerMDB',
                'orchestrator.core com.netcracker.platform.orchestrator.core.orm.SafeLifecycleManager',
                'orchestrator.manual_task com.netcracker.platform.orchestrator.usertask.OrchestratorUserTaskOperations.prepareUserTask',
                '',
                '# Custom code called from orchestrator',
                'solutions.orchestrator.sync >com.netcracker.platform.orchestrator.core.actions.synchronous.SynchronousJavaActionExecutor.executeAction',
                'solutions.orchestrator.async >com.netcracker.platform.orchestrator.core.actions.asynchronous.AsynchronousJavaActionExecutor.executeAction',
                'solutions.orchestrator.manual_task >com.netcracker.platform.orchestrator.core.actions.manualtask.ManualTaskActionExecutor.executeAction',
                'solutions.orchestrator.js_condition >com.netcracker.platform.orchestrator.core.scripting.ProcessBasedScriptingEngine.execute',
                '',
                '# Workflow',
                'workflow.engine.unsorted com.netcracker.ejb.wf.interface2.WfSubscriberSysJMSBean.onMessage',
                'workflow.engine.unsorted >com.netcracker.platform.orchestrator.core.actions.workflow.WorkflowExecutor.executeAction',
                'workflow.engine.load_opeartions_cache com.netcracker.ejb.wf.engine.processor.OperationCache.reloadNames2NodesCache',
                'workflow.engine.load_opeartions_cache com.netcracker.ejb.wf.engine.processor.OperationCache.getOperationNodes',

                '',
                '# Custom Workflow code',
                'solutions.workflow >com.netcracker.ejb.wf.engine.processor.ActionProcessor.invoke',
                'solutions.workflow >com.netcracker.ejb.wf.engine.processor.ActionProcessor.execute',
                '',
                '# Custom JSP code',
                'solutions.jsp.unsorted com.netcracker.jsp.Sheet.service',
                'solutions.jsp.get_data Provider.getObjectsByRequestWithMP',
                'solutions.jsp.get_data Provider.getRecordCount',
                'solutions.jsp.sort_data com.netcracker.ejb.session.common.CommonProviderBean.sort',
                'solutions.jsp.calculate_attributes com.netcracker.ejb.session.common.providers.ProviderSceleton.calculate',
                'solutions.jsp.calculate_attributes com.netcracker.ejb.session.common.ProviderHelper.calculate',
                'solutions.jsp.compute_filters com.netcracker.ejb.session.common.CommonProviderBean.getAvailableValues',
                'solutions.jsp.compute_filters solutions.jsp.compute_filters com.netcracker.ejb.session.common.providers.PortContextProvider.ensureAvalibleValues',
                'solutions.cbtui.unsorted com.netcracker.presentation.config.ActionNode.perform',
                '',
                '# JSP framework',
                'tui.jsp.print_html com.netcracker.jsp.Sheet.printContent',
                'tui.jsp.print_object_parameters com.netcracker.jsp.uni.control.ParameterCtrl.newPrint',
                'tui.jsp.unsorted com.netcracker.jsp.CommonPage.debugSecureService',
                'tui.jsp.calculate_page_structure.determine_header com.netcracker.jsp.BasicDesign.getHead',
                'tui.jsp.calculate_page_structure.determine_sheets com.netcracker.ejb.session.common.CommonProviderBean.init',
                'tui.jsp.calculate_page_structure.determine_sheets com.netcracker.jsp.ModernPage.setMainObject',
                'tui.jsp.calculate_page_structure.determine_columns com.netcracker.ejb.session.common.CommonProviderBean.getObjectsDisplayedAttributes',
                'tui.jsp.calculate_page_structure.build_navigation_menu com.netcracker.jsp.ModernPage.fillDepartments',
                'tui.jsp.calculate_page_structure.build_object_path com.netcracker.jsp.path.PathBuilder',
                'tui.jsp.data_to_xml com.netcracker.ejb.session.common.CommonProviderBean.buildXMLForObjects',
                'tui.jsp.check_security solutions.jsp.check_security com.netcracker.ejb.session.common.ProviderHelper.filterByPermissions',
                'cbtui.unsorted com.netcracker.presentation.process.NCControllerServlet.service',
                '',
                '# Search',
                'search.calculate_page_structure.determine_profiles com.netcracker.platform.search.jsp.facade.FavoriteProfilesFacade.getFavoriteProfilesModel',
                '',
                '# Custom job',
                'solutions.job >com.netcracker.platform.scheduler.impl.jobs.AbstractJobImpl$4.run',
                '',
                '# Scheduler',
                'scheduler com.netcracker.platform.scheduler.impl.jobs.AbstractJobImpl.execute',
            ].join('\n');

            CT.append = function(fw, bw) {
                var treeId = trees.length;
                var tree = new CallTree(treeId, fw, CallTree_TYPE_OUT_CALLS);
                return trees[treeId] = tree;
            };

            CT.filter = function (input, event) {
                var tree_id = parseInt(input.id.substr(DOM_TREE_FILTERED_CONTAINER.length));
                var nonFiltered = trees[tree_id];

                var prevTree = nonFiltered[CallTree_FILTERED_TREE];
                var prevValue = prevTree ? prevTree[CallTree_FILTERED_QUERY] : '';

                var newValue = $.trim($(input).val());

                var delay = 0;
                if (prevValue.lastIndexOf(newValue, 0) == 0 ||
                    newValue.lastIndexOf(prevValue, 0) == 0)
                    delay += 400;

                if (newValue.length < 5)
                    delay += 200;

                if (Math.abs(prevValue.length - newValue.length) < 3)
                    delay += 150;

                function performFilter() {
                    var $input = $(input);
                    if (event && event.keyCode == 27)
                        $input.val('');

                    var value = $input.val();

                    var trimmed = $.trim(value);

                    if (prevTree && prevTree[CallTree_FILTERED_QUERY] == trimmed && event)
                        return;

                    adjustWidth($input, value, 200, 1200, 100);

                    var $filtered = $('#' + DOM_TREE_FILTERED_CONTAINER + tree_id);
                    var $nonFiltered = $('#' + DOM_TREE_CONTAINER + tree_id);
                    if (!trimmed || trimmed.length == 0) {
                        $filtered.css({display: 'none'}).html('');
                        $nonFiltered.css({display: 'block'});
                        delete nonFiltered[CallTree_FILTERED_TREE];
                        $('#' + DOM_TREE_FILTER_SUMMARY + tree_id).text('');
                        return;
                    }

                    var html;
                    var tree = Tree__createFilteredTree(nonFiltered, trimmed);
                    if (tree && tree.fw && tree.fw[M_CHILDREN] && tree.fw[M_CHILDREN].length) {
                        tree.id = tree_id;
                        tree[CallTree_FILTERED_QUERY] = trimmed;
                        tree[CallTree_SUBTREE_ID] = CallTree_FILTERED_TREE;
                        html = renderTreeNoWrap(tree).join('');
                    } else {
                        html = 'No nodes found :-/';
                    }

                    nonFiltered[CallTree_FILTERED_TREE] = tree;
                    $nonFiltered.css({display: 'none'});
                    if (tree) {
                        var results = tree[CallTree_FILTERED_RESULTS];
                        if (results == 0)
                            results = 'no results found';
                        else
                            results = Integer__format(results) + ' result' + (results > 1 ? 's' : '');
                        $('#' + DOM_TREE_FILTER_SUMMARY + tree_id).text(results);
                    }
                    $filtered.html(html).css({display: 'block'});
                };

                invokeLater(CT.filter, function () {
                    if (newValue == prevValue)
                        $('#loading').hide();
                    else
                        $('#loading').show();

                    invokeLater(performFilter, function () {
                        performFilter();
                        $('#loading').hide();
                    }, delay / 2);
                }, delay / 2);
            };

            CT.activateTab = function(tabId) {
                var $tabs = $('#tabs');
                $('a[href="#'+tab_id+'"]').click();
                $tabs[0].scrollIntoView();
                return false;
            };

            CT.updateFormatFromPersonalSettings = updateFormatFromPersonalSettings;

            function Tree$findNodePath(li, skipFirstCollapse) {
                var nodePath = [];
                while (true) {
                    var collapseLevels = li.attr(ATTR_NODE_COLLAPSE_LEVELS);
                    if (collapseLevels && !li.attr(ATTR_NODE_COLLAPSE_EXPANDED) && (!skipFirstCollapse || nodePath.length > 0)) {
                        collapseLevels = parseInt(collapseLevels);
                        for (; collapseLevels > 0; collapseLevels--)
                            nodePath[nodePath.length] = 0;
                    }
                    var idx = li.attr(ATTR_NODE_IDX);
                    if (!idx) break;

                    nodePath[nodePath.length] = parseInt(idx);
                    li = li.parent().parent().parent();
                }
                var treeId = parseInt(li.attr(ATTR_TREE_ID));
                return [
                    [treeId, li.attr(ATTR_SUBTREE_ID)],
                    nodePath
                ];
            }

            function Tree$getNodeByPath(tree, nodePath, targetLevel) {
                var node = tree.fw;
                if (targetLevel == undefined)
                    targetLevel = 0;
                for (var i = nodePath.length - 1; i >= targetLevel; i--)
                    node = node[M_CHILDREN][nodePath[i]];
                return node;
            }

            function Tree$each(tree, nodePath, fn) {
                var node = tree.fw;
                for (var i = nodePath.length - 1; i >= 0; i--) {
                    if (fn(node, i)) return;
                    node = node[M_CHILDREN][nodePath[i]];
                }
                fn(node, 0);
            }

            function Tree$findNode(li, skipFirstCollapse) {
                var path = Tree$findNodePath(li, skipFirstCollapse);
                var treeId = path[0];
                var nodePath = path[1];

                var tree = CallTree__getTree(treeId);
                var node = Tree$getNodeByPath(tree, nodePath);

                return [tree, node, nodePath, 0];
            }

            var javaModules = [
                ['logging.log4j', 'org.apache.log4j'],
                ['logging.commons', 'org.apache.commons.logging'],

                ['db', 'org.postgresql.'],

                ['profiler', 'com.netcracker.profiler.']

                ['org.quartz', 'org.quartz.'],

                ['java.reflection', 'java.lang.Class'],
                ['java.other', 'java.'],

                ['other','other']
            ];

            javaModules.sort(function(a, b) {
                return b[1].length - a[1].length;
            });

            if (!app.sign) app.sign = {};

            var signatures = app.sign;

            signatures.sql = function(sql) {
                return sql.replace(/,/g, '').replace(/'(?:''|[^'])*'/g, '').replace(/\d+/g, '').replace(/(\w)\w*/g, '$1').replace(/\s+/g, '');
            };

            signatures.empty = function() {
                return '';
            };

            signatures.binds = signatures.sql;

            var $estimateWidthSpan;

            function adjustWidth($input, text, min, max, gap) {
                if (!$estimateWidthSpan) {
                    $estimateWidthSpan = $('<div>');
                    $estimateWidthSpan.css({
                        position: 'absolute',
                        top: -9999,
                        left: -9999,
                        width: 'auto',
                        fontSize: $input.css('fontSize'),
                        fontFamily: $input.css('fontFamily'),
                        fontWeight: $input.css('fontWeight'),
                        letterSpacing: $input.css('letterSpacing'),
                        whiteSpace: 'nowrap'
                    });
                    $estimateWidthSpan.appendTo(document.body);
                }
                $estimateWidthSpan.text(text.replace(/ /g, '\xa0'));
                var width = $estimateWidthSpan.width() + gap;
                if (width < min) width = min;
                if (width > max) width = max;
                $input.width(width);
            }

            function getTagSignature(tag) {
                var tagId = tag[P_ID];
                var info = tags.y[tagId];
                if (info && (info = info[T_TYPE_SIGNATURE]) && (info = signatures[info])) {
                    const hash = new FNV1a128();
                    for (const subvalue of tag[P_VALUE]) {
                        const subvalueSignature = info(subvalue);
                        hash.update(subvalueSignature);
                    }
                    return hash.finalize();
                }
                return '';
            }

            function getMethodSignature(node) {
                var t = node[M_TAGS];
                if (!t || t.length == 0) return '';
                var nodeSignature = node[M_SIGNATURE];
                if (nodeSignature) return nodeSignature;
                var s = {};
                for (var i = 0; i < t.length; i++) {
                    var tag = t[i];
                    var tagId = tag[P_ID];
                    s[tagId + '.' + getTagSignature(tag)] = true;
                }
                var sign = [];
                for (var k in s)
                    sign[sign.length] = k;
                sign.sort();
                return node[M_SIGNATURE] = sign.join(';');
            }

            var CallTree_render = CT.render = function(tree) {
                if (tree.id == 0) {
                    if (app.args && app.args.ro) {
                        $('#download').hide();
                        var args = app.args;
                        var tabs = {}, tabsDetected = false;
                        for (var k in args)
                            if (k.charAt(0) == 'z') {
                                tabs[k] = args[k];
                                tabsDetected = true;
                            }

                        if (tabsDetected)
                            pushState(tabs);
                    }
                }
                if (tree.id == 0 && tree.fw) {
                    var firstNode = tree.fw;
                    var dateFormatted = Date__format(new Date(firstNode[M_START_TIME]));
                    var name = dateFormatted;
                    name += '_' + Duration__formatTime(firstNode[M_DURATION]);
                    var callsFormatted = (firstNode[M_EXECUTIONS] + firstNode[M_CHILD_EXECUTIONS]) + 'calls';
                    name += '_' + callsFormatted;
                    var jmeter_id = tags.r['jmeter.step'], jmeter_step;
                    if (jmeter_id) {
                        var pars = Tree__gatherIndexedParams(tree, 4).fw[M_TAGS];
                        for (var i = 0; i < pars.length && i < 1000; i++) {
                            var pi = pars[i];
                            if (pi[P_ID] == jmeter_id) {
                                jmeter_step = pi[P_VALUE].replace(/[/\\?%*:|"<> ]+/g, '_').replace(/^\.+|\.+$/g, '');
                                break;
                            }
                        }
                    }
                    var tag = tags.t[firstNode[M_ID]], tag_name;
                    if (tag[T_CLASS])
                        tag_name = tag[T_CLASS] + '.' + tag[T_METHOD];
                    else
                        tag_name = tag[0];
                    if (jmeter_step && (
                        tag_name == 'WebAppServletContext$ServletInvocationAction.run'
                            || tag_name == 'ServletRequestImpl.run'
                            || tag_name == 'StandardEngineValve.invoke'))
                        tag_name = 'servlet';
                    if (jmeter_step)
                        name += '_' + encodeURIComponent(jmeter_step);
                    if (tag_name)
                        name += '_' + tag_name;
                    app.dn = name.replace(/\s+/g, '') + '.zip';
                    $('#download').attr('href', 'tree/' + app.dn + '?' + app.du);
                    $('#download').click(function(e){
                        $.doPost('tree/' + app.dn,
                            $.extend({}, app.args
                                , {pageState: $.param(getState()), callback: 'treedata'}
                                , {adjustDuration: Tree__adjustDuration_value, businessCategories: Tree__setupBc_value}
                            )
                        );
                        return false;
                    });
                    document.title = Duration__formatTime(firstNode[M_DURATION]) + ' ' + getServerName() + ' ' + dateFormatted + ' ' + callsFormatted;
                }

                var html = renderTree(tree).join('');
                $('#callTree').html(html);
                $('#loading').hide();
                CallTree__checkTabRequiresRender(selected_tab_id);
                if (tree.id != 0) return;

                $(window).trigger('hashchange');
            };

            /** @const */
            var BC_CURRENT = 0;
            /** @const */
            var BC_NEXT = 1;

            /** @const */
            var BC_COLOR = 0;
            /** @const */
            var BC_NAME = 1;
            /** @const */
            var BC_ACTIVE_NODE = 2;

            /** @const */
            var BC_CTX_BC = 0;
            /** @const */
            var BC_CTX_RECURSIVE_NODES = 1;
            /** @const */
            var BC_CTX_FLAT_PROFILE = 2;
            /** @const */
            var BC_CTX_NODE_TO_TIMES = 3;

            var nextId = -100;

            function CallTree_renderHotspots(tree, treeId, dstSelector, finalizer) {
                if (!dstSelector) dstSelector = '#hotspots';
                if (!treeId) treeId = 5;
                var dq = 0;
                var i;
                for (i = 0; i < javaModules.length; i++) {
                    javaModules[i][2] = new RegExp('^' + javaModules[i][1].replace(/\./g, '\\.'));
                }

                var flat, maxIndex;
                var currentCategory, currentIndex, currentResult = {};
                var shownTime = 0;

                var dqq = new Date().getTime();

                i = 0;
                setTimeout(nextIteration, 0);

                function nextIteration() {
                    dq -= new Date().getTime();
                    var qq = new Date().getTime() + 32;
                    if (!flat) {
                        var flatProfile = computeFlatProfile(tree);
                        flat = flatProfile[0];
                        maxIndex = flatProfile[1];
                    } else {
                        for (; i < maxIndex && new Date().getTime() <= qq;) {
                            // When unprocessed nodes exist or under time slice budget

                            if (currentCategory == undefined) {
                                for (var bc in flat) {
                                    currentCategory = flat[bc];
                                    delete flat[bc];
                                    break;
                                }
                                if (currentCategory === undefined) {
                                    alert([i, maxIndex]);
                                }
                                currentIndex = currentCategory[BC_CTX_FLAT_PROFILE].length - 1;
                            }

                            var profile = currentCategory[BC_CTX_FLAT_PROFILE];
                            var currentBc = currentCategory[BC_CTX_BC];
                            for (; currentIndex >= 0 && new Date().getTime() <= qq; i++,currentIndex--) { // currentIndex moves to 0
                                var x = profile[currentIndex], x_id = x[M_ID];

                                var m = getTagModule(x_id, javaModules);
                                var root = currentResult[m];
                                if (!root)
                                    root = currentResult[m] = Tree__createNode(undefined);

                                x[M_NOT_COMPUTED] = 1;
                                x[M_COMPUTATOR] = Tree__computeIncoming;
                                x[M_CATEGORY] = currentBc;
                                root[M_SELF_DURATION] += x[M_SELF_DURATION];
                                root[M_SELF_SUSPENSION] += x[M_SELF_SUSPENSION];
                                root[M_EXECUTIONS] += x[M_EXECUTIONS];
                                var children = root[M_CHILDREN];
                                children[children.length] = x;

                                shownTime += x[M_SELF_DURATION];
                            }

                            if (currentIndex == -1) { // finished category
                                mergeCategoryResults(currentBc, currentResult);

                                currentCategory = undefined; // try next category later
                                currentResult = {};
                            }
                        }
                    }
                    dq += new Date().getTime();
                    if (i == maxIndex) {
                        $('#hsStatus,#hsBar').remove();
                        finalizeHotspots();
                    } else {
                        $('#hsStatus').html('Computed ' + (90 * i / maxIndex).toFixed(1) + '%');
                        $('#hsBar').progressbar({value: 90 * i / maxIndex});
                        setTimeout(nextIteration, 0);
                    }
                }

                var all = Tree__createNode(-2);
                var id2node = {};
                var id2parentId = {};
                var group2node = {'': all};
                id2node[all[M_ID]] = all;

                function allocateGroupNode(groupName, node) {
                    var dstNode = group2node[groupName];
                    if (dstNode) return dstNode;

                    var nodeId = nextId--;
                    tags.a(nodeId, groupName);

                    var lastDot = groupName.lastIndexOf('.');
                    var title;
                    if (lastDot >= 0) {
                        title = groupName.substring(lastDot + 1);
                        groupName = groupName.substring(0, lastDot);
                    } else {
                        title = groupName;
                        groupName = '';
                    }

                    tags.t[nodeId][T_HTML] = '<b>' + title + '</b>';

                    if (!node)
                        dstNode = Tree__createNode(nodeId);
                    else {
                        node[M_ID] = nodeId;
                        dstNode = node;
                    }

                    id2node[nodeId] = dstNode;

                    var parentNode = allocateGroupNode(groupName);
                    var c = parentNode[M_CHILDREN];
                    c[c.length] = dstNode;
                    id2parentId[nodeId] = parentNode[M_ID];
                    group2node[groupName] = parentNode;

                    return dstNode;
                }

                function mergeCategoryResults(bc, currentResult) {
                    var bcName = bc[BC_NAME].toUpperCase();
                    var tmp = bcName + '.';
                    for (var j in currentResult) {
                        var res_j = currentResult[j];
                        var duration = res_j[M_DURATION] = res_j[M_SELF_DURATION];
                        var suspension = res_j[M_SUSPENSION] = res_j[M_SELF_SUSPENSION];
                        var executions = res_j[M_EXECUTIONS];
                        res_j[M_CHILD_EXECUTIONS] = 0;

                        allocateGroupNode(tmp + j, res_j);

                        for (var nodeId = res_j[M_ID]; nodeId = id2parentId[nodeId];) {
                            var dstNode = id2node[nodeId];
                            dstNode[M_DURATION] += duration;
                            dstNode[M_SELF_DURATION] += duration;
                            dstNode[M_SUSPENSION] += suspension;
                            dstNode[M_SELF_SUSPENSION] += suspension;
                            dstNode[M_EXECUTIONS] += executions;
                        }
                    }
                    var groupNode = group2node[bcName];
                    if (groupNode) // it might not exist when currentResult is empty
                        groupNode[M_CATEGORY] = bc;
                }

                function finalizeHotspots() {
                    sortNode(all, orderCallsBySelfDuration);
                    for (var k in id2node) {
                        id2node[k][M_COLLAPSE_LEVELS] = 0;
                    }
                    all[M_COLLAPSE_LEVELS] = 0;
                    var hsTree = trees[treeId] = new CallTree(treeId, all, CallTree_TYPE_IN_CALLS, true);
                    hsTree.srcTree = tree;

                    var html = [];
                    if (finalizer) finalizer(hsTree, html);

                    if (maxIndex < flat.length) {
                        html[html.length] = flat.length - maxIndex;
                        html[html.length] = flat.length - maxIndex == 1 ? ' method with 0ms self time is' : ' methods with 0ms self time are';
                        html[html.length] = ' not displayed here. ';
                    }
                    html[html.length] = '<!--Calculation took ';
                    html[html.length] = Duration__formatTime(new Date().getTime() - dqq);
                    html[html.length] = ' (computation ';
                    html[html.length] = Duration__formatTime(dq);
                    html[html.length] = ').-->';

                    html = renderTree(hsTree, html).join('');

                    $(dstSelector).html(html);
                    $('#loading').hide();
                }
            }

            function CallTree_renderParams(tree) {
                var paramsTree = Tree__gatherIndexedParams(tree);
                var html = renderNodeChilds(paramsTree, paramsTree.fw).join('');

                $('#params').html(html);
                $('#loading').hide();
            }

            function renderTree(tree, html) {
                if (!html) html = [];
                html[html.length] = 'Search: <input type="search" id="' + DOM_TREE_FILTER_INPUT + tree.id + '" style="width:200px" onkeyup="CT.filter(this,event);" onchange="CT.filter(this,event);" onsearch="CT.filter(this,event);">';
                html[html.length] = ' <span id=' + DOM_TREE_FILTER_SUMMARY + tree.id + '></span>';
                html[html.length] = '<div id=' + DOM_TREE_FILTERED_CONTAINER + tree.id + '></div>';
                html[html.length] = '<div id=' + DOM_TREE_CONTAINER + tree.id + '>';
                renderTreeNoWrap(tree, html);
                html[html.length] = '</div>';
                return html;
            }

            function renderTreeNoWrap(tree, html) {
                if (!html) html = [];
                var node = tree.fw;
                html[html.length] = '<div ' + ATTR_TREE_ID + '=';
                html[html.length] = tree.id;
                var subTreeId = tree[CallTree_SUBTREE_ID];
                if (subTreeId) {
                    html[html.length] = ' ' + ATTR_SUBTREE_ID + '=';
                    html[html.length] = subTreeId;
                }
                var collapseLevels = node[M_COLLAPSE_LEVELS];
                if (collapseLevels) {
                    html[html.length] = ' ' + ATTR_NODE_COLLAPSE_LEVELS + '="';
                    html[html.length] = collapseLevels + '"';
                }
                html[html.length] = '>';

                treeBeingRendered = tree;
                duration_bar_only_for_self_time = tree.rv;
                var cutoffDuration, cutoffCalls, scale;
                if (duration_bar_only_for_self_time) {
                    scale = node[M_SELF_DURATION];
                    cutoffCalls = node[M_EXECUTIONS] * 0.1;
                } else {
                    scale = node[M_DURATION];
                    cutoffCalls = (node[M_CHILD_EXECUTIONS] + node[M_EXECUTIONS]) * 0.1;
                }
                cutoffDuration = scale * 0.1;
                if (scale > 0) scale = 30 / scale;
                treeNode2html(html, node, undefined, cutoffDuration, cutoffCalls, scale);

                html[html.length] = '</div>';
                return html;
            }

            function renderNodeChilds(tree, node, uncollapsed) {
                var html = [];

                treeBeingRendered = tree;
                duration_bar_only_for_self_time = tree.rv;
                var cutoffDuration, cutoffCalls, scale;
                if (duration_bar_only_for_self_time) {
                    scale = tree.fw[M_SELF_DURATION];
                    cutoffDuration = node[M_SELF_DURATION];
                    cutoffCalls = node[M_EXECUTIONS] * 0.1;
                } else {
                    scale = tree.fw[M_DURATION];
                    cutoffDuration = node[M_DURATION];
                    cutoffCalls = (node[M_CHILD_EXECUTIONS] + node[M_EXECUTIONS]) * 0.1;
                }
                cutoffDuration *= 0.1;
                if (scale > 0) scale = 30 / scale;
                treeNodeChilds2html(html, node, cutoffDuration, cutoffCalls, scale, uncollapsed);

                return html;
            }

            function renderNodeSmallChilds(tree, node, initialIndex, html) {
                if (!html) html = [];
                var c = node[M_CHILDREN];
                if (!c || !c.length) return html;

                treeBeingRendered = tree;
                duration_bar_only_for_self_time = tree.rv;
                var cutoffDuration, cutoffCalls, scale;
                if (duration_bar_only_for_self_time) {
                    scale = tree.fw[M_SELF_DURATION];
                    cutoffCalls = tree.fw[M_EXECUTIONS] * 0.1;
                } else {
                    scale = tree.fw[M_DURATION];
                    cutoffCalls = (tree.fw[M_CHILD_EXECUTIONS] + tree.fw[M_EXECUTIONS]) * 0.1;
                }
                cutoffDuration = scale * 0.1;
                if (scale > 0) scale = 30 / scale;

                var i = initialIndex;
                if (i == c.length) return html;

                for (; i < c.length; i++) {
                    var child = c[i];
                    html[html.length] = TREE_ITEM_TAG;
                    if (!duration_bar_only_for_self_time) {
                        var bc = child[M_CATEGORY];
                        if (bc) html[html.length] = ' style="background-color:' + bc[BC_COLOR] + '"';
                    }
                    html[html.length] = ' ' + ATTR_NODE_IDX + '="';
                    html[html.length] = i + '"';
                    var collapseLevels = child[M_COLLAPSE_LEVELS];
                    if (collapseLevels) {
                        html[html.length] = ' ' + ATTR_NODE_COLLAPSE_LEVELS + '="';
                        html[html.length] = collapseLevels + '"';
                    }
                    html[html.length] = '>';
                    treeNode2html(html, child, node, cutoffDuration, cutoffCalls, scale);
                    html[html.length] = TREE_ITEM_TAG_CLOSE;
                }
                return html;
            }

            function jsonBeautify(str) {
                if (str && str.length > 60) {
                    try {
                        return vkbeautify.json(str, 2);
                    } catch (e) {
                    }
                }
                return str;
            }

            function printReformatted(html, value, lang, nocolor, classes, styles) {
                html[html.length] = '<pre class="prettyprint';
                if (classes) {
                    html[html.length] = ' ';
                    html[html.length] = classes;
                }
                html[html.length] = '"';
                if (styles) {
                    html[html.length] = ' style="';
                    html[html.length] = styles;
                    html[html.length] = '"';
                }
                html[html.length] = '>';
                var reformatted = false;
                var val;
                if (value.length > 60 && value.indexOf('\n') == -1) {
                    var beautified;
                    try {
                        beautified = vkbeautify[lang](value, 2);
                    } catch (e) {
                    }
                    if (beautified && beautified != value) {
                        reformatted = true;
                        html[html.length] = '<a href=# onclick="$(this).nextAll(\'span\').toggle(); $(this).text($(this).text()==\'view original\'?\'view reformatted\':\'view original\'); return false;">view original</a>';
                        html[html.length] = '<br><span style="display:none;">';
                        val = escapeHTML(value);
                        if (!nocolor) val = prettyPrintOne(val, lang);
                        html[html.length] = val;
                        html[html.length] = '</span><span>';
                        value = beautified;
                    }
                }
                val = escapeHTML(value);
                if (!nocolor) val = prettyPrintOne(val, lang);
                html[html.length] = val;
                if (reformatted)
                    html[html.length] = '</span>';
                html[html.length] = '</pre>';
                return html;
            }

            CT.printReformatted = printReformatted;

            function renderSimpleTag(html, tag, scale, expand, nocolor, transparent) {
                var tagName = tags.t[tag[P_ID]][0];
                var items = tag[P_CHILDREN];
                const values = tag[P_VALUE];
                if (items && items.length) {
                    html[html.length] = '<span class="ui-buttonset';
                    if (transparent)
                        html[html.length] = ' ui-state-disabled';
                    html[html.length] = '"><span ' + ATTR_BUTTON_ID + '="' + BUTTON_ID_FOLD_UNFOLD + '" class="uc ui-state-default ui-button ui-corner-left"';
                    html[html.length] = '><span class="ui-icon ui-icon-';
                    html[html.length] = expand ? 'minus' : 'plus';
                    html[html.length] = '"></span></span>';
                    html[html.length] = ' <span ' + ATTR_BUTTON_ID + '="' + BUTTON_ID_MENU + '" class="uc ui-state-default ui-button ui-corner-right"><span class="ui-icon ui-icon-triangle-1-s"></span></span></span>';
                }

                html[html.length] = Duration__formatTime(tag[P_DURATION]);
                html[html.length] = ' ';
                var t_count = tag[P_EXECUTIONS];
                if (t_count > 1) {
                    html[html.length] = ' - ';
                    html[html.length] = Integer__format(t_count);
                    html[html.length] = ' inv ';
                }
                html[html.length] = '<b>';
                html[html.length] = tagName;
                html[html.length] = '</b>';
                if (values !== null) {
                    html[html.length] = ': ';
                    html[html.length] = '<div style="display:inline-block; vertical-align: text-top;">';
                    if (items && items.length > 1) {
                    } else {
                        for (const value of values) {
                            if (value.length == app.args['params-trim-size']) {
                                html[html.length] = '<a href="';
                                if (app.args && app.args.ro) {
                                    html[html.length] = value._2;
                                    html[html.length] = '/';
                                    html[html.length] = value._0;
                                    html[html.length] = '_';
                                    html[html.length] = value._1;
                                    html[html.length] = value._2 == 'sql' ? '.sql' : '.txt';
                                } else {
                                    html[html.length] = 'get_clob/';
                                    html[html.length] = app.dn.replace(/.zip$/, '-' + value._0 + '-' + value._1 + '.' + tagName + '.zip');
                                    html[html.length] = '?dir=';
                                    for (var arg in app.args) {
                                        // TODO: support proper folder detection in aggregated tree case
                                        if (!/^f\[_/.test(arg))
                                            continue;
                                        html[html.length] = app.args[arg];
                                        break;
                                    }
                                    html[html.length] = '&file=';
                                    html[html.length] = value._0;
                                    html[html.length] = '&offs=';
                                    html[html.length] = value._1;
                                    html[html.length] = '&type=';
                                    html[html.length] = value._2;
                                }
                                html[html.length] = '" target=_blank><span class="ui-icon ui-icon-disk inline-block" style="vertical-align:text-bottom;"></span>';
                                if (!(app.args && app.args.ro)) {
                                    html[html.length] = 'Save as *.zip';
                                } else {
                                    html[html.length] = 'Open ';
                                    html[html.length] = value._2;
                                    html[html.length] = '/';
                                    html[html.length] = value._0;
                                    html[html.length] = '_';
                                    html[html.length] = value._1;
                                    html[html.length] = value._2 == 'sql' ? '.sql' : '.txt';
                                }
                                html[html.length] = '</a>';
                            }
                            var val;
                            var stealthMode;
                            if (tagName == 'sql' || tagName == 'sql.monitor' || tagName == 'binds' || tagName == 'mdx' || tagName == 'cassandra.query') {
                                printReformatted(html, value, tagName == 'binds' ? 'ignore_formatting' : 'sql', nocolor);
                            } else if (tagName.indexOf('xml') != -1 || value && value[0] == '<') {
                                printReformatted(html, value, 'xml', nocolor);
                            } else if (tagName == 'common.started' || tagName == 'jms.timestamp') {
                                html[html.length] = Date__formatWithMillis(new Date(Number(tag[P_VALUE])));
                                html[html.length] = ' (' + tag[P_VALUE] + ')';
                            } else if (tagName == 'log.written' || tagName == 'log.generated' || tagName == 'memory.allocated' ||
                                tagName.substr(0, 3) == 'io.')
                                html[html.length] = Bytes__format(tag[P_VALUE]);
                            else if (tagName == 'time.wait' || tagName == 'time.cpu' || tagName == 'time.queue.wait') {
                                var millis = Number(tag[P_VALUE]);
                                renderDurationBar(html, millis, scale);
                                html[html.length] = Duration__formatTime(millis);
                            } else if (tagName == 'exception' || /\.pre$/.test(tagName)) {
                                html[html.length] = '<pre>';
                                html[html.length] = escapeHTML(tag[P_VALUE]);
                                html[html.length] = '</pre>';
                            } else if (tagName == 'wf.process') {
                                html[html.length] = '<a target=_blank href="/tools/wf/wf_info.jsp?run=Run&id=';
                                html[html.length] = tag[P_VALUE];
                                html[html.length] = '">';
                                html[html.length] = tag[P_VALUE];
                                html[html.length] = ' (open /tools/wf/wf_info.jsp)</a>';
                            } else if (tagName == 'po.process' || tagName == 'job.id') {
                                html[html.length] = '<a target=_blank href="/ncobject.jsp?id=';
                                html[html.length] = tag[P_VALUE];
                                html[html.length] = '">';
                                html[html.length] = tag[P_VALUE];
                                html[html.length] = ' (open /ncobject.jsp)</a>';
                            } else if (tagName.indexOf('json') != -1) {
                                printReformatted(html, value, 'json', nocolor);
                            } else if (tagName == 'web.query') {
                                html[html.length] = '<pre class=prettyprint>';
                                html[html.length] = '<a href=# onclick="$(this).nextAll(\'span\').toggle(); $(this).text($(this).text()==\'view source\'?\'view decoded\':\'view source\'); return false;">view source</a>';
                                html[html.length] = '<br><span style="display:none;">';
                                html[html.length] = escapeHTML(value);
                                html[html.length] = '</span><span>';
                                var qs = new URLSearchParams(value);
                                var keys = new Set(qs.keys()).keys().toArray();
                                keys.sort();
                                for (var key_idx = 0; key_idx < keys.length; key_idx++) {
                                    html[html.length] = '<b>' + escapeHTML(keys[key_idx]) + '</b>';
                                    html[html.length] = GRAY_START + '=' + GRAY_END;
                                    const paramValues = qs.getAll(keys[key_idx]);
                                    for (var v_idx = 0; v_idx < paramValues.length; v_idx++) {
                                        var value_item = jsonBeautify(paramValues[v_idx]);
                                        value_item = escapeHTML(value_item);
                                        if (value_item.length > 15 && !nocolor)
                                            value_item = prettyPrintOne(value_item);
                                        paramValues[v_idx] = value_item;
                                    }
                                    html[html.length] = paramValues.join(GRAY_START + ';&nbsp;' + GRAY_END);
                                    html[html.length] = '<br>';
                                }
                                html[html.length] = '</span></pre>';
                            } else if (tagName == 'web.session.id') {
                                val = tag[P_VALUE];
                                html[html.length] = escapeHTML(val);
                                var m = val.match(/.*!(\d+)$/);
                                if (m && m[1]) {
                                    var sessionCreated = Number(m[1]);
                                    if (Math.abs(sessionCreated - new Date()) < 1000 * 3600 * 24 * 365 * 50 /* 50 years */) {
                                        html[html.length] = ' (created ' + Duration__formatTime(new Date() - sessionCreated) + ' ago, at ';
                                        html[html.length] = new Date(sessionCreated);
                                        html[html.length] = ')';
                                    }
                                }
                            } else if ((tagName == 'dataflow.stack' || tagName == 'dataflow.session') && tag[P_VALUE] != '::other') {
                                val = JSON.parse('{' + tag[P_VALUE] + '}');
                                stealthMode = !(val.i && val.i.length == 19);
                                if (!stealthMode || val.c) {
                                    html[html.length] = '<a target=_blank href="/ncobject.jsp?id=';
                                    html[html.length] = stealthMode ? val.c : val.i;
                                    html[html.length] = '">';
                                }
                                html[html.length] = val.n ? escapeHTML(val.n) : (val.i + ":" + val.c);
                                if (!stealthMode || val.c) {
                                    html[html.length] = ' (';
                                    html[html.length] = stealthMode ? 'configuration' : 'instance';
                                    html[html.length] = ')</a>';
                                }
                                if (!stealthMode && val.c) {
                                    html[html.length] = ', <a target=_blank href="/ncobject.jsp?id=';
                                    html[html.length] = val.c;
                                    html[html.length] = '">open configuration</a>';
                                }
                            } else if (tagName == 'profiler.title') {
                                html[html.length] = tag[P_VALUE];
                            } else
                                html[html.length] = escapeHTML(tag[P_VALUE]);

                            if (tagName == 'wf.process'
                                || tagName == 'wf.activity.wmo'
                                || tagName == 'po.process'
                                || tagName == 'job.id'
                                || tagName == 'web.session.id'
                                || tagName == 'common.started'
                                || tagName == 'jms.timestamp'
                                || tagName == 'dataflow.session'
                            ) {
                                var dateDiff = trees[0].fw[M_DURATION];
                                var dateTime;
                                var filterString = '';

                                if (tagName == 'common.started'
                                    || tagName == 'jms.timestamp'
                                )
                                    dateTime = Number(tag[P_VALUE]);
                                else
                                    dateTime = trees[0].fw[M_START_TIME];

                                if (tagName == 'wf.process'
                                    || tagName == 'wf.activity.wmo'
                                    || tagName == 'po.process'
                                    || tagName == 'job.id'
                                    || tagName == 'web.session.id'
                                )
                                    filterString = encodeURIComponent(tag[P_VALUE]);
                                if (tagName == 'dataflow.session' && val
                                ) {
                                    filterString = encodeURIComponent(val.i);
                                }

                                html[html.length] = ' <a target=_blank href="index.html#timerange%5Bmin%5D=' + Math.round(dateTime - dateDiff * 1.2) + '&timerange%5Bmax%5D=' + Math.round(dateTime + dateDiff * 2.2) + '&duration%5Bmin%5D=' +
                                    Math.round(trees[0].fw[M_DURATION] / 4) +
                                    '&q=' + filterString +
                                    '">';
                                html[html.length] = 'Open list of profiller calls for relevant timeframe</a>';
                            }
                        }
                    }
                    html[html.length] = '</div>';
                }

                if (!items) return;
                if (items.length == 1) { // should never happen
                    renderSimpleTag(html, items[0], scale, false, true, transparent);
                    return;
                }
                html[html.length] = expand ? TREE_GROUP_TAG : TREE_GROUP_TAG_HIDDEN;
                for (var i = 0; i < items.length; i++) {
                    var item = items[i];
                    html[html.length] = TREE_ITEM_TAG;
                    html[html.length] = '>';
                    renderSimpleTag(html, item, scale, false, values && i > 4, transparent);
                    html[html.length] = TREE_ITEM_TAG_CLOSE;
                }
                html[html.length] = TREE_GROUP_TAG_CLOSE;
            }

            function treeNodeChilds2html(html, x, cutoffDuration, cutoffCalls, scale, uncollapsed) {
                var c = x[M_CHILDREN], t = x[M_TAGS], j, k, n;
                if ((!t || !t.length) && (!c || !c.length)) return;
                html[html.length] = TREE_GROUP_TAG;
                if (t && t.length > 0) {
                    if (t.length == 1) {
                        html[html.length] = TREE_ITEM_TAG;
                        html[html.length] = '>';
                        renderSimpleTag(html, t[0], scale, 1);
                        html[html.length] = TREE_ITEM_TAG_CLOSE;
                    } else {
                        var tag2group = {}, group2items, items;
                        for (j = 0; j < t.length; j++) {
                            var tag = t[j];
                            var tagId = tag[P_ID];
                            group2items = tag2group[tagId];
                            if (!group2items)
                                group2items = tag2group[tagId] = {};

                            var sign = getTagSignature(tag);

                            items = group2items[sign];
                            if (!items)
                                items = group2items[sign] = [];
                            items[items.length] = tag;
                        }
                        var tag2groupArray = [];
                        for (var t_id in tag2group) {
                            var tid = Number(t_id);
                            group2items = tag2group[tid];
                            var group2itemsArray = [];
                            var dur = 0, execs = 0;
                            for (var gid in group2items) {
                                items = group2items[gid];
                                var groupDur = 0, groupExec = 0;
                                for (j = 0; j < items.length; j++) {
                                    var items_j = items[j];
                                    groupDur += items_j[P_DURATION];
                                    groupExec += items_j[P_EXECUTIONS];
                                }
                                if (items.length === 1) {
                                    group2itemsArray[group2itemsArray.length] = items[0];
                                } else {
                                    items.sort(orderTagsByDuration);
                                    group2itemsArray[group2itemsArray.length] = [tid, groupDur, groupExec, items[0][P_VALUE], items];
                                }
                                dur += groupDur;
                                execs += groupExec;
                            }
                            if (group2itemsArray.length === 1) {
                                tag2groupArray[tag2groupArray.length] = group2itemsArray[0];
                            } else {
                                group2itemsArray.sort(orderTagsByDuration);
                                tag2groupArray[tag2groupArray.length] = [tid, dur, execs, null, group2itemsArray];
                            }
                        }

                        var transparent = treeBeingRendered[CallTree_SUBTREE_ID] && x[M_PREV_SELF_DURATION] != -2;
                        var coloredLength = 0;
                        for (j = 0; j < tag2groupArray.length; j++) {
                            var tag_j = tag2groupArray[j];
                            html[html.length] = TREE_ITEM_TAG;
                            html[html.length] = '>';
                            if (tag_j[P_VALUE] && coloredLength < 10000)
                                coloredLength += tag_j[P_VALUE].length;
                            renderSimpleTag(html, tag_j, scale, tag_j[P_CHILDREN] && tag_j[P_CHILDREN].length <= 5, coloredLength >= 10000, transparent);
                            html[html.length] = TREE_ITEM_TAG_CLOSE;
                        }
                    }
                }
                if (c && c.length > 0) {
                    var hasFiltering = treeBeingRendered[CallTree_SUBTREE_ID];
                    var showSmallCallsButton = (duration_bar_only_for_self_time ? x[M_SELF_DURATION] : x[M_DURATION]) < 1
                        && (!hasFiltering || hasFiltering && !(x[M_PREV_SELF_DURATION] < 0));
                    for (var i = 0; i < c.length; i++) {
                        var child = c[i];
                        var durationIsSmall = (duration_bar_only_for_self_time ? child[M_SELF_DURATION] : child[M_DURATION]) < 1;
                        html[html.length] = TREE_ITEM_TAG;
                        html[html.length] = ' ' + ATTR_NODE_IDX + '="';
                        html[html.length] = i + '"';
                        var collapseLevels = child[M_COLLAPSE_LEVELS];
                        if (collapseLevels && !uncollapsed) {
                            html[html.length] = ' ' + ATTR_NODE_COLLAPSE_LEVELS + '="';
                            html[html.length] = collapseLevels + '"';
                        }
                        if (!duration_bar_only_for_self_time) {
                            var bc = child[M_CATEGORY];
                            if (bc) html[html.length] = ' style="background-color:' + bc[BC_COLOR] + '"';
                        }
                        html[html.length] = '>';
                        treeNode2html(html, child, x, cutoffDuration, cutoffCalls, scale, uncollapsed);
                        html[html.length] = TREE_ITEM_TAG_CLOSE;
                        uncollapsed = false;
                    }
                }
                html[html.length] = TREE_GROUP_TAG_CLOSE;
            }

            var duration_bar_only_for_self_time = 0, treeBeingRendered;

            function renderDurationBar(html, size, scale, force) {
                var width = size * scale;
                if (width < 0.6 && !force) return;
                if (width > 60) width = 60;
                html[html.length] = PBAR_START;
                html[html.length] = width.toFixed(0);
                html[html.length] = '></span> ';
            }

            function renderNodeDuration(html, x, prevNode, scale) {
                if (treeBeingRendered[CallTree_SUBTREE_ID] && !(x[M_PREV_SELF_DURATION] < 0))
                    html[html.length] = '<span class="ui-priority-secondary ' + DOM_CLASS_TREE_NUMBERS + '">';
                else
                    html[html.length] = '<span class=' + DOM_CLASS_TREE_NUMBERS + '>';
                var selfDuration = x[M_SELF_DURATION];
                var duration = x[M_DURATION];

                var suspensionTime = x[M_SUSPENSION];
                var selfSuspensionTime = x[M_SELF_SUSPENSION];

                if (suspensionTime > 0) {
                    var hideGc = profiler_settings.gc_show_mode == 'never' || prevNode == undefined || tags.toHTML(x[M_ID]).substr(0, 3) == '<b>';
                    if (hideGc
                        || profiler_settings.gc_show_mode == 'smart'
                        && (suspensionTime < 30 || (suspensionTime/(duration + suspensionTime)<0.03))) {
                        duration += suspensionTime;
                        suspensionTime = 0;
                    }
                    if (hideGc
                        || profiler_settings.gc_show_mode == 'smart'
                        && (selfSuspensionTime < 30 || (selfSuspensionTime/(selfDuration + selfSuspensionTime)<0.03))) {
                        selfDuration += selfSuspensionTime;
                        selfSuspensionTime = 0;
                    }
                }

                if (duration == selfDuration) { // this implies suspension==selfSuspension
                    renderDurationBar(html, duration, scale, true);
                    if (duration == 0) {
                        html[html.length] = '0ms';
                    } else {
                        html[html.length] = '<b>';
                        html[html.length] = Duration__formatTime(duration);
                        html[html.length] = '</b>';
                        if (x[M_ID] > 0) {
                            html[html.length] = ' self';
                        }
                    }
                    if (suspensionTime > 0) {
                        html[html.length] = GRAY_START;
                        html[html.length] = ' + ';
                        renderDurationBar(html, suspensionTime, scale);
                        html[html.length] = Duration__formatTime(suspensionTime);
                        html[html.length] = ' self gc';
                        html[html.length] = GRAY_END;
                    }
                } else {
                    if (!duration_bar_only_for_self_time)
                        renderDurationBar(html, duration - selfDuration, scale, true);
                    html[html.length] = Duration__formatTime(duration);

                    if (selfDuration > 0) {
                        if (suspensionTime > 0) {
                            html[html.length] = GRAY_START;
                            html[html.length] = ' + ';
                            if (!duration_bar_only_for_self_time)
                                renderDurationBar(html, suspensionTime - selfSuspensionTime, scale);
                            html[html.length] = Duration__formatTime(suspensionTime);
                            html[html.length] = ' gc';
                            html[html.length] = GRAY_END;
                        }
                        html[html.length] = ' (';

                        renderDurationBar(html, selfDuration, scale);

                        html[html.length] = '<b>';
                        html[html.length] = Duration__formatTime(selfDuration);
                        html[html.length] = '</b> self';

                        if (selfSuspensionTime > 0) {
                            html[html.length] = GRAY_START;
                            html[html.length] = ' + ';
                            renderDurationBar(html, selfSuspensionTime, scale);
                            html[html.length] = Duration__formatTime(selfSuspensionTime);
                            html[html.length] = ' self gc';
                            html[html.length] = GRAY_END;
                        }

                        html[html.length] = ')';
                    } else if (suspensionTime > 0) { // selfDuration == 0 && suspension>0
                        html[html.length] = GRAY_START;
                        if (suspensionTime == selfSuspensionTime) {
                            html[html.length] = ' + ';
                            renderDurationBar(html, selfSuspensionTime, scale);
                            html[html.length] = Duration__formatTime(suspensionTime);
                            html[html.length] = ' self gc';

                        } else {
                            html[html.length] = ' + ';
                            if (!duration_bar_only_for_self_time)
                                renderDurationBar(html, suspensionTime - selfSuspensionTime, scale);
                            html[html.length] = Duration__formatTime(suspensionTime);
                            html[html.length] = ' gc';


                            if (selfSuspensionTime > 0) {
                                html[html.length] = ' (';
                                renderDurationBar(html, selfSuspensionTime, scale);
                                html[html.length] = Duration__formatTime(selfSuspensionTime);
                                html[html.length] = ' self gc';
                                html[html.length] = ')';
                            }
                        }
                        html[html.length] = GRAY_END;
                    }
                }
                html[html.length] = ' ';
                html[html.length] = ' - ';
                var mult;
                if (x[M_ID] > 0) {
                    var executions = x[M_EXECUTIONS];

                    if (prevNode && !duration_bar_only_for_self_time)
                        mult = executions / prevNode[M_EXECUTIONS];

                    html[html.length] = Integer__format(executions);
                    html[html.length] = ' inv ';
                    if (mult >= 5 && mult < 1e10) {
                        html[html.length] = RED_START;
                        html[html.length] = '(x ' + Integer__format(mult) + ')';
                        html[html.length] = RED_END;
                    }
                    mult = 0;
                }
                if (x[M_CHILD_EXECUTIONS] > 0) {
                    if (duration_bar_only_for_self_time) {
                        var c = x[M_CHILDREN];
                        if (c.length > 0) {
                            mult = 0;
                            for (var i = 0; i < c.length; i++)
                                mult += c[i][M_CHILD_EXECUTIONS];
                            mult = x[M_CHILD_EXECUTIONS] / mult;
                        }
                    }
                    html[html.length] = GRAY_START;
                    html[html.length] = ' - ';
                    html[html.length] = Integer__format(x[M_CHILD_EXECUTIONS]);
                    html[html.length] = ' calls';
                    html[html.length] = GRAY_END;
                    if (mult >= 5 && mult < 1e10) {
                        html[html.length] = RED_START;
                        html[html.length] = ' (x ' + Integer__format(mult) + ')';
                        html[html.length] = RED_END;
                    }
                }
                html[html.length] = '</span>';

                html[html.length] = ' ';
                var highlightMode = x[M_PREV_SELF_DURATION];
                html[html.length] = treeBeingRendered[CallTree_SUBTREE_ID] && highlightMode != -1 ? tags.toHighlightHTML(x[M_ID], highlightMode == -2 ? 'ui-state-highlight' : 'ui-priority-secondary') : tags.toHTML(x[M_ID]);
            }

            function treeNode2html(html, x, prevNode, cutoffDuration, cutoffCalls, scale, uncollapsed) {
                var t;
                var isFiltering = treeBeingRendered[CallTree_SUBTREE_ID], highlightMode = x[M_PREV_SELF_DURATION];
                var initiallyHidden;
                if (duration_bar_only_for_self_time)
                    initiallyHidden = x[M_SELF_DURATION] <= cutoffDuration && (x[M_SELF_DURATION] != 0 || (x[M_EXECUTIONS] <= cutoffCalls));
                else
                    initiallyHidden = x[M_DURATION] <= cutoffDuration && (x[M_DURATION] != 0 || (x[M_CHILD_EXECUTIONS] + x[M_EXECUTIONS] <= cutoffCalls))
                        || (isFiltering && prevNode && prevNode[M_PREV_SELF_DURATION] < 0 && !(x[M_PREV_SELF_DURATION] < 0));

                var notComputed = x[M_NOT_COMPUTED];

                if (!initiallyHidden && notComputed) {
                    x[M_COMPUTATOR](treeBeingRendered, x);
                    notComputed = 0;
                }

                var collapseLevels = x[M_COLLAPSE_LEVELS];
                var nextNode = x;

                if ((t = x[M_CHILDREN]) && t.length || (t = x[M_TAGS]) && t.length || notComputed) {
                    if (!uncollapsed && collapseLevels > 0) {
                        var k = collapseLevels;
                        for (; k > 0; k--) nextNode = nextNode[M_CHILDREN][0];
                    }
                    html[html.length] = '<span class="ui-buttonset';
                    if (isFiltering && !(highlightMode < 0))
                        html[html.length] = ' ui-state-disabled';
                    html[html.length] = '"><span ' + ATTR_BUTTON_ID + '="' + BUTTON_ID_FOLD_UNFOLD + '" class="uc ui-state-default ui-button ui-corner-left"';
                    notComputed |= nextNode[M_NOT_COMPUTED];
                    var noGrandChildren = (!(t = nextNode[M_CHILDREN]) || t.length == 0) && (!(t = nextNode[M_TAGS]) || t.length == 0) && !notComputed;
                    if (noGrandChildren)
                        html[html.length] = ' ' + ATTR_NODE_NO_GRAND_CHILDREN + '="1"';
                    if (notComputed)
                        html[html.length] = ' ' + ATTR_NODE_NOT_COMPUTED + '="1"';
                    initiallyHidden |= noGrandChildren;
                    if (initiallyHidden) html[html.length] = ' ' + ATTR_NODE_LAZY + '="1"';
                    else if (collapseLevels > 0 && !uncollapsed) html[html.length] = ' ' + ATTR_NODE_UNFOLDED + '="1"';
                    html[html.length] = '><span class="ui-icon ui-icon-';
                    html[html.length] = (initiallyHidden || notComputed || collapseLevels) && !uncollapsed ?
                        (initiallyHidden || notComputed ? 'plus' : ('circle-plus" title="' + collapseLevels + ' level' + (collapseLevels > 1 ? 's' : ''))) : 'minus';
                    html[html.length] = '"></span></span>';
                } else {
                    html[html.length] = '<span class="ui-buttonset';
                    if (isFiltering && !(highlightMode < 0))
                        html[html.length] = ' ui-state-disabled';
                    html[html.length] = '" style="padding-left:18px">';
                }
                html[html.length] = ' <span ' + ATTR_BUTTON_ID + '="' + BUTTON_ID_MENU + '" class="uc ';
                if (x[M_ID] < 0) html[html.length] = 'ui-state-disabled ';
                html[html.length] = 'ui-state-default ui-button ui-corner-right"><span class="ui-icon ';
                html[html.length] = treeBeingRendered.ty == CallTree_TYPE_FIND_USAGES || !duration_bar_only_for_self_time
                    ? 'ui-icon-arrowthick-1-se' : 'ui-icon-arrowthick-1-nw';
                html[html.length] = '"></span></span></span>';

                renderNodeDuration(html, x, prevNode, scale);

                if (uncollapsed) {
                    if (!(collapseLevels > 0)) return;
                } else {
                    if (initiallyHidden) return;
                }

                treeNodeChilds2html(html, nextNode, cutoffDuration, cutoffCalls, scale, uncollapsed);
            }

            $(init);

            function plusOrMinus($node, flag) {
                var a = 'ui-icon-minus', b = 'ui-icon-plus';
                if (flag) {
                    var c = a;
                    a = b;
                    b = c;
                }
                $node.children().addClass(a).removeClass(b);
            }

            function plusOrMinusToggle($node) {
                var $plusMinus = $node.children();
                $plusMinus.toggleClass('ui-icon-plus').toggleClass('ui-icon-minus');
                return $plusMinus.is('.ui-icon-plus');
            }

            function setIcon($node, klass) {
                $node.children().attr('class', 'ui-icon ui-icon-' + klass);
            }

            var selected_tab_id = getState('tab') || 'tabs-call-tree';
            var dynamic_tabs = [], dynamic_tab_next_id = 10;
            var selected_ash, selected_ash_dateWindow;
            var ash_initialized;

            var Document_mouseIsDown;

            var toolTipTimer, toolTipRenderer;
            var toolTipNode, toolTipNode2, toolTipMouseIsOver;
            var toolTipPositionProperties = {
                my: 'center bottom',
                at: 'center top',
                collision: 'fit flip'
            };

            var toolTipRenderers = [], toolTipInited = [];

            /** @const */
            var TOOLTIP_OUT_CALLS = 0;
            /** @const */
            var TOOLTIP_FIND_USAGES = 1;
            /** @const */
            var TOOLTIP_IN_CALLS_GROUP = 2;
            /** @const */
            var TOOLTIP_IN_CALLS_ITEM = 3;

            CT.ttide = function() {
                return CT.ide(toolTipNode[1][M_ID]);
            };

            function ToolTip__scheduleHide() {
                clearTimeout(toolTipTimer);
                $('#h' + toolTipRenderer).hide();
            }

            function ToolTip__ensureInit() {
                if (toolTipInited[toolTipRenderer]) return;
                toolTipInited[toolTipRenderer] = 1;
                $('#h' + toolTipRenderer + ' .' + DOM_CLASS_HELP_LINK).click(function() {
                    $(this).parent().parent().next().toggle();
                    return false;
                });
            }

            toolTipRenderers[TOOLTIP_OUT_CALLS] = function() {
                ToolTip__ensureInit();
                var n = toolTipNode[1];
                var tag = tags.t[n[M_ID]];
                $('#h0-m').html(tags.toWrapHTML(n[M_ID]));
                $('#h0-d').text(Duration__formatTime(n[M_SELF_DURATION]));
                $('#h0-dw').text(Duration__formatTime(n[M_DURATION]));
                $('#h0-ai').text(Duration__formatTime(Math.round(n[M_SELF_DURATION] / n[M_EXECUTIONS])));
                $('#h0-aiw').text(Duration__formatTime(Math.round(n[M_DURATION] / n[M_EXECUTIONS])));
                $('#h0-s').text(Duration__formatTime(n[M_SELF_SUSPENSION]));
                $('#h0-sw').text(Duration__formatTime(n[M_SUSPENSION]));
                $('#h0-i').text(Integer__format(n[M_EXECUTIONS]));
                $('#h0-iw').text(Integer__format(n[M_EXECUTIONS] + n[M_CHILD_EXECUTIONS]));
                var treeStartTime = toolTipNode[0].fw[M_START_TIME];
                if (n === toolTipNode[0].fw) {
                    $('#h0-ap').text('+0, +' + Duration__formatTime(n[M_END_TIME] - treeStartTime));
                    // The root node contains absolute timestamp for M_START_TIME, M_END_TIME
                    treeStartTime = 0;
                } else {
                    $('#h0-ap').text('+' + Duration__formatTime(n[M_START_TIME]) + ', +' + Duration__formatTime(n[M_END_TIME]));
                }
                $('#h0-apf').text(Date__formatWithMillis(new Date(treeStartTime + n[M_START_TIME])));
                $('#h0-apl').text(Date__formatWithMillis(new Date(treeStartTime + n[M_END_TIME])));
                $('#h0-sj').text((tag[T_JAR] || '').replace(/^\[(.*)\]$/, '$1'));
                $('#h0-l').text((tag[T_SOURCE] || '').replace(/^\((.*)\)$/, '$1'));
                $('#h0').show().position(toolTipPositionProperties);
            };

            toolTipRenderers[TOOLTIP_FIND_USAGES] = function() {
                ToolTip__ensureInit();
                var n = toolTipNode[1];
                var tagId = toolTipNode[0].srcId;
                var tag = tags.t[tagId];
                $('#h1-m').html(tags.toWrapHTML(tagId));
                $('#h1-m1,#h1-m2,#h1-m3').html(tags.toShortHTML(tagId));
                $('#h1-mc').html(tags.toShortHTML(n[M_ID]));
                $('#h1-mci').text(Integer__format(n[M_CHILD_EXECUTIONS]) + ' invocation' + (n[M_CHILD_EXECUTIONS] > 1 ? 's' : ''));
                $('#h1-d').text(Duration__formatTime(n[M_SELF_DURATION]));
                $('#h1-dw').text(Duration__formatTime(n[M_DURATION]));
                $('#h1-ai').text(Duration__formatTime(Math.round(n[M_SELF_DURATION] / n[M_EXECUTIONS])));
                $('#h1-aiw').text(Duration__formatTime(Math.round(n[M_DURATION] / n[M_EXECUTIONS])));
                $('#h1-i').text(Integer__format(n[M_EXECUTIONS]));
                $('#h1-sj').text((tag[T_JAR] || '').replace(/^\[(.*)\]$/, '$1'));
                $('#h1-l').text((tag[T_SOURCE] || '').replace(/^\((.*)\)$/, '$1'));
                $('#h1').show().position(toolTipPositionProperties);
            };

            toolTipRenderers[TOOLTIP_IN_CALLS_GROUP] = function() {
                ToolTip__ensureInit();
                var n = toolTipNode[1];
                var executions = n[M_EXECUTIONS];
                $('#h2-1').html((executions > 1 ? 'were ' + Integer__format(executions) + ' ' : 'was a single ') + tags.toHTML(n[M_ID]) + ' call' + (executions > 1 ? 's' : ''));
                $('#h2-2').text(Duration__formatTime(n[M_SELF_DURATION]));
                $('#h2').show().position(toolTipPositionProperties);
            };


            toolTipRenderers[TOOLTIP_IN_CALLS_ITEM] = function() {
                ToolTip__ensureInit();
                var n = toolTipNode[1];
                var tag = tags.t[n[M_ID]];
                $('#h3-m').html(tags.toWrapHTML(toolTipNode2[M_ID]));
                $('#h3-m1,#h3-m2,#h3-m3').html(tags.toShortHTML(toolTipNode2[M_ID]));
                $('#h3-mc').html(tags.toShortHTML(n[M_ID]));
                $('#h3-mci').text(Integer__format(n[M_CHILD_EXECUTIONS]) + ' invocation' + (n[M_CHILD_EXECUTIONS] > 1 ? 's' : ''));
                $('#h3-d').text(Duration__formatTime(n[M_SELF_DURATION]));
                $('#h3-dw').text(Duration__formatTime(n[M_DURATION]));
                $('#h3-ai').text(Duration__formatTime(Math.round(n[M_SELF_DURATION] / n[M_EXECUTIONS])));
                $('#h3-aiw').text(Duration__formatTime(Math.round(n[M_DURATION] / n[M_EXECUTIONS])));
                $('#h3-s').text(Duration__formatTime(n[M_SELF_SUSPENSION]));
                $('#h3-sw').text(Duration__formatTime(n[M_SUSPENSION]));
                $('#h3-i').text(Integer__format(n[M_EXECUTIONS]));
                $('#h3-sj').text((tag[T_JAR] || '').replace(/^\[(.*)\]$/, '$1'));
                $('#h3-l').text((tag[T_SOURCE] || '').replace(/^\((.*)\)$/, '$1'));
                $('#h3').show().position(toolTipPositionProperties);
            };

            function ToolTip__scheduleShow(event, $node, tagId, position) {
                if (Document_mouseIsDown) {
                    ToolTip__scheduleHide();
                    return;
                }

                toolTipNode = Tree$findNode($node.parent(), true);
                toolTipNode[3] = tagId;
                if (position) {
                    toolTipPositionProperties.of = position;
                } else {
                    toolTipPositionProperties.of = $node[0];
                }
                var tree = toolTipNode[0];
                var n = toolTipNode[1];
                var nextRenderer, treeType = tree.ty;
                if (treeType === CallTree_TYPE_OUT_CALLS)
                    nextRenderer = TOOLTIP_OUT_CALLS;
                else if (treeType === CallTree_TYPE_FIND_USAGES)
                    nextRenderer = TOOLTIP_FIND_USAGES;
                else if (treeType === CallTree_TYPE_IN_CALLS) {
                    if (toolTipNode[1][M_ID] < 0)
                        nextRenderer = TOOLTIP_IN_CALLS_GROUP;
                    else {
                        var path = toolTipNode[2];
                        var parentNode;
                        Tree$each(tree, path, function(node, idx) {
                            if (node[M_ID] < 0) return;
                            parentNode = node;
                            return true;
                        });

                        toolTipNode2 = parentNode;
                        nextRenderer = parentNode === toolTipNode[1] ? CallTree_TYPE_OUT_CALLS : TOOLTIP_IN_CALLS_ITEM;
                    }
                }

                if (nextRenderer === undefined) {
                    ToolTip__scheduleHide();
                    return;
                }

                if (nextRenderer !== toolTipRenderer)
                    $('#h' + toolTipRenderer).hide();

                toolTipRenderer = nextRenderer;
                var renderer = toolTipRenderers[toolTipRenderer];
                if (renderer)
                    toolTipTimer = setTimeout(renderer, (event.ctrlKey || event.metaKey) ? 100 : 15000);
            }

            function CallTree__checkTabRequiresRender(newTab) {
                if (app[newTab]) return;
                app[newTab] = true;
                if (newTab == 'tabs-hotspots') {
                    if (!trees[0]) {
                        app[newTab] = false;
                        return;
                    }
                    $('#loading').show();
                    setTimeout(function () {
                        CallTree_renderHotspots(trees[0]);
                    }, 0);
                } else if (newTab == 'tabs-params') {
                    if (!trees[0]) {
                        app[newTab] = false;
                        return;
                    }
                    $('#loading').show();
                    setTimeout(function () {
                        CallTree_renderParams(trees[0]);
                    }, 0);
                } else if (newTab == 'tabs-explore') {
                    var lastSuggest = '', lastResult = [];
                    $('#methodName').autocomplete({
                        source: function (req, resp) {
                            var r = tags.r, term = req.term.toLowerCase();
                            if (term == lastSuggest) return resp(lastResult);
                            var t = tags.t;
                            var suggest = [];
                            for (var k in r) {
                                if (k.toLowerCase().indexOf(term) != -1)
                                    suggest[suggest.length] = tags.toHTML(r[k]);
                            }
                            suggest.sort();
                            lastSuggest = term;
                            lastResult = suggest;
                            resp(suggest);
                        }
                    });
                } else if (newTab == 'tabs-db') {
                    if (CT.dbStats === undefined) {
                        app[newTab] = false;
                        return;
                    }
                    ash_initialized = true;
                    var ash_group = getState('ash') || 'event';
                    $('#ashg-' + ash_group).attr('checked', true).button('refresh');
                    ASH_render(ash_group);
                    $('#db-queries').html(CT.dbStats);
                    if (CT.dbExceptions)
                        $('#db-exceptions').html(CT.dbExceptions);
                }
            }

            function init() {
                app.inited = 1;

                $notify = app.notify = $("#jqn-container").notify();

                $('#vrs').show();
                var $tabs =  $("#tabs");
                $tabs.tabs({
                    beforeActivate: function(e, ui) {
                        selected_tab_id = ui.newPanel.attr('id');
                        pushState({tab: selected_tab_id});
                        CallTree__checkTabRequiresRender(selected_tab_id);
                    }
                    , active: $('#tabs').children('div').index($('#' + selected_tab_id))
                }).show().find(".ui-tabs-nav").sortable({
                    update: function(event, ui) {
                        var $tabs = $('#tabs');
                        var order = $(this).sortable('toArray', {attribute: 'href'});
                        for (var i = 0; i < order.length; i++)
                            $(order[i]).appendTo($tabs);
                    }
                });

                $tabs.on("click", "span.ui-icon-close", function() {
                    var tab_id = $(this).closest("li").remove().attr("aria-controls");
                    var tab = $("#" + tab_id);
                    var idx = tab.index();
                    $tabs.tabs('option', 'active', idx);
                    tab.remove();
                    $tabs.tabs("refresh");
                    $.bbq.removeState(tab_id);
                });

                $('#ash-grouping').buttonset();
                $('#ash-grouping input:radio[value]').change(function(e) {
                    pushState({ash: e.target.value});
                });

                $(document).mouseover(function(event) {
                    var $target = $(event.target);
                    $target.parent().addBack().filter('.ui-button:not(.ui-state-disabled)').addClass('ui-state-hover');
                    var cc = ctrl_pressed;
                    ctrl_pressed = event.ctrlKey || event.metaKey;
                    if (ctrl_pressed) {
                        IDE_highlight(event);
                    } else if (cc) $('#ide').hide();
                    var $parents = $target.firstParents(6).addBack();

                    clearTimeout(toolTipTimer);
                    toolTipMouseIsOver = $parents.filter('.' + DOM_CLASS_TOOLTIP).length > 0;
                    if (toolTipMouseIsOver) return;

                    var $node = $parents.filter('.' + DOM_CLASS_TREE_NUMBERS).last();
                    if ($node.length == 0) {
                        ToolTip__scheduleHide();
                        return;
                    }

                    ToolTip__scheduleShow(event, $node, 0);
                });

                $(document).mousedown(function() {
                    Document_mouseIsDown = true;
                    if (toolTipRenderer !== undefined && !toolTipMouseIsOver)
                        ToolTip__scheduleHide();
                });

                $(document).mouseup(function() {
                    Document_mouseIsDown = false;
                });

                $(document).mouseout(function(event) {
                    $(event.target).parent().addBack().filter('.ui-button:not(.ui-state-disabled)').removeClass('ui-state-hover');
                });

                var $w = $(window);
                $(document).keydown(function(event) {
                    ctrl_pressed = event.ctrlKey || event.metaKey;
                    if (ctrl_pressed) {
                        var elem = document.elementFromPoint(mouseX - $w.scrollLeft(), mouseY - $w.scrollTop());
                        if (elem) {
                            event.target = elem;
                            IDE_highlight(event);
                        }
                    }
                });

                $(document).keyup(function(event) {
                    if (ctrl_pressed) {
                        ctrl_pressed = event.ctrlKey || event.metaKey;
                        if (!ctrl_pressed) $('#ide').hide();
                    }
                });

                $(document).mousemove(function(event) {
                    mouseX = event.pageX;
                    mouseY = event.pageY;
                    if (ctrl_pressed) {
                        ctrl_pressed = event.ctrlKey || event.metaKey;
                        if (!ctrl_pressed) $('#ide').hide();
                    }
                });

                if (!app.du && app.args) {
                    var paramUrl = app.du = $.param(app.args) + '&callback=treedata';
                    $('#download').attr('href', 'tree/' + app.dn + '?' + paramUrl);
                }

                if (window.addEventListener)
                    $('#download')[0].addEventListener('dragstart', function(e) {
                        var t = $('#download');
                        var loc = window.location;
                        var fileInfo = 'application/octet-stream:' + app.dn + ':' + loc.origin + loc.pathname.replace(/\/[^/]+$/, '/') + 'tree/tree.zip?' + app.du;
                        e.dataTransfer.setData("DownloadURL", fileInfo);
                    }, false);

                function Tree__ensureComputed($x, $li) {
                    var node;
                    if (!$x.attr(ATTR_NODE_NOT_COMPUTED)) return;
                    node = Tree$findNode($li);
                    var comp = node[1][M_COMPUTATOR];
                    if (!comp) return;
                    comp(node[0], node[1]);
                    var children = node[1][M_CHILDREN];
                    if (children && children.length)
                        $x.attr(ATTR_NODE_LAZY, 1);
                    else
                        $x.css('visibility', 'hidden');
                }

                var clickHandlers = {};

                clickHandlers[BUTTON_ID_FOLD_UNFOLD] = function(event) {
                    var x = $(this);
                    var $buttonset = x.parent();
                    var $li = $buttonset.parent();
                    var collapseLevels, node, $last, html;

                    Tree__ensureComputed(x, $li);

                    var isOpen = x.children().is('.ui-icon-minus,.ui-icon-circle-minus');

                    var justUncollapse = (event.ctrlKey || event.shiftKey) && x.children().is('.ui-icon-circle-minus');

                    if (isOpen && !x.attr(ATTR_NODE_LAZY) && $li.attr(ATTR_NODE_COLLAPSE_LEVELS) || justUncollapse) {
                        node = Tree$findNode($li);
                        $li.removeAttr(ATTR_NODE_COLLAPSE_EXPANDED);
                        $last = Tree$html$skipLevels($li, node[1][M_COLLAPSE_LEVELS]);

                        $collapseContent = $buttonset.nextAll('.q');
                        var $lastChildren = $last.children('.q');

                        if ($lastChildren.length)
                            $lastChildren.appendTo($li);

                        $collapseContent.remove();

                        if (justUncollapse) {
                            setIcon(x, 'circle-plus');
                            return;
                        }
                    }

                    var justFold = (event.ctrlKey || event.shiftKey) && x.children().is('.ui-icon-circle-plus');

                    if (isOpen || justFold) { // collapse and hide node
                        setIcon(x, 'plus');
                        x.removeAttr(ATTR_NODE_UNFOLDED);

                        if (x.attr(ATTR_NODE_LAZY)) { // it is fully rerendered on open
                            $li.removeAttr(ATTR_NODE_COLLAPSE_EXPANDED);
                            // just remove all children
                            $buttonset.nextAll('.q').remove();
                            return;
                        }

                        // hide all elements
                        $buttonset.nextAll('.q').hide();
                    } else {
                        collapseLevels = $li.attr(ATTR_NODE_COLLAPSE_LEVELS);

                        // the button was "plus"
                        var noGrandChildren = x.attr(ATTR_NODE_NO_GRAND_CHILDREN);
                        if (!x.attr(ATTR_NODE_UNFOLDED)
                            && !noGrandChildren
                            ) {
                            x.attr(ATTR_NODE_UNFOLDED, '1');
                            if (!x.attr(ATTR_NODE_LAZY)) { // just show node
                                $buttonset.nextAll('.q').show();
                            } else { // render and append to the list
                                node = Tree$findNode($li);
                                html = renderNodeChilds(node[0], node[1]);

                                $(html.join('')).appendTo($li);
                            }
                            // when simple node (no expand-collapse) flip button to minus
                            // when there is expand, set button to circle-plus
                            setIcon(x, collapseLevels ? 'circle-plus' : 'minus');
                        } else { // expand collapsed contents
                            setIcon(x, noGrandChildren ? 'minus' : 'circle-minus');
                            $li.attr(ATTR_NODE_COLLAPSE_EXPANDED, '1');
                            node = Tree$findNode($li);

                            var $collapseContent, $children;

                            html = renderNodeChilds(node[0], node[1], true).join('');
                            $collapseContent = $(html);
                            $children = $buttonset.nextAll('.q');
                            if (!$children.length) {
                                $collapseContent = $collapseContent.appendTo($li);
                            } else {
                                $collapseContent = $collapseContent.insertBefore($children);
                            }
                            $last = Tree$html$skipLevels($li, node[1][M_COLLAPSE_LEVELS]);
                            var $lastFoldUnfold = $last.find('span[' + ATTR_BUTTON_ID + '=' + BUTTON_ID_FOLD_UNFOLD + ']');

                            if ($children.length)
                                $children.appendTo($last);
                            if (x.attr(ATTR_NODE_LAZY))
                                $lastFoldUnfold.attr(ATTR_NODE_LAZY, '1');
                        }
                    }

                };

                clickHandlers[BUTTON_ID_ZERO_METHOD] = function() {
                    var x = $(this);
                    var onHide = plusOrMinusToggle(x);

                    var $buttonDiv = x.parent();
                    var $li = $buttonDiv.parent();
                    if (onHide) {
                        $li.nextAll().remove();
                        return;
                    }
                    var $ul = $li.parent();
                    var node = Tree$findNode($ul.parent());
                    var html = renderNodeSmallChilds(node[0], node[1], Number($buttonDiv.attr(ATTR_NODE_IDX))).join('');

                    var $items = $(html);
                    if ($items.length < 20)
                        $items.hide();
                    $items.appendTo($ul);
                    if ($items.length < 20)
                        $items.toggle();
                };

                function Tree$html$skipLevels($node, levels) {
                    var t;
                    for (; levels > 0; levels--) {
                        $node = $node.children('.q');
                        if (!(t = $node[0]) || !(t = t.firstChild) || !(t = t.firstChild)) return $([]);
                        $node = $(t);
                    }
                    return $node;
                }

                clickHandlers[BUTTON_ID_MENU] = function() {
                    var elem = $(this);
                    var m = $('#ct_menu');
                    if (!m.data('menu'))
                        m.menu();

                    var src = elem.closest('div');
                    var node = Tree$findNode(src, true);
                    m.data('source', src);
                    m.data('node', node);
                    $('#ct_ide').toggle(tags.t[node[1][M_ID]][T_SOURCE] != null);

                    m.show();
                    m.position({
                        my: 'right top',
                        at: 'left bottom',
                        of: elem
                    });
                };

                var $currentExpandedSql;
                $(document).click(function(event) {
                    var x = $(event.target), btn;
                    var buttonId = (btn = x).attr(ATTR_BUTTON_ID) || (btn = x.parent()).attr(ATTR_BUTTON_ID);
                    if (buttonId && !btn.is('.ui-state-disabled')) {
                        var handler = clickHandlers[buttonId];
                        if (handler) handler.call(btn, event);
                    } else if (event.altKey && selected_tab_id != 'tabs-db') {
                        var isPre = (btn = x).is('pre') || (btn = btn.parent()).is('pre') || (btn = btn.parent()).is('pre');
                        if (!isPre) return;
                        if (!btn.toggleClass('lightbox').hasClass('lightbox')) {
                            $currentExpandedSql = undefined;
                            return;
                        }

                        var $w = $(window);

                        var $prevSql = $currentExpandedSql;

                        $currentExpandedSql = btn.css({
                            left: Math.max($w.width() - btn.width(), 0) / 2 + $w.scrollLeft(),
                            top: Math.max($w.height() - btn.height(), 0) / 2 + $w.scrollTop()
                        });
                        if ($prevSql)
                            $prevSql.removeClass('lightbox');
                    }
                });

                $('#ct_out_calls,#ct_in_calls,#ct_usages,#ct_local_hotspots').click(function(event) {
                    var activateNow = !event.shiftKey;
                    Tabs__scheduleCreate($(event.target).closest('a').attr('x'), 0, activateNow);
                });

                $('#ct_get_stacktrace').click(function() {
                    var t = $('#ct_menu').data('source');
                    var treeAndPath = Tree$findNodePath(t, true);
                    var p = treeAndPath[1];
                    var n = CallTree__getTree(treeAndPath[0]).fw;
                    var T = tags.t;

                    function nodeToStacktrace(n) {
                        var t = T[n[M_ID]];
                        var args = t[T_ARGUMENTS];
                        if (args == '()') args = '(no args)';
                        return [
                            Duration__formatTime(Number(n[M_DURATION])),
                            Duration__formatTime(Number(n[M_SELF_DURATION])),
                            n[M_EXECUTIONS].toString(),
                            n[M_CHILD_EXECUTIONS].toString(),
                            t[T_PACKAGE] + t[T_CLASS] + '.' + t[T_METHOD] + t[T_SOURCE] + ' ' + t[T_JAR] + ' ' + args + (t[T_RETURN_TYPE] == 'void' ? '' : (' : ' + t[T_RETURN_TYPE]))
                        ];
                    }

                    var r = [], i;
                    var x = r[p.length] = nodeToStacktrace(n);
                    var width = [];
                    for (i = 0; i < 4; i++)
                        width[i] = x[i].length;
                    for (i = 0; i < p.length; i++) {
                        n = n[M_CHILDREN][p[p.length - i - 1]];
                        x = r[p.length - i - 1] = nodeToStacktrace(n);
                        for (var j = 0; j < 4; j++)
                            width[j] = Math.max(width[j], x[j].length);
                    }
                    var res = [];
                    for (i = 0; i < r.length; i++) {
                        x = r[i];
                        res[res.length] = '       '.substr(0, width[0] - x[0].length) + x[0];
                        res[res.length] = ', ';
                        res[res.length] = '       '.substr(0, width[1] - x[1].length) + x[1];
                        res[res.length] = ' self, ';
                        res[res.length] = '       '.substr(0, width[2] - x[2].length) + x[2];
                        res[res.length] = ' inv, ';
                        res[res.length] = '       '.substr(0, width[3] - x[3].length) + x[3];
                        res[res.length] = ' calls  at ';
                        res[res.length] = x[4];
                        res[res.length] = '\n';
                    }
                    res[res.length] = '&nbsp;';
                    $('<div>').append($('<pre>').html(prettyPrintOne(res.join('')))).dialog({title: 'StackTrace', width:$(window).width() * 0.90, resizable:true});
                });
                $('#ct_mark_red').click(function() {
                    var t = $('#ct_menu').data('source');
                    t.toggleClass('redFrame');
                });

                $('#ct_ide').click(function() {
                    var node = $('#ct_menu').data('node');
                    CT.ide(node[1][M_ID]);
                });

                $('#ct_adj').click(function(event) {
                    var node = $('#ct_menu').data('node');
                    var activateDialog = event.shiftKey;
                    Tree__adjustDuration(node[1], node[2], activateDialog);
                });

                $('#ct_cat').click(function(event) {
                    var node = $('#ct_menu').data('node');
                    var activateDialog = event.shiftKey;
                    Tree__setupBc(node[1], activateDialog);
                });

                $('#cmd-adj-dur').click(Tree__adjustDuration);

                $('#cmd-setup-bc').click(Tree__setupBc);

                $('#cmd-setup-p').click(Tree__setupPersonal);

                $(document).bind('mouseup', function() {
                    $('#ct_menu:visible').hide();
                });

                $('#tabs-db').click(function(e) {
                    if (!e.altKey) return;
                    var t = e.target;
                    for (var i = 0; i < 8 && t && !(t.getAttribute('w') || t.w); i++) {
                        t = t.parentElement;
                    }
                    if (i == 8 || !t) return;
                    var u;
                    var lastCol = t.cells.length - 1;
                    if (t.cells[lastCol].className) {
                        $(t.cells[lastCol - 1].firstChild).css('max-height', 'none');
                        u = t.cells[lastCol];
                        $(u).attr('oc', u.className);
                        u.className = '';
                    } else {
                        $(t.cells[lastCol - 1].firstChild).css('max-height', '');
                        u = t.cells[lastCol];
                        u.className = $(u).attr('oc');
                    }
                });

                if (app.data) {
                    CallTree_render(app.data());
                    delete app.data;
                }

                $(window).bind('hashchange', onHashChange);
                $(window).trigger('hashchange');
            }

            function getTabNames(s) {
                var tabs = [];
                for (var i in s)
                    if (i.charAt(0) == 'z')
                        tabs[parseInt(i.substring(1))] = s[i];
                return tabs;
            }

            function onHashChange(event) {
                var t;

                var s = event.getState();
                if (trees[0]) {
                    var tabs = getTabNames(s);

                    // when the call tree is loaded
                    var j;
                    for (j = 0; j < dynamic_tab_next_id || j < tabs.length; j++) {
                        if (!tabs[j]) {
                            Tabs__delete(j);
                            continue;
                        }

                        if (!dynamic_tabs[j] || dynamic_tabs[j][0] != tabs[j]) {
                            Tabs__replace(j, tabs[j]);
                        }
                    }
                }

                if ((t = event.getState('tab')) && t != selected_tab_id) {
                    $('a[href="#'+t+'"]').click();
                }

                if ((t = event.getState('ash')) && t != selected_ash && ash_initialized) {
                    ASH_render(t);
                    $('#ashg-'+t).attr('checked', true).button('refresh');
                }
            }

            function Tabs__delete(idx) {
                var value = dynamic_tabs[idx];
                if (!value) return;
                delete dynamic_tabs[idx];
                var tab = $("#z" + idx);
                tab.remove();
                delete trees[idx];
            }

            function Tabs__replace(idx, params) {
                if (dynamic_tab_next_id <= idx) dynamic_tab_next_id = idx + 1;
                var parsed = params.split('/');
                parsed[0] = parseInt(parsed[0]);
                parsed[1] = parseInt(parsed[1]);
                parsed[2] = parseInt(parsed[2]);

                var srcTreeId = parsed[1];
                var srcNode;

                if (parsed[2] == 0) {
                    parsed[3] = parseInt(parsed[3]);
                    srcNode = Tree__createNode(parsed[3]);
                } else if (parsed[2] == 1) {
                    var path = parsed[3].replace(/(\d*)!(\d+)/g, function(x, a, b) {
                        var s = [];
                        a = (a || '0').toString();
                        b = parseInt(b) + 1;
                        for (var i = b; i >= 0; i--)
                            s[i] = a;
                        return s.join('*');
                    }).split('*');
                    if (path[0] == '') path = [];

                    var srcTree = trees[srcTreeId];
                    if (srcTree)
                        srcNode = Tree$getNodeByPath(srcTree, path);
                }

                dynamic_tabs[idx] = [params, parsed, 0, srcNode];

                var fn = tabRenderingMethods[parsed[0]];

                fn(idx, parsed);
            }

            function Tabs__scheduleCreate(function_id, defaultTree, activateNow) {
                var node = $('#ct_menu').data('node');

                var tags = node[1][M_TAGS];

                var tree_id, compr, comprScheme;
                if (!tags || tags.length == 0) {
                    comprScheme = 0;
                    tree_id = defaultTree;
                    compr = node[1][M_ID];
                } else {
                    comprScheme = 1;
                    tree_id = node[0].id;

                    var path = node[2];
                    if (node[0][CallTree_SUBTREE_ID]) {
                        path = Tree__findSimilarNode(CallTree__getTree(tree_id), node[1]);
                    }

                    path = path.join('*');
                    compr = path.replace(/(\d+)(?:\*\1)+/g, function(x, a) {
                        return (a > 0 ? a : '') + '!' + ((x.length - a.length) / (a.length + 1) - 1);
                    });
                }

                var state = {};
                var tab_id = ('z' + (dynamic_tab_next_id++));
                state[tab_id] = function_id + '/' + tree_id + '/' + comprScheme + '/' + compr;
                if (activateNow) {
                    state['tab'] = tab_id;
                    $('#tabs')[0].scrollIntoView()
                } else {
                    app.notify.notify('create', 'jqn-tab-added', {
                        methodName: CT.tags.toShortHTML(node[1][M_ID]) //label.replace(/float:left/g)
                        , tabId: tab_id
                    }, {expires: 5000, custom: true})
                }

                pushState(state);
            }

            function Tabs__createTab(idx, label) {
                var tab_id = 'z' + idx;
                var $tabs = $('#tabs');
                var tabTemplate = "<li href='#{href}'><a href='#{href}' style='padding-left:0.4em;padding-right:0.2em'>#{label}<span class='ui-icon ui-icon-close inline-block' style='cursor: pointer'>Remove Tab</span></a></li>";
                var li = $(tabTemplate.replace( /#\{href\}/g, "#" + tab_id ).replace( /#\{label\}/g, label ));
                $tabs.find( ".ui-tabs-nav" ).append( li );
                $tabs.append( "<div id='" + tab_id + "'>/div>" );
                $tabs.tabs( "refresh" );
                if (selected_tab_id == tab_id)
                    $('a[href="#'+tab_id+'"]').click();
            }

            var tabRenderingMethods = [];

            tabRenderingMethods[0] = function(idx, params) {
                var currentTab = dynamic_tabs[idx];
                var srcNode = currentTab[3];

                var tree = mergeTopDown(trees[currentTab[2]], srcNode);
                tree.id = idx;
                trees[idx] = tree;

                var shortName = tags.toShortHTML(srcNode[M_ID]);
                var html = renderTree(tree, [
                    '<b>Outgoing calls</b> for method ', tags.toHTML(srcNode[M_ID]), '<br>',
                    'This view displays all the calls that were issued by ', shortName,
                    '.<br><br>'
                ]);
                shortName = shortName.replace(/<span/, '<span style="float:left;"');
                Tabs__createTab(idx, "<span class='ui-icon ui-icon-arrowthick-1-se inline-block' style='float:left;'>Outgoing calls</span>" + shortName);
                $('#z' + idx).html(html.join(''));
            };

            tabRenderingMethods[1] = function(idx, params) {
                var currentTab = dynamic_tabs[idx];
                var srcNode = currentTab[3];

                var tree = mergeBottomUp(trees[currentTab[2]], srcNode);
                tree.id = idx;
                trees[idx] = tree;

                var shortName = tags.toShortHTML(srcNode[M_ID]);
                var html = renderTree(tree, [
                    '<b>Incoming calls</b> for method ', tags.toHTML(srcNode[M_ID]),
                    '<br><font color=red><b>Note:</b> this tree is a bottom-up view of the source call tree.<br>All the "self times" mean the time spent in ',
                    shortName, ' without children, and "total times" mean the time spent in ', shortName, ' with children.',
                    '</font><br><br>'
                ]);
                shortName = shortName.replace(/<span/, '<span style="float:left;"');
                Tabs__createTab(idx, "<span class='ui-icon ui-icon-arrowthick-1-nw inline-block' style='float:left;'>Incoming calls</span>" + shortName);
                $('#z' + idx).html(html.join(''));
            };

            tabRenderingMethods[2] = function(idx, params) {
                var currentTab = dynamic_tabs[idx];
                var srcNode = currentTab[3];
                var tree = findUsages(trees[currentTab[2]], srcNode);
                tree.id = idx;
                tree.srcId = srcNode[M_ID];
                trees[idx] = tree;

                var shortName = tags.toShortHTML(srcNode[M_ID]);
                var html = renderTree(tree, [
                    '<b>Find usages</b> for method ', tags.toHTML(srcNode[M_ID]), '<br>',
                    'This view shows the places where ', shortName, ' was used in the original call tree.',
                    '<br><br>'
                ]);
                shortName = shortName.replace(/<span/, '<span style="float:left;"');
                Tabs__createTab(idx, "<span class='ui-icon ui-icon-arrowthickstop-1-s inline-block' style='float:left;'>Find usages</span>" + shortName);
                $('#z' + idx).html(html.join(''));
            };

            tabRenderingMethods[3] = function(idx, params) {
                var currentTab = dynamic_tabs[idx];
                var srcNode = currentTab[3];
                var shortName = tags.toShortHTML(srcNode[M_ID]);
                shortName = shortName.replace(/<span/, '<span style="float:left;"');
                Tabs__createTab(idx, "<span class='ui-icon ui-icon-arrowthickstop-1-n inline-block' style='float:left;'>Local hotspots</span>" + shortName);

                var outgoing = mergeTopDown(trees[currentTab[2]], srcNode);
                outgoing.fw[M_CATEGORY] = srcNode[M_CATEGORY] || Tree__setupBc_parsed[Tree__setupBc_parsed.length - 1];
                CallTree_refreshCategories(outgoing.fw);
                CallTree_renderHotspots(outgoing, idx, '#z' + idx, function(tree, html) {
                    tree.srcId = srcNode[M_ID];
                    html[html.length] = '<b>Local hotspots</b> for method ' + tags.toHTML(srcNode[M_ID]);
                    html[html.length] = '<br><font color=red><b>Note:</b> this tree is a bottom-up view of the source call tree.';
                    html[html.length] = '</font><br><br>';
                });
            };

            var ctrl_pressed = false, mouseX, mouseY;
            var IDE_activeTagId;

            function IDE_highlight(event) {
                var t = event.target;
                if (t.tagName == 'B') t = t.parentNode;
                if (t) {
                    t = $(t);
                    var tagId = t.attr(ATTR_NODE_ID);
                    if (tagId) {
                        var tag = tags.t[tagId];
                        if (tag[T_SOURCE]) {
                            IDE_activeTagId = tagId;
                            var $ide = $('#ide');
                            var $idep = $('#idep');
                            $idep.text(tag[T_PACKAGE]);
                            $('#idec').text(tag[T_CLASS] + '.');
                            $('#idem').text(tag[T_METHOD]);
                            var lineNumber = tag[T_SOURCE];
                            if (lineNumber) {
                                var m = lineNumber.match(/(:\d+)/);
                                if (m)
                                    lineNumber = m[1];
                            }
                            $('#idel').text(lineNumber);
                            var pos = t.offset();
                            $ide.show();
                            pos.left -= $idep.width();
                            $ide.css(pos);
                        }
                    }
                }
            }

            CT.ashGraph = [];
            CT.ashData = {};
            var ashBlockRedraw = false;

            function ASH_render(new_ash_group) {
                selected_ash = new_ash_group;
                var colorMap, colors, j, dateWindow;
                for (var i = 1; i >= 0; i--) {
                    if (CT.ashGraph[i]) {
                        CT.ashGraph[i].destroy();
                        CT.ashGraph[i] = null;
                    }
                    var dstDiv = $('#ash' + i);
                    var ash = CT.ashData[selected_ash], ashData;
                    if (!ash || (ashData = CT.ashData[selected_ash][i]).data.length == 0) {
                        dstDiv.html(
                            ashData ?
                                'Empty dataset received for chart ' + ashData.title
                                : 'No data received for chart #' + i + ' by ' + selected_ash

                        );
                        continue;
                    }
                    var opts = {
                        labels: ashData.labels
                        , legend: ashData.labels.length <= 20 ? 'always' : 'onmouseover'
                        , stepPlot: true
                        , axisLabelFontSize: 13
                        , axes: {
                            x: { pixelsPerLabel: 70 }, y: { pixelsPerLabel: 50 }
                        }, xAxisLabelWidth: 60, showRoller: true
                        , stackedGraph: true
                        , strokeWidth: 1
                        , includeZero: true
                        , title: ashData.title
                        , labelsDivWidth: 500
                        , strokeBorderWidth: null
                        , highlightCircleSize: 2
                        , highlightSeriesOpts: {
                            strokeWidth: 3
                            , strokeBorderWidth: 1
                            , highlightCircleSize: 5
                        }
                        ,drawCallback: function(me, initial) {
                            if (ashBlockRedraw || initial) return;
                            ashBlockRedraw = true;
                            var range = me.xAxisRange();
                            selected_ash_dateWindow = range;
                            for (var j = 0; j < CT.ashGraph.length; j++) {
                                if (CT.ashGraph[j] == me) continue;
                                CT.ashGraph[j].updateOptions( {
                                    dateWindow: range
                                });
                            }
                            ashBlockRedraw = false;
                        }
                    };
                    if (i == 0 && colorMap) {
                        colors = [];
                        for (j = 0; j < ashData.labels.length; j++)
                            colors[j] = colorMap[ashData.labels[j + 1]];
                        opts.colors = colors;
                    }
                    if (selected_ash_dateWindow)
                        opts.dateWindow = selected_ash_dateWindow;
                    else if (dateWindow)
                        opts.dateWindow = dateWindow;
                    CT.ashGraph[i] = new Dygraph(dstDiv[0], ashData.data, opts);
                    if (i == 1) {
                        colorMap = {};
                        colors = CT.ashGraph[i].getColors();
                        for (j = 0; j < ashData.labels.length; j++)
                            colorMap[ashData.labels[j + 1]] = colors[j];
                        if (!selected_ash_dateWindow)
                            dateWindow = CT.ashGraph[i].xAxisRange();
                    }
                }
            }

            CT.ide = function(id) {
                if (id === void 0)
                    id = IDE_activeTagId;
                if (!id) {
                    $notify.notify('create', 'jqn-error', {title: 'Open in IDE failed', text: 'Unable to determine the method to open'}, {expires:5000, custom: true});
                    return;
                }

                var tag = tags.t[id];
                if (!id) {
                    $notify.notify('create', 'jqn-error', {title: 'Open in IDE failed', text: 'Unable to find tag for id ' + id}, {expires:5000, custom: true});
                    return;
                }
                var source = tag[T_SOURCE];
                if (!source) {
                    $notify.notify('create', 'jqn-error', {title: 'Open in IDE failed', text: 'Unable to detect file source'}, {expires:5000, custom: true});
                    return;
                }
                var m = source.match(/\(([^:]+):?(\d*)/);
                if (!m) {
                    $notify.notify('create', 'jqn-error', {title: 'Open in IDE failed', text: 'Unable to detect file source'}, {expires:5000, custom: true});
                    return;
                }
                var file = m[1], line = m[2];
                var pkg = tag[T_PACKAGE];

                if (/^jsp_servlet.*/.test(pkg))
                    file = pkg.replace(/^jsp_servlet._/, '').replace(/\.[_]?/g, '/') + m[1].replace(/__(.*)\.java/, '$1.jsp');
                else
                    file = pkg.replace(/\./g, '/') + m[1];

                var command = 'file?file=' + file + '&line=' + line;
                Activator.doOpen(command);
                return false;
            };

            function refToRexExp(ref) {
                var arr = ref.split("*");
                for (var i = 0; i < arr.length; i++) {
                    arr[i] = escapeRegExp(arr[i]);
                }
                return new RegExp(arr.join(".*"));
            }

            var Tree__adjustDuration_initDone;
            var Tree__adjustDuration_value = '';
            var Tree__adjustDuration_parsed = {};

            function Tree__adjustDuration(node, nodePath, activateDialog) {
                if (!Tree__adjustDuration_initDone)
                    $('#adj_cfg').val(Tree__adjustDuration_value);

                if (node && node[M_ID] != undefined) {
                    var tag = tags.toMethodName(node[M_ID]);

                    if (Tree__adjustDuration_value.indexOf(tag) == -1) {
                        var newVal = Tree__adjustDuration_value;
                        if (newVal.length > 0)
                            newVal += '\n';
                        var koeff = nodePath && nodePath.length <= 3 && node[M_EXECUTIONS] > 1 ? node[M_EXECUTIONS] : 10000;

                        var newAdjLine = '1/' + koeff + ' ' + tag;
                        newVal += newAdjLine;
                        $('#adj_cfg').val(newVal);
                        if (!activateDialog) {
                            var tree = trees[0];
                            var durationBefore = tree.fw[M_DURATION];
                            Tree__adjustDuration_apply();
                            var durationAfter = tree.fw[M_DURATION];
                            app.notify.notify('create', 'jqn-tree-adjusted', {
                                  methodName: tags.toShortHTML(node[M_ID])
                                , adjustFactor: koeff
                                , durationBefore: Duration__formatTime(durationBefore)
                                , durationAfter: Duration__formatTime(durationAfter)
                                , durationDiff: Duration__formatTime(durationBefore - durationAfter)
                                , newAdjLine: newAdjLine
                            }, {expires: 10000, custom:true});
                            return false;
                        }
                    }
                }

                if (!Tree__adjustDuration_initDone) {
                    $('#adj').dialog({
                        title: 'Adjust duration',
                        width: 920,
                        height: 420,
                        resizable: true,
                        buttons: {
                            OK: function() {
                                Tree__adjustDuration_apply();
                                $(this).dialog("close");
                            },
                            Cancel: function() {
                                $('#adj_cfg').val(Tree__adjustDuration_value);
                                $(this).dialog("close");
                            },
                            Apply : function() {
                                Tree__adjustDuration_apply();
                            }
                        }
                    });
                    Tree__adjustDuration_initDone = true;
                }
                $('#adj').dialog('open');
                return false;
            }

            var Tree__adjustTree = CT.setAdjustments = function (tree, adjustments) {
                Tree__adjustDuration_updateValue(adjustments);
                Tree__makeAdjustments(tree, Tree__adjustDuration_parsed);
                if (app.durationFormat == 'SAMPLES') // ensure toal duration is normalized to 100%
                    ESCDataFormat.time_k = 100 / tree.fw[M_DURATION];
            };

            CT.undoAdjustment = function(lineToRemove) {
                var pos = Tree__adjustDuration_value.indexOf(lineToRemove);
                if (pos == -1)
                    return false;
                var val = Tree__adjustDuration_value.substr(0, pos) + Tree__adjustDuration_value.substr(pos + lineToRemove.length + 1);
                $('#adj_cfg').val(val);
                Tree__adjustDuration_apply();
                return false;
            };

            function Tree__adjustDuration_updateValue(newValue) {
                if (Tree__adjustDuration_value == newValue) return false;
                Tree__adjustDuration_value = newValue;
                var lines = newValue.split('\n');
                var pairs = [], i;
                for (i = 0; i < lines.length; i++) {
                    var line = $.trim(lines[i]);
                    if (line.length == 0 || line.charAt(0) == '#') continue;
                    var m = line.match(/(\S+)\s*(.+\S)/);
                    if (!m) continue;
                    var dt = m[1], num;
                    if (dt) {
                        num = Number(dt);
                        if (isNaN(num)) { // parse fraction (e.g. 1/100)
                            dt = dt.replace(/\s/g);
                            var ab = dt.match(/([^/]+)\/(.+)/);
                            if (!ab) continue;
                            num = Number(ab[1]) / Number(ab[2]);
                        }
                        if (isNaN(num))
                            continue;
                    }
                    var pattern = m[2];
                    if (app.durationFormat == 'SAMPLES') {
                        var bra = pattern.indexOf('(');
                        if (bra > -1)
                            pattern = pattern.substr(0, bra);
                    }
                    pairs.push([num, pattern]);
                }
                pairs.sort(function(a, b) {
                    return a[1].length > b[1].length;
                });

                var tgs = tags.t;
                Tree__adjustDuration_parsed = {};
                for (i = 0; i < pairs.length; i++) {
                    var pair = pairs[i];
                    var re = refToRexExp(pair[1]);
                    for (var j in tgs)
                        if (re.test(tgs[j][0]))
                            Tree__adjustDuration_parsed[j] = pair[0];
                }
                return true;
            }

            function Tree__adjustDuration_apply() {
                var newValue = $.trim($('#adj_cfg').val());
                if (!Tree__adjustDuration_updateValue(newValue)) return;
                var tree = trees[0];
                Tree__makeAdjustments(tree, Tree__adjustDuration_parsed);

                if (app.durationFormat == 'SAMPLES') // ensure toal duration is normalized to 100%
                    ESCDataFormat.time_k = 100/tree.fw[M_DURATION];

                var html = renderTreeNoWrap(tree);
                html = html.join('');
                $('#' + DOM_TREE_CONTAINER + tree.id).html(html);
                if (trees[0][CallTree_FILTERED_TREE])
                    CT.filter($('#' + DOM_TREE_FILTER_INPUT + 0)[0]);
                if (selected_tab_id != 'tabs-hotspots') {
                    delete app['tabs-hotspots'];
                    return;
                }
                CallTree_renderHotspots(trees[0]);
            }

            var Tree__setupBc_initDone;
            var Tree__setupBc_value;
            var Tree__setupBc_parsed;

            function Tree__setupBc(node, activateDialog) {
                if (!Tree__setupBc_initDone)
                    $('#bc_cfg').val(Tree__setupBc_value);

                if (node && node[M_ID] != undefined) {
                    var tag = tags.toMethodName(node[M_ID]);

                    if (Tree__setupBc_value.indexOf(tag) == -1) {
                        var newVal = Tree__setupBc_value;
                        if (newVal.length > 0)
                            newVal += '\n';
                        var tg = tags.t[node[M_ID]];
                        var categoryName = tg[T_CLASS] + '.' + tg[T_METHOD];
                        var newCategoryLine = categoryName + ' ' + tag;
                        if (Tree__setupBc_value == CT.defaultCategories)
                            newVal = newCategoryLine;
                        else
                            newVal += newCategoryLine;
                        $('#bc_cfg').val(newVal);
                        if (!activateDialog) {
                            Tree__setupBc_apply();
                            app.notify.notify('create', 'jqn-tree-category-added', {
                                  methodName: tags.toShortHTML(node[M_ID])
                                , categoryName: categoryName
                                , duration: Duration__formatTime(1000)
                                , newCategoryLine: newCategoryLine
                            }, {expires: 10000, custom:true});
                            return false;
                        }
                    }
                }

                if (!Tree__setupBc_initDone) {
                    $('#setup-bc').dialog({
                        title: 'Setup business categories',
                        width: 920,
                        height: 520,
                        resizable: true,
                        buttons: {
                            OK: function() {
                                Tree__setupBc_apply();
                                $(this).dialog("close");
                            },
                            Cancel: function() {
                                $('#bc_cfg').val(Tree__setupBc_value);
                                $(this).dialog("close");
                            },
                            Apply : function() {
                                Tree__setupBc_apply();
                            }
                        }
                    });
                    Tree__setupBc_initDone = true;
                }
                $('#setup-bc').dialog('open');
                return false;
            }

            function Tree__setupBc_apply() {
                var newValue = $.trim($('#bc_cfg').val());
                if (!Tree_setupBc_updateValue(newValue)) return;

                var tree = trees[0];
                CallTree_cleanCategories(tree.fw);
                tree.fw[M_CATEGORY] = Tree__setupBc_parsed[Tree__setupBc_parsed.length - 1];
                CallTree_refreshCategories(tree.fw);
                var html = renderTreeNoWrap(tree);
                html = html.join('');
                $('#' + DOM_TREE_CONTAINER + tree.id).html(html);
                if (trees[0][CallTree_FILTERED_TREE])
                    CT.filter($('#' + DOM_TREE_FILTER_INPUT + 0)[0]);
                if (selected_tab_id != 'tabs-hotspots') {
                    delete app['tabs-hotspots'];
                    return;
                }
                CallTree_renderHotspots(trees[0]);
            }

            var Tree__setupBc_setTreeCategories = CT.setCategories = function (tree, categories) {
                Tree_setupBc_updateValue(categories);
                tree.fw[M_CATEGORY] = Tree__setupBc_parsed[Tree__setupBc_parsed.length - 1]; // set "undefined" category for root
                CallTree_refreshCategories(tree.fw);
            }

            CT.undoCategory = function(lineToRemove) {
                var pos = Tree__setupBc_value.indexOf(lineToRemove);
                if (pos == -1)
                    return false;
                var val = Tree__setupBc_value.substr(0, pos) + Tree__setupBc_value.substr(pos + lineToRemove.length + 1);
                $('#bc_cfg').val(val)
                Tree__setupBc_apply();
                return false;
            }

            function Tree_setupBc_updateValue(newValue) {
                if (Tree__setupBc_value == newValue) return false;
                Tree__setupBc_value = newValue;
                var lines = newValue.split('\n');
                var pairs = [], i;
                for (i = 0; i < lines.length; i++) {
                    var line = $.trim(lines[i]);
                    if (line.length == 0 || line.charAt(0) == '#') continue;
                    var m = line.match(/(\S+)\s*(.+\S)/);
                    if (!m) continue;
                    var pattern = m[2];
                    if (app.durationFormat == 'SAMPLES') {
                        var bra = pattern.indexOf('(');
                        if (bra > -1)
                            pattern = pattern.substr(0, bra);
                    }

                    if (pattern.charAt(0) == '>')
                        pairs.push([m[1], pattern.substr(1), BC_NEXT]);
                    else
                        pairs.push([m[1], pattern, BC_CURRENT]);
                }
                pairs.sort(function(a, b) {
                    return a[1].length < b[1].length;
                });

                var name2item = {};
                Tree__setupBc_parsed = [];
                for (i = 0; i < pairs.length; i++) {
                    var pair = pairs[i];
                    pair[3] = refToRexExp(pair[1]);
                    var bcName = pair[0];
                    if (name2item[bcName]) continue;
                    Tree__setupBc_parsed.push(name2item[bcName] = ['hsl(' + (Tree__setupBc_parsed.length * 150 % 360) + ',100%,95%)', bcName]);
                }
                Tree__setupBc_parsed.push(['#fff', 'unsorted']);

                var tgs = tags.t, len = pairs.length;
                for (var j in tgs) {
                    var tag = tgs[j], tagName = tag[0];
                    for (i = 0; i < len && !pairs[i][3].test(tagName); i++);
                    if (i < len) {
                        tag[T_CATEGORY] = name2item[pairs[i][0]];
                        tag[T_CATEGORY_ACTIVE] = pairs[i][2];
                    } else {
                        delete tag[T_CATEGORY];
                        delete tag[T_CATEGORY_ACTIVE];
                    }
                }
                return true;
            }

            var Tree__setupPersonal_initDone;

            function Tree__setupPersonal() {
                if (!Tree__setupPersonal_initDone) {
                    Tree__setupPersonal_init();
                    $('#setup-p').dialog({
                                title: 'Personal settings',
                                width: 390,
                                height: 220,
                                resizable: true,
                                buttons: {
                                    OK: function() {
                                        Tree__setupPersonal_apply();
                                        Tree__setupPersonal_refreshTrees();
                                        $(this).dialog("close");
                                    },
                                    Cancel: function() {
                                        Tree__setupPersonal_init();
                                        $(this).dialog("close");
                                    },
                                    Apply: function() {
                                        Tree__setupPersonal_apply();
                                        Tree__setupPersonal_preview();
                                    }
                                }
                            });
                    Tree__setupPersonal_initDone = true;
                }
                $('#setup-p').dialog('open');
                return false;
            }

            function Tree__setupPersonal_init() {
                $('#ps-ms-' + profiler_settings.millis_format).attr('checked', true);
                $('#ps-sm').val(profiler_settings.omit_ms);
                $('#ps-int-' + profiler_settings.int_format).attr('checked', true);
                $('#ps-thr-mode-' + profiler_settings.threaddump_format).attr('checked', true);
                $('#ps-gc-mode-' + profiler_settings.gc_show_mode).attr('checked', true);
                $('#ps-thr-stack-dur').val(profiler_settings.thr_stack_duration);
            }

            function Tree__setupPersonal_apply() {
                var $dialog = $('#setup-p');
                profiler_settings.millis_format = $dialog.children('input[name=ps-millis]:checked').attr('id').substr(6);
                profiler_settings.omit_ms = $('#ps-sm').val();
                profiler_settings.int_format = $dialog.children('input[name=ps-ints]:checked').attr('id').substr(7);
                profiler_settings.threaddump_format = $dialog.children('input[name=ps-thr-mode]:checked').attr('id').substr(12);
                profiler_settings.gc_show_mode = $dialog.children('input[name=ps-gc-mode]:checked').attr('id').substr(11);
                profiler_settings.thr_stack_duration = $('#ps-thr-stack-dur').val();
                ESCProfilerSettings.ProfilerSettings__save();
                updateFormatFromPersonalSettings();
            }

            function Tree__setupPersonal_preview() {
                var treeId;

                if (selected_tab_id == 'tabs-hotspots') {
                    CallTree_renderHotspots(trees[0]);
                    return;
                }

                if (selected_tab_id == 'tabs-call-tree')
                    treeId = 0;
                else if (/z\d+/.test(selected_tab_id))
                    treeId = Number(selected_tab_id.substr(1));
                else return;

                Tree__setupPersonal_refreshTree(treeId);
            }

            function Tree__setupPersonal_refreshTree(treeId) {
                var html = renderTreeNoWrap(trees[treeId]);
                html = html.join('');
                $('#' + DOM_TREE_CONTAINER + treeId).html(html);
                if (trees[treeId][CallTree_FILTERED_TREE])
                    CT.filter($('#' + DOM_TREE_FILTER_INPUT + treeId)[0]);
            }

            function Tree__setupPersonal_refreshTrees() {
                if (selected_tab_id == 'tabs-hotspots')
                    CallTree_renderHotspots(trees[0]);
                else delete app['tabs-hotspots'];

                Tree__setupPersonal_refreshTree(0);

                for (var treeId in dynamic_tabs) {
                    Tree__setupPersonal_refreshTree(treeId);
                }
            }

            function orderCallsByDuration(a, b) {
                var u = a[M_PREV_SELF_DURATION], v = b[M_PREV_SELF_DURATION];
                if (u < 0)
                    return v < 0 ? u - v : -1;
                else if (v < 0)
                    return 1;
                var x = b[M_DURATION] - a[M_DURATION];
                if (x != 0) return x;
                x = b[M_SELF_DURATION] - a[M_SELF_DURATION];
                if (x != 0) return x;
                x = b[M_SUSPENSION] - a[M_SUSPENSION];
                if (x != 0) return x;
                x = b[M_SELF_SUSPENSION] - a[M_SELF_SUSPENSION];
                if (x != 0) return x;
                x = b[M_EXECUTIONS] + b[M_CHILD_EXECUTIONS] - a[M_CHILD_EXECUTIONS] - a[M_EXECUTIONS];
                if (x != 0) return x;
                return b[M_EXECUTIONS] - a[M_EXECUTIONS];
            }

            function orderCallsBySelfDuration(a, b) {
                var u = a[M_PREV_SELF_DURATION], v = b[M_PREV_SELF_DURATION];
                if (u < 0)
                    return v < 0 ? u - v : -1;
                else if (v < 0)
                    return 1;
                var x = b[M_SELF_DURATION] - a[M_SELF_DURATION];
                if (x != 0) return x;
                x = b[M_SELF_SUSPENSION] - a[M_SELF_SUSPENSION];
                if (x != 0) return x;
                x = b[M_DURATION] - a[M_DURATION];
                if (x != 0) return x;
                x = b[M_SUSPENSION] - a[M_SUSPENSION];
                if (x != 0) return x;
                x = b[M_EXECUTIONS] - a[M_EXECUTIONS];
                if (x != 0) return x;
                return b[M_EXECUTIONS] + b[M_CHILD_EXECUTIONS] - a[M_CHILD_EXECUTIONS] - a[M_EXECUTIONS];
            }

            function orderTagsByDuration(a, b) {
                var x = b[P_DURATION] - a[P_DURATION];
                if (x != 0) return x;
                x = b[P_EXECUTIONS] - a[P_EXECUTIONS];
                //                return x;
                if (x != 0) return x;
                x = tags.t[a[P_ID]][0];
                var y = tags.t[b[P_ID]][0];
                return x > y ? 1 : (x < y ? -1 : 0);
            }

            function getTagModule(id, modules) {
                var t = tags.t[id];
                if (t.length == 1) return 'other';
                var name = t[T_PACKAGE] + t[T_CLASS];

                for (var i = 0, modulesLength = modules.length; i < modulesLength; i++) {
                    var module = modules[i][1];
                    var len = module.length - 2;
                    if (name.length < len + 2) continue;
                    if (name[len] == module[len] && name[len - 2] == module[len - 2] &&
                            modules[i][2].test(name))
                        return modules[i][0];
                }
                return 'other';
            }

            function getChildren(node) {
                var t = node[M_CHILDREN];
                if (!t)
                    return node[M_CHILDREN] = [];
                return t;
            }

            function getOrCreateChild(a, node) {
                var j;
                var nodeId = node[M_ID], nodeSignature = getMethodSignature(node);
                for (j = 0; j < a.length; j++) {
                    var a_j = a[j];
                    if (a_j[M_ID] == nodeId && getMethodSignature(a_j) == nodeSignature)
                        break;
                }

                if (j == a.length) {
                    return a[j] = Tree__createNode(nodeId);
                }
                return a[j];
            }

            function Tree__createNode(id) {
                return [id,
                    0 /*M_DURATION*/,
                    0 /*M_SELF_DURATION*/,
                    0 /*M_SUSPENSION*/,
                    0 /*M_SELF_SUSPENSION*/,
                    0 /*M_EXECUTIONS*/,
                    0 /*M_CHILD_EXECUTIONS*/,
                    0 /*M_START_TIME*/,
                    0 /*M_END_TIME*/,
                    [],/*M_CHILDREN*/
                    0 /* M_COLLAPSE_LEVELS */
                ];
            }

            function Tree__cloneNode(id,
                                     duration,
                                     self_duration,
                                     suspension,
                                     self_suspension,
                                     executions,
                                     child_executions) {
                return [id,
                    duration,
                    self_duration,
                    suspension,
                    self_suspension,
                    executions,
                    child_executions,
                    0 /*M_START_TIME*/,
                    0 /*M_END_TIME*/,
                    [],
                    0 /* M_COLLAPSE_LEVELS */
                ];
            }

            function mergeNodes(dst, src) {
                dst[M_EXECUTIONS] += src[M_EXECUTIONS];
                dst[M_SELF_DURATION] += src[M_SELF_DURATION];
                dst[M_SELF_SUSPENSION] += src[M_SELF_SUSPENSION];

                var x = src[M_TAGS];
                if (!x || x.length == 0) return;

                var y = dst[M_TAGS];
                if (!y)
                    y = dst[M_TAGS] = [];

                // Merge tags
                for (var i = 0; i < x.length; i++) {
                    var a = x[i], j;
                    for (j = 0; j < y.length && (y[j][P_ID] != a[P_ID] || y[j][P_VALUE] != a[P_VALUE]); j++) {
                    }
                    if (j == y.length)
                        y[j] = [a[P_ID], a[P_DURATION], a[P_EXECUTIONS], a[P_VALUE]];
                    else {
                        var y_j = y[j];
                        y_j[P_DURATION] += a[P_DURATION];
                        y_j[P_EXECUTIONS] += a[P_EXECUTIONS];
                    }
                }
            }

            function findNodeAndCopyTopDown(dst_root, tree, id, sign, dst_node) {
                if (tree[M_ID] == id && getMethodSignature(tree) == sign) {
                    if (dst_root != dst_node) {
                        findNodeAndCopyTopDown(dst_root, tree, id, sign, dst_root);
                        return;
                    }
                }

                mergeNodes(dst_node, tree);

                var t = tree[M_CHILDREN];
                if (!t || t.length == 0) return 0;

                var dst_child = getChildren(dst_node);

                for (var i = 0; i < t.length; i++) {
                    var t_i = t[i];
                    var y = getOrCreateChild(dst_child, t_i);

                    findNodeAndCopyTopDown(dst_root, t_i, id, sign, y);
                }
            }

            function findNodeTopDown(dst, tree, id, sign) {
                if (tree[M_ID] == id && getMethodSignature(tree) == sign) {
                    findNodeAndCopyTopDown(dst, tree, id, sign, dst);
                    return;
                }
                var t = tree[M_CHILDREN];
                if (!t || t.length == 0) return;

                for (var i = 0; i < t.length; i++) {
                    findNodeTopDown(dst, t[i], id, sign);
                }
            }

            function computeTotals(tree, prevTree) {
                var t = tree[M_CHILDREN];
                if (t && t.length > 0)
                    for (var i = 0; i < t.length; i++) {
                        computeTotals(t[i], tree);
                    }
                tree[M_DURATION] += tree[M_SELF_DURATION];
                tree[M_SUSPENSION] += tree[M_SELF_SUSPENSION];

                prevTree[M_DURATION] += tree[M_DURATION];
                prevTree[M_CHILD_EXECUTIONS] += tree[M_EXECUTIONS] + tree[M_CHILD_EXECUTIONS];
                prevTree[M_SUSPENSION] += tree[M_SUSPENSION];
            }

            function mergeTopDown(tree, node) {
                var root = Tree__createNode(node[M_ID]);

                findNodeTopDown(root, tree.fw, node[M_ID], getMethodSignature(node));

                var t = root[M_CHILDREN];
                if (t && t.length > 0)
                    for (var i = 0; i < t.length; i++) {
                        computeTotals(t[i], root);
                    }
                root[M_DURATION] += root[M_SELF_DURATION];
                root[M_SUSPENSION] += root[M_SELF_SUSPENSION];

                sortNode(root);

                return new CallTree(null, root, CallTree_TYPE_OUT_CALLS);
            }

            function sortNode(node, cmp, parentNode) {
                if (!cmp)
                    cmp = orderCallsByDuration;
                var t = node[M_CHILDREN];
                if (t && t.length > 0) {
                    if (t.length > 1)
                        t.sort(cmp);

                    var firstChild = t[0];
                    var canCollapse = sortNode(firstChild, cmp, node);
                    if (node[M_TAGS] && node[M_TAGS].length > 0 || node[M_PREV_SELF_DURATION] == -2) canCollapse = -2;
                    else if (
                        ( cmp === orderCallsByDuration &&
                            (node[M_DURATION] - node[M_SELF_DURATION] - firstChild[M_DURATION] + firstChild[M_SELF_DURATION]) * 10 <= node[M_DURATION] &&
                            (node[M_DURATION] != 0 || (node[M_CHILD_EXECUTIONS] - firstChild[M_CHILD_EXECUTIONS] + firstChild[M_EXECUTIONS]) * 10 < (node[M_CHILD_EXECUTIONS] + node[M_EXECUTIONS])) &&
                            (node[M_EXECUTIONS] == 0 || node[M_EXECUTIONS] * 5 > firstChild[M_EXECUTIONS])
                            ) ||
                            ( cmp !== orderCallsByDuration &&
                                (node[M_SELF_DURATION] - firstChild[M_SELF_DURATION]) * 10 <= node[M_SELF_DURATION] &&
                                (node[M_SELF_DURATION] != 0 || (node[M_EXECUTIONS] - firstChild[M_EXECUTIONS]) * 10 < node[M_EXECUTIONS])
                                ))
                        if (canCollapse >= 0) canCollapse++;
                        else canCollapse--;
                    else if (cmp === orderCallsByDuration && !(node[M_EXECUTIONS] == 0 || node[M_EXECUTIONS] * 5 > firstChild[M_EXECUTIONS])) canCollapse = -1;
                    else canCollapse = canCollapse < 0 ? -3 : 0;

                    for (var i = 1, tLength = t.length; i < tLength; i++) {
                        var canCollapseChild = sortNode(t[i], cmp, node);
                        if (canCollapseChild < 0 && canCollapse > 0) canCollapse = -3;
                    }

                    if (cmp !== orderCallsByDuration && parentNode && parentNode[M_CHILD_EXECUTIONS] > node[M_CHILD_EXECUTIONS] * 5)
                        canCollapse = -1;

                    node[M_COLLAPSE_LEVELS] = canCollapse < -2 ? -3 - canCollapse : (canCollapse > 0 ? canCollapse : 0);
                    return canCollapse;
                }

                return node[M_COLLAPSE_LEVELS] = 0;
            }

            function mergeBottomUp(tree, srcNode, root, justMerge, requiredBc) {
                if (!root)
                    root = Tree__createNode(TAGS_ROOT);

                var srcId = srcNode[M_ID];
                var srcSignature = getMethodSignature(srcNode);
                var nodes = [], time = [];

                function getOrCreateChild(a, id, sign) {
                    var j;
                    for (j = 0; j < a.length; j++) {
                        var aj = a[j];
                        if (aj[M_ID] == id && getMethodSignature(aj) == sign)
                            break;
                    }

                    if (j == a.length) {
                        var newNode = Tree__createNode(id);
                        newNode[M_SIGNATURE] = sign;
                        a[j] = newNode;
                        return newNode;
                    }
                    return a[j];
                }

                function appendStacktrace(level,
                                          duration,
                                          selfDuration,
                                          suspension,
                                          selfSuspension,
                                          executions) {
                    var node = root;
                    node[M_EXECUTIONS] += executions;
                    node[M_DURATION] += duration;
                    node[M_SELF_DURATION] += selfDuration;
                    node[M_SUSPENSION] += suspension;
                    node[M_SELF_SUSPENSION] += selfSuspension;
                    node[M_CHILD_EXECUTIONS] += executions;
                    for (var i = level; i >= 0; i--) {
                        var x = nodes[i];
                        var xSign = getMethodSignature(x);
                        node = getOrCreateChild(node[M_CHILDREN], x[M_ID], xSign);
                        node[M_EXECUTIONS] += executions;
                        node[M_CHILD_EXECUTIONS] += x[M_EXECUTIONS];
                        node[M_DURATION] += duration;
                        node[M_SELF_DURATION] += selfDuration;
                        node[M_SUSPENSION] += suspension;
                        node[M_SELF_SUSPENSION] += selfSuspension;

                        var xTags = x[M_TAGS];
                        if (i == 0 || !xTags || xTags.length == 0) continue;

                        var nodeTags = node[M_TAGS];
                        if (!nodeTags)
                            nodeTags = node[M_TAGS] = [];

                        for (var j = 0, xTagsLen = xTags.length; j < xTagsLen; j++) {
                            var a = xTags[j], k;
                            var nodeTagsLen = nodeTags.length;
                            for (k = 0; k < nodeTagsLen; k++) {
                                var nodeTagk = nodeTags[k];
                                if (nodeTagk[P_ID] == a[P_ID] && nodeTagk[P_VALUE] == a[P_VALUE]) break;
                            }
                            if (k == nodeTagsLen)
                                nodeTags[k] = [a[P_ID], a[P_DURATION], a[P_EXECUTIONS], a[P_VALUE]];
                            else {
                                var y_k = nodeTags[k];
                                y_k[P_DURATION] += a[P_DURATION];
                                y_k[P_EXECUTIONS] += a[P_EXECUTIONS];
                            }
                        }
                    }
                }

                function findNodeBottomUp(node, level) {
                    time[level] = 0;
                    nodes[level] = node;
                    var t = node[M_CHILDREN];
                    if (t && t.length > 0)
                        for (var i = t.length - 1; i >= 0; i--)
                            findNodeBottomUp(t[i], level + 1);

                    if (node[M_ID] != srcId || getMethodSignature(node) != srcSignature) {
                        time[level - 1] += time[level];
                        return;
                    }

                    var duration = node[M_DURATION] - time[level];
                    appendStacktrace(
                        level,
                        duration,
                        node[M_SELF_DURATION],
                        node[M_SUSPENSION],
                        node[M_SELF_SUSPENSION],
                        node[M_EXECUTIONS]
                    );
                    time[level - 1] += duration;
                }

                function findNodeBottomUpWithCategories(node, level, bc) {
                    var nodeId = node[M_ID];

                    var newBc = node[M_CATEGORY];
                    if (newBc !== undefined && newBc !== bc) bc = newBc;

                    time[level] = 0;
                    nodes[level] = node;
                    var t = node[M_CHILDREN];
                    if (t && t.length > 0)
                        for (var i = t.length - 1; i >= 0; i--)
                            findNodeBottomUpWithCategories(t[i], level + 1, bc);

                    if (node[M_ID] != srcId || getMethodSignature(node) != srcSignature || bc !== requiredBc) {
                        time[level - 1] += time[level];
                        return;
                    }

                    var duration = node[M_DURATION] - time[level];
                    appendStacktrace(
                        level,
                        duration,
                        node[M_SELF_DURATION],
                        node[M_SUSPENSION],
                        node[M_SELF_SUSPENSION],
                        node[M_EXECUTIONS]
                    );
                    time[level - 1] += duration;
                }

                if (!requiredBc)
                    findNodeBottomUp(tree.fw, 0);
                else
                    findNodeBottomUpWithCategories(tree.fw, 0);

                if (justMerge) return root;

                if (root[M_ID] == TAGS_ROOT && root[M_CHILDREN].length == 1) {
                    var reactorFrame = root[M_REACTOR_FRAME];
                    root = root[M_CHILDREN][0];
                    root[M_REACTOR_FRAME] = reactorFrame;
                }

                sortNode(root, orderCallsBySelfDuration);
                return new CallTree(null, root, CallTree_TYPE_IN_CALLS, true);
            }

            function computeFlatProfile(tree) {
                var bcContext = {}; // categoryName -> [bc, {recursiveNodes}, [flatProfile], {nodePk -> nodeTimings}]

                function getBcContext(bc) {
                    var bcId = bc[BC_NAME];
                    var result = bcContext[bcId];
                    if (!result)
                        result = bcContext[bcId] = [bc, {}, [], {}];
                    return result;
                }

                function appendTiming(node, pk, nodeIsRegular, flatProfile, nodePkToTimings) {
                    var dst = nodePkToTimings[pk];
                    if (!dst)
                        dst = flatProfile[flatProfile.length] = nodePkToTimings[pk] = Tree__createNode(node[M_ID]);
                    dst[M_EXECUTIONS] += node[M_EXECUTIONS];
                    dst[M_SELF_DURATION] += node[M_SELF_DURATION];
                    dst[M_SELF_SUSPENSION] += node[M_SELF_SUSPENSION];
                    if (nodeIsRegular) {
                        dst[M_CHILD_EXECUTIONS] += node[M_CHILD_EXECUTIONS];
                        dst[M_DURATION] += node[M_DURATION];
                        dst[M_SUSPENSION] += node[M_SUSPENSION];
                    } else
                        dst[M_CHILD_EXECUTIONS] -= node[M_EXECUTIONS];

                    var nodeTags = node[M_TAGS];
                    if (!nodeTags || nodeTags.length == 0) return;

                    var dstTags = dst[M_TAGS];
                    if (!dstTags) {
                        dstTags = dst[M_TAGS] = [];
                        dstTags[P_ID2ITEM_MAP] = {};
                    }
                    var dstTagsMap = dstTags[P_ID2ITEM_MAP];

                    for (var i = 0, xTagsLen = nodeTags.length; i < xTagsLen; i++) {
                        var tag = nodeTags[i];
                        var tagPK = tag[P_ID] + '.' + tag[P_VALUE];

                        var dstTag = dstTagsMap[tagPK];
                        if (!dstTag)
                            dstTags[dstTags.length] = dstTagsMap[tagPK] = [tag[P_ID], tag[P_DURATION], tag[P_EXECUTIONS], tag[P_VALUE]];
                        else {
                            dstTag[P_DURATION] += tag[P_DURATION];
                            dstTag[P_EXECUTIONS] += tag[P_EXECUTIONS];
                        }
                    }
                }

                function findNodeBottomUp(node, bc, recursiveNodes, flatProfile, nodePkToTimings) {
                    var nodeId = node[M_ID];
                    var pk = nodeId + '.' + getMethodSignature(node);

                    var newBc = node[M_CATEGORY];

                    if (newBc !== undefined && newBc !== bc) {
                        bc = newBc;
                        var newCtx = getBcContext(bc);
                        recursiveNodes = newCtx[BC_CTX_RECURSIVE_NODES];
                        flatProfile = newCtx[BC_CTX_FLAT_PROFILE];
                        nodePkToTimings = newCtx[BC_CTX_NODE_TO_TIMES];
                    }

                    var nodeIsRegular = recursiveNodes[pk] === undefined;

                    if (nodeId >= 0)
                        appendTiming(node, pk, nodeIsRegular, flatProfile, nodePkToTimings);

                    var t = node[M_CHILDREN];
                    if (!t || t.length == 0) return;

                    if (nodeIsRegular)
                        recursiveNodes[pk] = true;

                    for (var i = t.length - 1; i >= 0; i--)
                        findNodeBottomUp(t[i], bc, recursiveNodes, flatProfile, nodePkToTimings);

                    if (nodeIsRegular)
                        delete recursiveNodes[pk];
                }

                findNodeBottomUp(tree.fw);

                var totalNodes = 0;
                for (var bc in bcContext) {
                    var ctx = bcContext[bc];
                    delete ctx[BC_CTX_NODE_TO_TIMES];
                    totalNodes += ctx[BC_CTX_FLAT_PROFILE].length;
                }

                return [bcContext, totalNodes];
            }

            function findUsages(tree, srcNode, root, justMerge) {
                if (!root)
                    root = Tree__createNode(TAGS_ROOT);

                var srcId = srcNode[M_ID];
                var srcSignature = getMethodSignature(srcNode);

                var nodes = [], time = [], minLevel = 0;

                function getOrCreateChild(a, id, sign) {
                    var j;
                    for (j = 0; j < a.length; j++) {
                        var aj = a[j];
                        if (aj[M_ID] == id && getMethodSignature(aj) == sign)
                            break;
                    }

                    if (j == a.length) {
                        var newNode = Tree__createNode(id);
                        newNode[M_SIGNATURE] = sign;
                        a[j] = newNode;
                        return newNode;
                    }
                    return a[j];
                }

                function appendStacktrace(level,
                                          duration,
                                          selfDuration,
                                          suspension,
                                          selfSuspension,
                                          executions) {
                    var node = root;
                    node[M_EXECUTIONS] += executions;
                    node[M_DURATION] += duration;
                    node[M_SELF_DURATION] += selfDuration;
                    node[M_SUSPENSION] += suspension;
                    node[M_SELF_SUSPENSION] += selfSuspension;
                    for (var i = 0; i <= level; i++) {
                        var x = nodes[i];
                        var xSign = getMethodSignature(x);
                        node = getOrCreateChild(node[M_CHILDREN], x[M_ID], xSign);
                        node[M_EXECUTIONS] += executions;
                        node[M_DURATION] += duration;
                        node[M_SELF_DURATION] += selfDuration;
                        node[M_SUSPENSION] += suspension;
                        node[M_SELF_SUSPENSION] += selfSuspension;
                        if (i < minLevel)
                            continue;
                        node[M_CHILD_EXECUTIONS] += x[M_EXECUTIONS];

                        var xTags = x[M_TAGS];
                        if (!xTags || xTags.length == 0) continue;

                        var nodeTags = node[M_TAGS];
                        if (!nodeTags)
                            nodeTags = node[M_TAGS] = [];

                        for (var j = 0, xTagsLen = xTags.length; j < xTagsLen; j++) {
                            var a = xTags[j], k;
                            var nodeTagsLen = nodeTags.length;
                            for (k = 0; k < nodeTagsLen; k++) {
                                var nodeTagk = nodeTags[k];
                                if (nodeTagk[P_ID] == a[P_ID] && nodeTagk[P_VALUE] == a[P_VALUE]) break;
                            }
                            if (k == nodeTagsLen)
                                nodeTags[k] = [a[P_ID], a[P_DURATION], a[P_EXECUTIONS], a[P_VALUE]];
                            else {
                                var y_k = nodeTags[k];
                                y_k[P_DURATION] += a[P_DURATION];
                                y_k[P_EXECUTIONS] += a[P_EXECUTIONS];
                            }
                        }
                    }
                    minLevel = i;
                }

                function findNodeBottomUp(node, level) {
                    time[level] = 0;
                    nodes[level] = node;
                    var t = node[M_CHILDREN];
                    if (t && t.length > 0)
                        for (var i = t.length - 1; i >= 0; i--)
                            findNodeBottomUp(t[i], level + 1);

                    if (node[M_ID] != srcId || getMethodSignature(node) != srcSignature) {
                        time[level - 1] += time[level];
                        if (level < minLevel)
                            minLevel = level;
                        return;
                    }

                    var duration = node[M_DURATION] - time[level];
                    appendStacktrace(
                        level,
                        duration,
                        node[M_SELF_DURATION],
                        node[M_SUSPENSION],
                        node[M_SELF_SUSPENSION],
                        node[M_EXECUTIONS]
                    );
                    time[level - 1] += duration;
                    if (level < minLevel)
                        minLevel = level;
                }

                findNodeBottomUp(tree.fw, 0);

                if (justMerge) return root;

                if (root[M_ID] == TAGS_ROOT && root[M_CHILDREN].length == 1)
                    root = root[M_CHILDREN][0];

                sortNode(root, orderCallsBySelfDuration);
                return new CallTree(null, root, CallTree_TYPE_FIND_USAGES, true);
            }

            function Tree__computeIncoming(tree, node) {
                var root = mergeBottomUp(tree.srcTree, node, null, true, node[M_CATEGORY]);
                delete node[M_NOT_COMPUTED];
                root = root[M_CHILDREN];
                if (!root || root.length == 0) return;
                node[M_CHILDREN] = root[0][M_CHILDREN];
                sortNode(node, orderCallsBySelfDuration);
                node[M_COLLAPSE_LEVELS] = 0;
            }

            function Tree__gatherIndexedParams(tree, max_depth) {
                var root = Tree__createNode(-3);

                var dstTags = root[M_TAGS] = [];
                var dstTagsMap = {}, paramInfo = tags.y;

                if (!max_depth) max_depth = 99999;

                function findNodeBottomUp(node, max_depth) {
                    var t = node[M_CHILDREN], i;
                    if (t && t.length > 0) {
                        max_depth--;
                        for (i = t.length - 1; i >= 0; i--)
                            findNodeBottomUp(t[i], max_depth);
                    }

                    var tags = node[M_TAGS];
                    if (!tags || tags.length == 0) return;

                    for (i = tags.length - 1; i >= 0; i--) {
                        var tag = tags[i];
                        var tagId = tag[P_ID];
                        var info = paramInfo[tagId];
                        if (!info || !info[T_TYPE_INDEX]) continue;
                        var tagPK = tagId + '.' + tag[P_VALUE];

                        var dstTag = dstTagsMap[tagPK];
                        if (!dstTag)
                            dstTags[dstTags.length] = dstTagsMap[tagPK] = [tag[P_ID], tag[P_DURATION], tag[P_EXECUTIONS], tag[P_VALUE]];
                        else {
                            dstTag[P_DURATION] += tag[P_DURATION];
                            dstTag[P_EXECUTIONS] += tag[P_EXECUTIONS];
                        }
                    }
                }

                findNodeBottomUp(tree.fw, max_depth);

                return new CallTree(null, root, undefined, true);
            }

            function Tree__makeAdjustments(tree, map) {
                var duration = 0, gc = 0, calls = 0, adj = 0;

                function walk(node, k) {
                    var startDuration = duration, startGc = gc, startCalls = calls, startAdj = adj;
                    var node_id = node[M_ID];
                    delete node[M_CATEGORY];
                    var dt = map[node_id], i;
                    if (dt !== undefined) {
                        k *= dt;
                        adj++; // increase number of adjusted nodes
                    }

                    var t = node[M_CHILDREN];
                    if (t && t.length > 0)
                        for (i = t.length - 1; i >= 0; i--)
                            walk(t[i], k);

                    var firstUpdate = !(M_PREV_SELF_DURATION in node);
                    if (adj == startAdj && firstUpdate && k == 1) {
                        duration += node[M_SELF_DURATION];
                        gc += node[M_SELF_SUSPENSION];
                        calls += node[M_EXECUTIONS];
                        return;
                    }

                    var childDuration = duration - startDuration;
                    var childGc = gc - startGc;
                    var childCalls = calls - startCalls;

                    if (firstUpdate) {
                        node[M_PREV_SELF_DURATION] = node[M_SELF_DURATION];
                        node[M_PREV_SELF_SUSPENSION] = node[M_SELF_SUSPENSION];
                        node[M_PREV_EXECUTIONS] = node[M_EXECUTIONS];
                    }

                    var newSelfDuration = node[M_PREV_SELF_DURATION] * k;
                    var newDuration;
                    var newSelfSuspension = node[M_PREV_SELF_SUSPENSION] * k;
                    var newSuspension = childGc + newSelfSuspension;
                    var newExecutions = node[M_PREV_EXECUTIONS] * k;

                    duration += newSelfDuration;
                    gc += newSelfSuspension;

                    node[M_DURATION] = newDuration;
                    node[M_SELF_DURATION] = newSelfDuration;
                    node[M_SUSPENSION] = newSuspension;
                    node[M_SELF_SUSPENSION] = newSelfSuspension;
                    node[M_EXECUTIONS] = newExecutions;
                    node[M_CHILD_EXECUTIONS] = childCalls;

                    calls += newExecutions;

                    var tags = node[M_TAGS], tag;
                    if (!tags || tags.length == 0) return;

                    if (firstUpdate) {
                        for (i = tags.length - 1; i >= 0; i--) {
                            tag = tags[i];
                            tag[P_PREV_DURATION] = tag[P_DURATION];
                        }
                    }

                    var prevDuration = 0;
                    for (i = tags.length - 1; i >= 0; i--)
                        prevDuration += tags[i][P_PREV_DURATION];

                    if (prevDuration != 0) {
                        var kTags = (newDuration + newSuspension) / prevDuration;

                        for (i = tags.length - 1; i >= 0; i--) {
                            tag = tags[i];
                            tag[P_DURATION] = Math.round(tag[P_PREV_DURATION] * kTags);
                        }
                    }
                }

                walk(tree.fw, 1);

                sortNode(tree.fw, tree.rv ? orderCallsBySelfDuration : orderCallsByDuration);
                tree.fw[M_CATEGORY] = Tree__setupBc_parsed[Tree__setupBc_parsed.length - 1];
                CallTree_refreshCategories(tree.fw);
            }

            function Tree__createFilteredTree(tree, val) {
                var goodTags = {}, allTags = tags.t;
                val = new RegExp(escapeRegExp(val), 'i');
                var numberOfResults = 0;

                for (var j in allTags) {
                    var t = allTags[j];
                    if (!val.test(t[0]))
                        continue;
                    goodTags[j] = 1;
                }

                function nodeMatches(node) {
                    if (goodTags[node[M_ID]]) {
                        numberOfResults++;
                        return true;
                    }

                    var tags = node[M_TAGS], newTags;
                    if (tags && tags.length > 0) {
                        for (var i = 0, len = tags.length; i < len; i++) {
                            var tag = tags[i];
                            if (goodTags[tag[P_ID]] || val.test(tag[P_VALUE])) {
                                numberOfResults++;
                                return true;
                            }
                        }
                    }
                }

                function copyTree(node) {
                    var dst = Tree__cloneNode(
                        node[M_ID],
                        node[M_DURATION],
                        node[M_SELF_DURATION],
                        node[M_SUSPENSION],
                        node[M_SELF_SUSPENSION],
                        node[M_EXECUTIONS],
                        node[M_CHILD_EXECUTIONS]
                        );
                    var tags = node[M_TAGS];
                    if (nodeMatches(node))
                        dst[M_PREV_SELF_DURATION] = -2;
                    if (tags)
                        dst[M_TAGS] = tags;
                    var tmp = node[M_COMPUTATOR];
                    var children = node[M_CHILDREN];
                    if (tmp) {
                        dst[M_COMPUTATOR] = tmp;
                        dst[M_NOT_COMPUTED] = 1;
                    } else if (children) {
                        var dstChildren = dst[M_CHILDREN] = [];
                        var hasInterestingNode = false;
                        for (var i = 0, len = children.length; i < len; i++) {
                            var child = dstChildren[i] = copyTree(children[i]);
                            if (!hasInterestingNode && child[M_PREV_SELF_DURATION])
                                hasInterestingNode = true;
                        }
                        if (hasInterestingNode && !dst[M_PREV_SELF_DURATION])
                            dst[M_PREV_SELF_DURATION] = -1;
                    }
                    tmp = node[M_CATEGORY];
                    if (tmp)
                        dst[M_CATEGORY] = tmp;
                    return dst;
                }

                function markMatchedNodes(node) {
                    var matchState = nodeMatches(node) ? -2 : 0;
                    var children = node[M_CHILDREN];
                    if (children) {
                        if (node[M_COMPUTATOR]) {
                            node[M_NOT_COMPUTED] = 1;
                            node[M_CHILDREN] = [];
                        } else {
                            var hasInterestingNode = matchState != 0;
                            for (var i = 0, len = children.length; i < len; i++) {
                                hasInterestingNode |= markMatchedNodes(children[i]);
                            }
                            if (hasInterestingNode && matchState == 0)
                                matchState = -1;
                        }
                    }
                    node[M_PREV_SELF_DURATION] = matchState
                    return matchState != 0;
                }

                var root;
                if (!tree[CallTree_FILTERED_TREE])
                    root = copyTree(tree.fw);
                else {
                    root = tree[CallTree_FILTERED_TREE].fw;
                    markMatchedNodes(root);
                }

                sortNode(root, tree.rv ? orderCallsBySelfDuration : orderCallsByDuration);

                var filteredTree = new CallTree(null, root, tree.ty, tree.rv);
                var srcTree = tree.srcTree;
                if (srcTree)
                    filteredTree.srcTree = srcTree;
                filteredTree[CallTree_FILTERED_RESULTS] = numberOfResults;
                return filteredTree;
            }

            function Tree__findSimilarNode(tree, srcNode) {
                var srcId = srcNode[M_ID];
                var srcSignature = getMethodSignature(srcNode);

                function findNode(node) {
                    var id = node[M_ID];
                    if (id == srcId && srcSignature == getMethodSignature(node)) return [];

                    var t = node[M_CHILDREN], i,  dst, len;
                    if (t && t.length > 0)
                        for (i = 0,len = t.length; i < len; i++) {
                            var path = findNode(t[i]);
                            if (path) {
                                path.push(i);
                                return path;
                            }
                        }
                    return undefined;
                }

                return findNode(tree.fw);
            }

            //Gantt Chart

            var G_TIME = 0;
            var G_DURATION = 1;
            var G_ROWID = 2;
            var G_FOLDER_ID = 3;
            var G_METHOD = 4;
            var G_EMIT = 5;

            var dataView;
            var timeRange;
            var colors = [];
            var grid, gridOptions;
            var timeline, timelineItemRowids = [], timelineRowId2Idx = {};
            var loaded_timerange;
            var filters_timerange;
            var filters_timeline_range;
            var folderContents = [];
            var etcGmt = new RegExp('^Etc/GMT.');
            var filters_timerange_is_pending = false;
            var timezone = profiler_settings.timezone;
            var timezone_pending = timezone;

            CT.timeRange = function(min, max) {
                return timeRange = {min:min, max:max};
            };

            links.Timeline.StepDate.prototype.getLabelMajor = function(options, date) {
                if (date == undefined) {
                    date = this.current;
                }
                var v = Date__toUTC(date);
                date = v.date;

                switch (this.scale) {
                    case links.Timeline.StepDate.SCALE.MILLISECOND:
                        return lpad2(date.getUTCHours())+ ":" +
                            lpad2(date.getUTCMinutes()) + ":" +
                            lpad2(date.getUTCSeconds()) + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.SECOND:
                        return  date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            lpad2(date.getUTCHours()) + ":" +
                            lpad2(date.getUTCMinutes()) + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.MINUTE:
                        return  options.DAYS[date.getUTCDay()] + " " +
                            date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear() + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.HOUR:
                        return  options.DAYS[date.getUTCDay()] + " " +
                            date.getUTCDate() + " " +
                            options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear() + " " + v.shortLabel;
                    case links.Timeline.StepDate.SCALE.WEEKDAY:
                    case links.Timeline.StepDate.SCALE.DAY:
                        return  options.MONTHS[date.getUTCMonth()] + " " +
                            date.getUTCFullYear();
                    case links.Timeline.StepDate.SCALE.MONTH:
                        return String(date.getUTCFullYear());
                    default:
                        return "";
                }
            };

            links.Timeline.StepDate.prototype.getLabelMinor = function(options, date) {
                if (date == undefined) {
                    date = this.current;
                }
                var v = Date__toUTC(date);
                date = v.date;

                switch (this.scale) {
                    case links.Timeline.StepDate.SCALE.MILLISECOND:  return String(date.getUTCMilliseconds());
                    case links.Timeline.StepDate.SCALE.SECOND:       return String(date.getUTCSeconds());
                    case links.Timeline.StepDate.SCALE.MINUTE:
                        return lpad2(date.getUTCHours()) + ":" + lpad2(date.getUTCMinutes());
                    case links.Timeline.StepDate.SCALE.HOUR:
                        return lpad2(date.getUTCHours()) + ":" + lpad2(date.getUTCMinutes());
                    case links.Timeline.StepDate.SCALE.WEEKDAY:      return options.DAYS_SHORT[date.getUTCDay()] + ' ' + date.getUTCDate();
                    case links.Timeline.StepDate.SCALE.DAY:          return String(date.getUTCDate());
                    case links.Timeline.StepDate.SCALE.MONTH:        return options.MONTHS_SHORT[date.getUTCMonth()];   // month is zero based
                    case links.Timeline.StepDate.SCALE.YEAR:         return String(date.getUTCFullYear());
                    default:                                         return "";
                }
            };

            function updateTimeline() {
                var order = dataView.rows, MAX_ROWS = 1000;
                if (order.length > MAX_ROWS) { // get top MAX_ROWS
                    order = order.slice(0, order.length); // make copy
                    if (order.length > MAX_ROWS) {
                        order = order.slice(0, MAX_ROWS);
                    }
                }

                var tl = [];
                timelineItemRowids = [];
                timelineRowId2Idx = {};
                for (var i = 0; i < order.length; i++) {
                    var o = order[i];
                    timelineItemRowids[i] = o[G_ROWID];
                    timelineRowId2Idx[o[G_ROWID]] = i;
                    var title = format_duration_for_timeline(o, 10000);
                    var start = o[G_TIME];
                    var duration = o[G_DURATION];
                    var item;
                    if (duration > 0)
                        item = {
                            start: start, end: start + duration, content: title
                        };
                    else
                        item = {
                            start: start, content: title
                        };

                    tl[tl.length] = item;
                }
                var intervalMin, intervalMax;
                if (filters_timerange) {
                    intervalMin = filters_timerange.min;
                    intervalMax = filters_timerange.max;
                } else {
                    intervalMin = new Date().getTime() - 1000 * 3600 * 24;
                    intervalMax = new Date().getTime() + 1000 * 3600 * 24
                }
                var diff = intervalMax - intervalMin;
                if (diff) {
                    intervalMax += diff*0.05;
                    intervalMin -= diff*0.05;
                }
                var options = {
                    width: '100%'
                    , height: '100%'
                    , eventMarginAxis: 7
                    , eventMargin: 7
                    , cluster: true
                    , axisOnTop: true
                    , animate: false
                    , animateZoom: false
                    , showNavigation: true
                    , min: intervalMin
                    , max: intervalMax
                    , customStackOrder: function (a, b) {
                        // Sort earliest to finish first
                        if ((a instanceof links.Timeline.ItemRange) && !(b instanceof links.Timeline.ItemRange)) {
                            return -1;
                        }

                        if (!(a instanceof links.Timeline.ItemRange) &&
                            (b instanceof links.Timeline.ItemRange)) {
                            return 1;
                        }
                        if (a.right != b.right)
                            return a.right - b.right;
                        return a.left - b.left;
                    }
                };
                timeline.draw(tl, options);
                var range = filters_timeline_range;
                if (!range && (filters_timerange || loaded_timerange)) {
                    range = filters_timerange || loaded_timerange;
                    var rdiff = (range.max - range.min)*0.05;
                    range = {
                        min: range.min - rdiff
                        , max: range.max + rdiff
                    }
                }
                if (!range)
                    range = timeRange;
                timeline.setVisibleChartRange(range.min - 1000, range.max + 1000);
            }

            function format_row_css(row) {
                var params = row[18];
                if (!params)
                    return '';
                if (TAGS_CALL_ACTIVE_STR in params)
                    return 'inf';
                var tags = folderContents[row[7]].tags;
                if (tags.r['call.red'] in params)
                    return 'err';
                return '';
            }

            function Date__toUTC(date) {
                var tzName = filters_timerange_is_pending ? timezone_pending : timezone;
                var tz = moment.tz.zone(tzName);
                var value = date.getTime();
                var lab = Timezone__label(tzName, value);
                value -= lab.offs * 60000;
                return {date: new Date(value), shortLabel: lab.shortLabel};
            }

            function Timezone__label(tz, now) {
                var zone = moment.tz.zone(tz);
                var uiTz = Timezone__humanName(tz);
                var offs = zone.utcOffset(now);
                var sign = offs < 0 ? '+' : '-';
                var absOffs = Math.abs(offs);
                var mm = absOffs % 60;
                var shortLabel = sign + lpad2(Math.round((absOffs - mm) / 60)) + ':' + lpad2(mm);
                return {
                    shortLabel: shortLabel
                    , label: shortLabel !== uiTz ? (shortLabel + ' ' + uiTz) : shortLabel
                    , offs: offs
                }
            }

            function Timezone__humanName(tz) {
                var uiTz = tz;
                if (etcGmt.test(tz)) {
                    uiTz = uiTz.substring(8);
                    if (uiTz.length === 1) {
                        uiTz = '0' + uiTz;
                    }
                    if (tz.charAt(7) === '-')
                        uiTz = '+' + uiTz;
                    else
                        uiTz = '-' + uiTz;
                    uiTz += ':00';
                }
                return uiTz;
            }

            function format_duration_for_timeline(dataContext, redBoundary) {
                var value = dataContext[G_DURATION];
                var formatted = Duration__formatTime(value);
                if (value > redBoundary)
                    formatted = '<ins>' + formatted + '</ins>';

                var folderId = dataContext[G_FOLDER_ID];
                var folderName = folderContents[folderId].name;
                var emit = dataContext[G_EMIT];
                var res = '';
                if(emit != 0) {
                    res = '<div style="background-color:' + getRandomColor(emit) + '; border-radius: 5px">';
                }
                res += '<a target="_blank" href="tree.html#params-trim-size=15000&f[_' + folderId + ']=' + folderName + '&i=' + dataContext[G_ROWID] +
                '&s=' + dataContext[G_TIME] + '&e=' + (dataContext[G_TIME] + dataContext[G_DURATION]) + '" title="';
                var title = format_title(dataContext);
                res += ' ' + title.replace(/["\n]/g);
                res += '">' + formatted + ' ' + title + '</a>';
                if(emit != 0) {
                    res += '</div>';
                }
                return res;
            }

            function getRandomColor(emit) {
                if(colors[emit]) {
                    return colors[emit];
                }
                return colors[emit] = getRandom();
            }

            function getRandom() {
                var letters = 'BCDEF'.split('');
                var color = '#';
                for (var i = 0; i < 6; i++ ) {
                    color += letters[Math.floor(Math.random() * letters.length)];
                }
                return color;
            }

            function resizeCallList(resizeTimeline) {
                var $timeline = $('#timeline');
                if (resizeTimeline) {
                    var $filter_config = $('#calls-list-configuration');
                    var timelineTop = $filter_config.offset().top + $filter_config.outerHeight() + 5;
                    $timeline.css({top: timelineTop, height: timelineBottom - timelineTop});
                }
                timeline.render();
            }

            function format_title(dataContext) {
                var value = dataContext[G_METHOD];
                if (value == null || value === "")
                    return "";
                var p = tags.t[dataContext[G_METHOD]];
                var r = p[tags.r['java.thread']];
                r += ' ';
                var url = p[tags.r['reactor.web.info']];
                var sql = p[tags.r['cassandra.sql']];
                if (url) {
                    r = r + url;
                } else if(sql) {
                    r = r + sql;
                } else {
                    r = r + tags.toShortHTML(value);
                }
                return r;
            }
        })();
})();
