var ESCInjected = {
    //import moment from 'moment'
    //not used in classic UI
    moment: moment,
    //import jqDeParam from 'jquery-deparam'
    jqDeParam: $.deparam,
    //import jqParam from 'jquery-param'
    jqParam: $.param,
    //import Cookies from 'js-cookie'
    //Cookies.set('profiler_settings', jqParam(profiler_settings), {domain: settings_cookie_domain, expires: 30 * 6});
    cookieSet: $.cookie,
    //Cookies.get('profiler_settings');
    cookiesGet: $.cookie,
    //browser['msie'] || browser['mozilla']
    //import browser from 'jquery.browser'
    browser: $.browser
}
