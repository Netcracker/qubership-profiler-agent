import jQuery from 'jquery';

jQuery.browser = {
    msie: false
};
if (window.navigator.userAgent.toLowerCase().indexOf("compatible") < 0 &&
    window.navigator.userAgent.toLowerCase().match(/(mozilla)(?:.*? rv:([\w.]+)|)/)) {
    jQuery.browser.mozilla = true;
}

export default jQuery;
