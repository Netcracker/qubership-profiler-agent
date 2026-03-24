var ESCUtils = window.ESCUtils || new function ()
{
    var ESC_REGEX = {
        AMP: /&/g,
        LT: /</g,
        GT: />/g,
        SPECIALS: /[-[\]{}()*+?.,\\^$|#\s]/g
    }
    this.escapeHTML = function (s) {
        if (!s) return s;
        return s.replace(ESC_REGEX.AMP, '&amp;').replace(ESC_REGEX.LT, '&lt;').replace(ESC_REGEX.GT, '&gt;');

    }
    this.escapeRegExp = function (text) {
        return text.replace(ESC_REGEX.SPECIALS, "\\$&");
    }
}

if(typeof module === 'object' && typeof module.exports === 'object') {
    module.exports['ESCUtils'] = ESCUtils;
}
window.ESCUtils = ESCUtils;
