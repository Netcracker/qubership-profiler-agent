const ESC_REGEX = {
    AMP: /&/g,
    LT: /</g,
    GT: />/g,
    SPECIALS: /[-[\]{}()*+?.,\\^$|#\s]/g
}

export function escapeHTML(s) {
    if (!s) return s;
    return s.replace(ESC_REGEX.AMP, '&amp;').replace(ESC_REGEX.LT, '&lt;').replace(ESC_REGEX.GT, '&gt;');
};

export function escapeRegExp(text) {
    return text.replace(ESC_REGEX.SPECIALS, "\\$&");
};
