try {
    if (!window.XMLHttpRequest) // fix for IE6
        document.execCommand("BackgroundImageCache", false, true);
} catch (e) {
}
var showUpgradeBorwserWarning;
if (!$.cookie('oldbrowser')){
    showUpgradeBorwserWarning = function() {
        $(document.body).append($('<iframe id="oldbrowser" class="whatbrowser" src="http://www.whatbrowser.org"/>'));
        if (showUpgradeBorwserWarning.done) return;
        showUpgradeBorwserWarning.done = true;
        $.cookie('oldbrowser', '1', {expires: 7});
        var horizontalPadding = 30;
        var verticalPadding = 30;
        var prevOverflow = document.body.style.overflow;
        document.body.style.overflow = 'scroll';
        document.body.parentElement.style.overflow = 'scroll';
        $('#oldbrowser').css('position', 'relative').css('left', 0).dialog({
            title: 'Please, consider better browser',
            autoOpen: true,
            width: 830,
            height: 725,
            modal: true,
            resizable: true,
            autoResize: false,
            overlay: {
                opacity: 0.5,
                background: "black"
            },
            close: function(){
                document.body.style.overflow = prevOverflow;
                document.body.parentElement.style.overflow = prevOverflow;
            }
        }).width(830 - horizontalPadding).height(725 - verticalPadding);
    };

    $(function() {
        setTimeout(function () {
            app.notify.notify('create', 'jqn-notice',
                {title: 'Consider faster browser', text: 'You are using Internet Explorer. Profiler works much faster when using newer browser (e.g. Chrome, Opera or FireFox). <a href="#" onclick="showUpgradeBorwserWarning(); return false;">Explain more</a>'},
                {expires: 30000, custom: true}
            );
        }, 5000);
    });
}
