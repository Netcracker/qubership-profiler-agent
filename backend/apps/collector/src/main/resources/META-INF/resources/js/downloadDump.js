$('#downloadDumpButton').click(function(event){
    let searchConditions = "";
    try {
        searchConditions = callPodFilter.getConditionsString();
    } catch (e) {
        if(console) {
            console.log("error parsing search conditions " + e);
        }
        return;
    }

    let timerange = $.bbq.getState('timerange');
    let url = "exportDump?searchConditions=" + encodeURIComponent(searchConditions)
        + "&" + encodeURIComponent("timerange[min]")+ "=" + encodeURIComponent(timerange.min)
        + "&" + encodeURIComponent("timerange[max]") + "=" + encodeURIComponent(timerange.max)
    ;
    window.open(url, 'blank')
});

function ExportToExcel__download() {
    let searchConditions = "";
    try {
        searchConditions = callPodFilter.getConditionsString();
    } catch (e) {
        if(console) {
            console.log("error parsing search conditions " + e);
        }
        return;
    }

    let timerange = $.bbq.getState('timerange');
    let duration = $.bbq.getState('duration');
    if (!duration) {
        duration = {};
    }
    duration.min = $('#export-to-excel-duration-min').val();
    let type = $('#export-to-excel-dialog input:radio[name=export-to-excel-type]:checked').val();
    let minDigitsInId = $('#export-to-excel-min-digits-in-id').val();
    let urlReplacePatterns = $('#export-to-excel-url-replace-patterns').val();
    let disableDefaultUrlReplacePatterns = $("#export-to-excel-disable-default-replace-patterns")[0].checked;
    let nodes = $('#export-to-excel-nodes').val();
    let url = "exportExcel?searchConditions=" + encodeURIComponent(searchConditions)
        + "&" + encodeURIComponent("timerange[min]")+ "=" + encodeURIComponent(timerange.min)
        + "&" + encodeURIComponent("timerange[max]") + "=" + encodeURIComponent(timerange.max)
        + (duration.min ? "&" + encodeURIComponent("duration[min]") + "=" + encodeURIComponent(duration.min) : "")
        + (duration.max ? "&" + encodeURIComponent("duration[max]") + "=" + encodeURIComponent(duration.max) : "")
        + "&type=" + encodeURIComponent(type) + "&minDigitsInId=" + encodeURIComponent(minDigitsInId)
        + "&urlReplacePatterns=" + encodeURIComponent(urlReplacePatterns) + "&disableDefaultUrlReplacePatterns=" + encodeURIComponent(disableDefaultUrlReplacePatterns)
        + "&nodes=" + encodeURIComponent(nodes);
    window.open(url, 'blank')
};

var ExportToExcel$initDone;

function ExportToExcel__open() {
    if (!ExportToExcel$initDone) {
        let duration = $.bbq.getState('duration');
        if (!duration) {
            duration = {min: 500};;
        }
        $('#export-to-excel-duration-min').val(duration.min);
        $('#export-to-excel-all').click(function(event){
            $('.export-to-excel-aggregate-params').css('display', 'none');
        });
        $('#export-to-excel-aggregate').click(function(event){
            $('.export-to-excel-aggregate-params').css('display', 'table-row');
        });
        $('#export-to-excel-dialog').dialog({
                    title: 'Export to excel',
                    width: 515,
                    height: 320,
                    resizable: true,
                    buttons: {
                        Download: ExportToExcel__download,
                        Cancel: function() {
                            $(this).dialog('close');
                        }
                    }
                });
        ExportToExcel$initDone = true;
        ThreadDumps__updateFileSize();
    }

    $('#export-to-excel-dialog').dialog('open');
    return false;
};

$('#downloadExcelbutton').click(ExportToExcel__open);
