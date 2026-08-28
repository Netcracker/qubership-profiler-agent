import '../styles/activePodPopup.css';
import { default as jQuery, default as $ } from 'jquery';
import 'jquery-bbq';
import { default as jstz } from 'jstz';

class ActivePodPopup {

    static get ENDPOINT(){
        return 'fetchActivePods';
    }

    last2MinOrRangeOption = "last2min";

    presentLoading(){
        let _this = this;
        if(!this.screenBlocker){
            this.screenBlocker = $('<div class="activePodScreenblocker"></div>');
            $(document.body).append(this.screenBlocker);
            this.screenBlocker.click(function(){
                _this.hidePopup();
            });
        }

        if(!this.popup){
            this.popup = $('<div class="activePodPopup">' +
                "<div class='Last2MinOrRange' id='Last2MinOrRange'>" +
                "<form name='Last2MinOrRange'>" +
                "<div class='form_radio_btn'>" +
                "<input type='radio' value='last2min' name='last2MinOrRangeOption' id='last2MinOrRangeOption_last2min'/><label for='last2MinOrRangeOption_last2min'>Last 2 minutes</label>" +
                "</div><div class='form_radio_btn'>" +
                "<input type='radio' value='selectedRange' name='last2MinOrRangeOption' id='last2MinOrRangeOption_selectedRange'/><label for='last2MinOrRangeOption_selectedRange'>Specified Time Range</label>" +
                "</div>" +
                "</div>" +
                "<div id='listOfActivePods' class='listOfActivePods'></div>" +
                '</div>');
            $(document.body).append(this.popup);
            this.listOfActivePods = $('#listOfActivePods');

            if(this.last2MinOrRangeOption){
                $("[name=last2MinOrRangeOption]").val([this.last2MinOrRangeOption]);
            }
            let _this = this;
            $('#Last2MinOrRange').change(function(){
                if(document.forms['Last2MinOrRange'] && document.forms['Last2MinOrRange'].last2MinOrRangeOption.value){
                    _this.last2MinOrRangeOption =  document.forms['Last2MinOrRange'].last2MinOrRangeOption.value;
                }
                _this.showPopup();
            });
        }
        this.popup.addClass("loading");
        this.listOfActivePods.empty();
    }

    getCurrentTimezone(){
        if(this.currentTimezone){
            return this.currentTimezone;
        }

        let profilerSettings = $.cookie('profiler_settings');
        let timezone = jstz.determine().name();
        if(profilerSettings) {
            profilerSettings = $.deparam(profilerSettings);
            if(profilerSettings.timezone) {
                timezone = profilerSettings.timezone;
            }
        }
        return this.currentTimezone = moment.tz.zone(timezone);
    }

    formatLastActive(row, cell, value, columnDef) {
        let timezoneOffset = this.getCurrentTimezone().utcOffset(new Date(value));
        let properDate = new Date(value - timezoneOffset * 60000);
        return '' + properDate.getUTCFullYear() + '/' +
            (properDate.getUTCMonth()+1).toString().padStart(2, '0') + '/' +
            properDate.getUTCDate().toString().padStart(2, '0') + ' ' +
            properDate.getUTCHours().toString().padStart(2, '0') + '-' +
            properDate.getUTCMinutes().toString().padStart(2, '0') + '-' +
            properDate.getUTCSeconds().toString().padStart(2, '0');
        // return '' + new Date(value);
    }

    formatActiveFor(row, cell, value, columnDef) {
        return Math.round(value / 1000) + ' sec';
    }

    formatDataAccumulated(row, cell, value, columnDef) {
        return Math.round(value / 1000) + ' KB';
    }

    formatCurrentBitrate(row, cell, value, columnDef) {
        return value + ' KB/s';
    }

    formatDownloadGC(row, cell, value, columnDef) {
        return "<a class='DownloadGC' style='text-decoration: underline; cursor: pointer; color: brown;'>GC</a>"
    }

    presentData(data){
        this.presentLoading();

        let gridHost = $('<div class="activePodPopupMain" id="activePodPopupMain"></div>');
        this.listOfActivePods.append(gridHost);
        this.popup.removeClass('loading');

        let gridColumns = [
            {id:"podName", name:"Pod Name", field:"podName", behavior:"select", resizable:true, sortable:true},
            {id:"serviceName", name:"Service Name", field:"serviceName", behavior:"select", resizable:true, sortable:true},
            {id:"namespace", name:"Namespace Name", field:"namespace", behavior:"select", resizable:true, sortable:true},
            {id:"lastActive", name:"Last Active", field:"lastSampleMillis", behavior:"select", width:135, resizable:true, sortable:true, formatter: this.formatLastActive.bind(this)},
            {id:"activeFor", name:"Active For", field:"acriveForMillis", behavior:"select", width:135, resizable:true, sortable:true, formatter: this.formatActiveFor.bind(this)},
            {id:"dataAccumulated", name:"Data Accumulated", field:"dataAtEnd", behavior:"select", width:135, resizable:true, sortable:true, formatter: this.formatDataAccumulated.bind(this)},
            {id:"currentBitrate", name:"Current Bitrate", field:"currentBitrate", behavior:"select", width:135, resizable:true, sortable:true, formatter: this.formatCurrentBitrate.bind(this)},
            {id:"downloadGC", name:"GC", width:135, resizable:true, sortable:true, formatter: this.formatDownloadGC.bind(this)}
        ];

        let gridOptions = {
            enableCellNavigation: true,
            forceFitColumns: true,
            secondaryHeaderRowHeight: 25,
            enableTextSelectionOnCells: true,
            // rowCssClasses: format_row_css,
            rowHeight: 30
        };

        if(typeof(data) === "string"){
            data = JSON.parse(data);
        }

        for(let row of data) {
            row.acriveForMillis = row.lastSampleMillis - row.activeSinceMillis;
        }

        this.grid = new Slick.Grid(gridHost, data, gridColumns, gridOptions);
        var _this = this;
        this.grid.onClick = function (e, row) {
            //check if your button was clicked
            if ($(e.target).hasClass("DownloadGC")) {
                var item = data[row];
                let timerange = $.bbq.getState('timerange');
                if(_this.last2MinOrRangeOption === "last2min") {
                    timerange.max = Date.now();
                    timerange.min = timerange.max - 120000;
                }

                var url =  "downloadStream?podName=" + encodeURIComponent(item.podName) + "&streamName=gc"
                    + "&" + encodeURIComponent("timerange[min]")+ "=" + encodeURIComponent(timerange.min)
                    + "&" + encodeURIComponent("timerange[max]") + "=" + encodeURIComponent(timerange.max)
                window.open(url, 'blank')
            }
        };
        this.grid.render();

    }

    hidePopup(){
        this.popup.detach();
        this.screenBlocker.detach();

        this.popup = null;
        this.screenBlocker = null;
        this.grid = null;

        window.activePodPopup = null;
    }

    constructor(searchConditions){
        this.searchConditions = searchConditions;
    }

    showPopup(){
        let _this = this;
        let timerange = $.bbq.getState('timerange');
        $.ajax({
            type: "POST",
            data: {
                searchConditions: this.searchConditions,
                "timerange[min]": timerange.min,
                "timerange[max]": timerange.max,
                "last2MinOrRange": this.last2MinOrRangeOption
            },
            url: ActivePodPopup.ENDPOINT,
            success:function(response){
                _this.dataReceived(response);
            },
            error: function(xhr, ajaxOptions, thrownError){
                _this.errorReceived(xhr, ajaxOptions, thrownError);
            }
        });
        this.presentLoading();
    }

    dataReceived(response){
        this.presentData(response);
    }

    errorReceived(xhr, ajaxOptions, thrownError){
        this.hidePopup();
        app.notify.notify('create', 'jqn-error', {title:'Failed to fetch list of active PODs',text:xhr}, {expires:false, custom: true});
    }
}

$('#showActivePODsbutton').click(function(event){
    let searchConditions = "";
    try {
        searchConditions = callPodFilter.getConditionsString();
    } catch (e) {
        if(console) {
            console.log("error parsing search conditions " + e);
        }
    }
    window.activePodPopup = new ActivePodPopup(searchConditions);
    window.activePodPopup.showPopup();
});

$(document).keyup(function(event){
    if(!window.activePodPopup) {
        return;
    }
    if(event.which === 27){
        window.activePodPopup.hidePopup();
    }
});
