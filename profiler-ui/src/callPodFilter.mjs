import '../styles/callPodFilter.css';
import { default as jQuery, default as $ } from 'jquery';

(function($) {
    $.fn.getCursorPosition = function() {
        var input = this.get(0);
        if (!input) return; // No (input) element found
        if ('selectionStart' in input) {
            // Standard-compliant browsers
            return input.selectionStart;
        } else if (document.selection) {
            // IE
            input.focus();
            var sel = document.selection.createRange();
            var selLen = document.selection.createRange().text.length;
            sel.moveStart('character', -input.value.length);
            return sel.text.length - selLen;
        }
    };
    $.fn.setCursorPosition = function(caretPos) {
        var elem = this.get(0);

        if(elem != null) {
            if(elem.createTextRange) {
                var range = elem.createTextRange();
                range.move('character', caretPos);
                range.select();
            }
            else {
                if(elem.selectionStart) {
                    elem.focus();
                    elem.setSelectionRange(caretPos, caretPos);
                }
                else
                    elem.focus();
            }
        }
    }
})(jQuery);

(function($) {
    $.fn.selectRange = function(start, end) {
        return this.each(function() {
            if (this.setSelectionRange) {
                this.focus();
                this.setSelectionRange(start, end);
            } else if (this.createTextRange) {
                var range = this.createTextRange();
                range.collapse(true);
                range.moveEnd('character', end);
                range.moveStart('character', start);
                range.select();
            }
        });
    };
})(jQuery);

class SearchParsingError extends Error{
    // partialResult = null;

    // constructor(message){
    //     super(message);
    //     this.partialResult = null;
    //     Error.captureStackTrace(this, SearchParsingError);
    // }

    constructor(message, partialResult){
        super(message);
        this.partialResult = partialResult;
        if(Error.captureStackTrace) { //FF does not support this one
            Error.captureStackTrace(this, SearchParsingError);
        }
    }
}

class DateCondition {
    static get FUNCTIONS(){
        return [
            'now',
            'endOfDay',
            'endOfMonth',
            'endOfWeek',
            'endOfYear',
            'startOfDay',
            'startOfMonth',
            'startOfWeek',
            'startOfYear'
        ]
    }

    static get MASKS(){
        return [
        'yyyy/MM/dd HH:mm',
        'yyyy-MM-dd HH:mm',
        'yyyy/MM/dd',
        'yyyy-MM-dd'
        ];
    }

    static get TIME_UNITS(){
        return [
            'y',
            'M',
            'w',
            'd',
            'H',
            'm'
        ]
    }

    constructor(fieldName){
        this.fieldName = fieldName;
    }

    getFieldName(){
        return this.fieldName;
    }

    rValueAdvisor(typedValue, suggestionsReadyCallback) {
        let suggestions = [];
        for(let func of DateCondition.FUNCTIONS){
            if(func.startsWith(typedValue)){
                suggestions.push(func + '(');
            }
            // /^now\(\-?[0-9]+$/
            let startRegexp = new RegExp('^' + func + "\\(\\-?[0-9]+$");
            if(startRegexp.test(typedValue)){
                for(let unit of DateCondition.TIME_UNITS){
                    suggestions.push(typedValue + unit + ')');
                }
            }
        }
        suggestionsReadyCallback(suggestions);
    }

    rValueValidator(typedValue) {

    }
}

class ServerRequestCondition {

    static get MAPPINGS(){
        return [
            {fieldName: 'pod_name', func: function(){return window.availableServices.podNames}},
            {fieldName: 'service_name', func: function(){return window.availableServices.serviceNames}},
            {fieldName: 'namespace', func: function(){return window.availableServices.namespaces}},
            {fieldName: 'pod_info', func: function(){return window.availableServices.serviceNames}},
            {fieldName: 'rc_info', func: function(){return window.availableServices.serviceNames}},
            {fieldName: 'dc_info', func: function(){return window.availableServices.serviceNames}}
        ]
    }

    constructor(fieldName){
        this.fieldName = fieldName;
    }

    getFieldName(){
        return this.fieldName;
    }

    rValueAdvisor (typedValue, suggestionsReadyCallback) {
        let result = [];
        for(let value of this.getMappingFunction()()){
            if(value.toLowerCase().indexOf(typedValue.toLowerCase()) >= 0){
                result.push(value);
            }
        }
        suggestionsReadyCallback(result);
    }

    getMappingFunction(){
        for(let mapping of ServerRequestCondition.MAPPINGS){
            if(this.fieldName === mapping.fieldName){
                return mapping.func;
            }
        }
    }

    rValueValidator (typedValue) {
        return true;
    }
}

var callPodFilter = $('#callPodFilter');
callPodFilter.extend({
    searchFields: [
        new ServerRequestCondition('pod_name'),
        new ServerRequestCondition('service_name'),
        new ServerRequestCondition('namespace'),
        new ServerRequestCondition('pod_info'),
        new ServerRequestCondition('rc_info'),
        new ServerRequestCondition('dc_info'),
        new DateCondition('date')
    ],
    //words that do appear in the middle of other words and should be separate words if they do
    specialWords: [
        "<=",
        "!=",
        ">=",
        "=",
        ">",
        "<"
    ],

    inputField: null,
    suggestionBox: null,
    suggestionsPresented: false,
    selectedSuggestionIndex: -1,
    applySuggestionsCallback: null,

    compilePattern: function(){

    },

    isSpecialWord: function(word){
        for(let specialWord of this.specialWords){
            if(word === specialWord){
                return true;
            }
        }
        return false;
    },

    splitBySpecialWords: function(word){
        let splits = [word];
        for(let specialWord of this.specialWords){
            let newSplits = [];
            for(let oldSplit of splits){
                if(this.isSpecialWord(oldSplit)){
                    newSplits.push(oldSplit);
                    continue;
                }
                let freshSplits = oldSplit.split(specialWord);
                for(let i=0; i<freshSplits.length; i++){
                    //if special word was the last word, split method will return an empty word at the end
                    if(freshSplits[i].length > 0){
                        newSplits.push(freshSplits[i]);
                    }
                    if(i+1 !== freshSplits.length){
                        newSplits.push(specialWord);
                    }
                }
            }
            splits = newSplits;
        }
        return splits;
    },

    pushWord: function(caret, escapedByQuote){
        if(caret.currentWord.length > 0){
            let bySpecialWords;
            if(escapedByQuote){
                bySpecialWords = [caret.currentWord];
            } else {
                bySpecialWords = this.splitBySpecialWords(caret.currentWord);
            }
            let currentLength = 0;
            for(let split of bySpecialWords){
                let beingTyped = caret.beingTyped - currentLength;
                //some of the following words is being typed
                if(beingTyped > split.length){
                    beingTyped = -1;
                }
                beingTyped = Math.max(beingTyped, -1);
                let toPush = {
                    word: split,
                    beingTyped: beingTyped,
                    escapedByQuote: escapedByQuote
                };
                caret.words.push(toPush);
                currentLength += split.length;
            }
        }
        caret.currentWord = '';
        caret.beingTyped = -1;
    },

    appendSymbol: function(caret, symbol){
        caret.currentWord = caret.currentWord + '' + symbol;
        if(caret.index + 1 === caret.cursorPosition){
            caret.beingTyped = caret.currentWord.length;
        }
    },

    extractWords(){
        var caret = {
            index: 0,
            words: [],
            escaped: false,
            escapedByQuote: false,
            currentWord: '',
            cursorPosition: this.getCursorPosition(),
            beingTyped: -1
        };

        var input = this.val();

        if(input ==='' && window.isFromDump ==="true"){
            input = 'pod_name like %';
        }

        // var index=0;
        // var words = [];

        // var escaped = false;
        // var escapedByQuote = false;
        // var currentWord = '';
        // var cursorPosition = this.getCursorPosition();

        var subWordBeingTyped = '';

        for(;caret.index <  input.length; caret.index++){
            if(caret.escaped) {
                this.appendSymbol(caret, input[caret.index]);
                caret.escaped = false;
                continue;
            }
            if(caret.escapedByQuote && '\"' !== input[caret.index] && '\\' !== input[caret.index]){
                this.appendSymbol(caret, input[caret.index]);
                continue;
            }
            switch(input[caret.index]){
                case '\\':
                    caret.escaped = true;
                    break;
                case '\t':
                case ' ':
                case '\n':
                    this.pushWord(caret);
                    break;
                case ',':
                case '(':
                case ')':
                    this.pushWord(caret);
                    this.appendSymbol(caret, input[caret.index])
                    this.pushWord(caret);
                    break;
                case '\"':
                    if(caret.escapedByQuote){
                        this.pushWord(caret, true);
                        caret.escapedByQuote = false;
                    } else {
                        this.pushWord(caret, false); //in case of something like service_name="some_name"
                        caret.escapedByQuote = true;
                    }
                    break;
                default:
                    this.appendSymbol(caret, input[caret.index]);

            }
        }
        //push the last word
        this.pushWord(caret);
        return caret.words;
    },

    validateCapacity: function(caret, message, partialResult){
        if(caret.words.length <= caret.index){
            throw new SearchParsingError(message, partialResult);
        }
    },

    nextWordIs: function(caret, word){
        if(caret.words.length <= caret.index){
            return false;
        }
        return word === caret.words[caret.index].word;
    },

    concatenateUntil: function(caret, resultWord, stopWord){
        let nextWord;
        let bracketIndex = 0;
        do {
            nextWord = caret.words[caret.index++];
            if(nextWord.word === '('){
                bracketIndex++;
            } else if(nextWord.word === ')'){
                bracketIndex--;
            }
            if(nextWord.beingTyped >= 0){
                resultWord.beingTyped = resultWord.word.length + nextWord.beingTyped;
            }
            resultWord.word = resultWord.word + nextWord.word;
        } while(caret.index < caret.words.length && nextWord.word !== stopWord && bracketIndex !== 0)
    },

    nextCondition: function(caret){
        let result = {
            lValue: null,
            comparator: null,
            rValues: []
        };
        result.lValue = caret.words[caret.index++];

        //that which can not be a comparator
        this.validateCapacity(caret, 'expecting an operand after ' + result.lValue.word, result);
        if(this.nextWordIs(caret, '(') || this.nextWordIs(caret, ')')){
            throw new SearchParsingError('\"' + caret.words[caret.index].word + '\" can not be an operand', result);
        }
        result.comparator= caret.words[caret.index++].word;
        this.validateCapacity(caret, 'expecting second part of an operand or a condition after ' + result.lValue.word + ' ' + result.comparator, result);
        if(result.comparator === 'is' && caret.words[caret.index+1 ].word === 'in'){
            result.comparator = result.comparator + ' ' + caret.words[caret.index++].word;
        } else if(result.comparator === 'not '){
            this.validateCapacity(caret, 'expecting second part of an operand after ' + result.comparator, result);
            result.comparator = result.comparator + ' ' + caret.words[caret.index++].word;
        }
        let bracketIndex = 0;
        do {
            this.validateCapacity(caret, 'expecting right value after ' + result.lValue.word + ' ' + result.comparator + ' ' + result.rValues, result);
            let nextWord = caret.words[caret.index++];
            if(')' === nextWord.word){
                bracketIndex --;
            } else  if ('(' === nextWord.word){
                bracketIndex++;
            } else if(',' === nextWord.word && !nextWord.escapedByQuote){
                //if comma is not escaped, it can only be a separator
                //do nothing
            } else {
                let toPush = $.extend({},nextWord); //clone the original word
                if(this.nextWordIs(caret, '(')){
                    this.concatenateUntil(caret, toPush, ')');
                    result.rValues.push(toPush);
                    if(!toPush.word.endsWith(')')){
                        throw new SearchParsingError('missing closing bracker in ' + toPush.word, result);
                    }
                } else {
                    result.rValues.push(toPush);
                }
            }
        } while (bracketIndex > 0);

        return result;
    },

    determineLogicalOperation: function(caret){
        let nextWord = caret.words[caret.index++].word;
        if(nextWord !== 'and' && nextWord !== 'or'){
            throw new SearchParsingError('Expecting logical and or logical or instead of ' + nextWord);
        }
        this.validateCapacity(caret, 'Expecting second part of logical operator after ' + nextWord);
        if(caret.words[caret.index].word === 'not'){
            nextWord = nextWord + ' ' + caret.words[caret.index++].word
        }
        this.validateCapacity(caret, 'Expecting second part of logical operator after ' + nextWord);
        return nextWord;
    },

    assembleBracket: function(inputConditions, logicalOperations){
        if(inputConditions.length === 0){
            throw new SearchParsingError("empty brackets are not acceptable");
        }
        let result = {
            operation: 'or',
            conditions: [{
                operation: 'and',
                conditions: [inputConditions[0]]
            }]
        };
        if(inputConditions.length !== logicalOperations.length+1){
            throw new SearchParsingError("Number of search conditions " + inputConditions.length +" does not match number of logical operators " + logicalOperations.length, result);
        }
        for(let i=1; i < inputConditions.length; i++){
            let currentInputCondition = inputConditions[i];
            if(logicalOperations[i-1].endsWith('not')){
                currentInputCondition = {
                    operation: 'not',
                    conditions: currentInputCondition
                };
            }

            if(logicalOperations[i-1].startsWith('and')){
                result.conditions[result.conditions.length-1].conditions.push(inputConditions[i]);
            } else if (logicalOperations[i-1].startsWith('or')){
                result.conditions.push({
                    operation: 'and',
                    conditions: [currentInputCondition]
                });
            }
        }

        //simplify the expression
        for(let i=0; i<result.conditions.length;i++){
            if(result.conditions[i].conditions.length === 1){
                result.conditions[i] = result.conditions[i].conditions[0];
            }
        }
        if(result.conditions.length === 1){
            result = result.conditions[0];
        }
        return result;
    },

    compileSearchConditionsBracket: function(caret, insideBracket = false) {
        var conditions = [];
        var logicalOperations = [];
        while(caret.index < caret.words.length){
            let nextCondition;
            try {
                if(caret.words[caret.index].word === '('){
                    caret.index++;
                    nextCondition = this.compileSearchConditionsBracket(caret, true)
                } else {
                    nextCondition = this.nextCondition(caret);
                }
            } catch(e){
                if(!e instanceof SearchParsingError){
                    throw e;
                }
                if(!e.partialResult){
                    throw e;
                }
                conditions.push(e.partialResult);
                throw new SearchParsingError(e.message, this.assembleBracket(conditions, logicalOperations));
            }
            conditions.push(nextCondition);

            if(caret.words.length > caret.index && caret.words[caret.index].word === ')'){
                caret.index++;
                return this.assembleBracket(conditions, logicalOperations);
            }

            if(caret.words.length > caret.index){
                try {
                    let nextLogicalOperation = this.determineLogicalOperation(caret);
                    logicalOperations.push(nextLogicalOperation);
                } catch(e){
                    if(! e instanceof SearchParsingError || !e.partialResult){
                        throw e;
                    }
                    throw new SearchParsingError(e.message, this.assembleBracket(conditions, logicalOperations));
                }
            }
        }

        let result = this.assembleBracket(conditions, logicalOperations);
        if(insideBracket){
            throw new SearchParsingError('Reached the end of line and not matched the opening bracket', result);
        }

        return result;
    },

    compileSearchConditions: function(){
        var words = this.extractWords();
        var caret = {
            words: words,
            index: 0
        };
        return this.compileSearchConditionsBracket(caret);
    },

    printWords: function(array, result){
        for(let i=0; i<array.length;i++){
            result.value += "\"";
            this.appendWord(array[i], result);
            result.value +=  "\"";
            if(i+1 !== array.length){
                result.value += ',';
            }
        }
        return result;
    },

    appendWord: function(word, result){
        if(word.beingTyped >= 0){
            result.cursorPosition = result.value.length + word.beingTyped;
        }
        result.value += word.word;
    },

    printCondition: function(condition, result){
        if(condition.lValue){
            this.appendWord(condition.lValue, result);
        }
        if(condition.comparator){
            result.value += ' ' + condition.comparator + ' ';
        }
        if(condition.rValues){
            if(condition.rValues.length > 1){
                result.value += '('
            }
            this.printWords(condition.rValues, result);
            if(condition.rValues.length > 1){
                result.value += ')'
            }
        }
        return result;
    },

    printLogicalOperation: function(logicalOperation, result, bracketsNeeded){
        if(bracketsNeeded){
            result.value += '('
        }
        for(let i=0; i< logicalOperation.conditions.length; i++){
            if(logicalOperation.conditions[i].lValue){
                this.printCondition(logicalOperation.conditions[i], result);
            } else {
                this.printLogicalOperation(logicalOperation.conditions[i], result, true);
            }
            if(i+1 !== logicalOperation.conditions.length && logicalOperation.operation){
                result.value += ' ' + logicalOperation.operation + ' ';
            }
        }
        if(bracketsNeeded){
            result.value += ')'
        }
    },

    searchConditionsToString: function(searchConditions){
        let result = {
            value: '',
            cursorPosition: -1
        };
        if(searchConditions.lValue){
            this.printCondition(searchConditions, result);
        }
        if(searchConditions.conditions){
            this.printLogicalOperation(searchConditions, result);
        }
        return result;
    },

    outputAppliedSuggestion: function(searchConditions, wordToSuggest, selectedSuggestion){
        let currentScrollLeft = this.get(0).scrollLeft;
        wordToSuggest.word = selectedSuggestion.text();
        wordToSuggest.beingTyped = selectedSuggestion.text().length;
        let valueWithSuggestion =this.searchConditionsToString(searchConditions);

        this.requestSkipPostLoad();
        $.bbq.pushState({searchConditions: JSON.stringify(searchConditions)});
        $.bbq.pushState({callPodFilter: valueWithSuggestion.value});
        this.inputField.val(valueWithSuggestion.value);

        this.inputField.get(0).scrollLeft = currentScrollLeft;
        if(valueWithSuggestion.cursorPosition >= 0) {
            // this.inputField.setCursorPosition(valueWithSuggestion.cursorPosition);
            this.inputField.selectRange(valueWithSuggestion.cursorPosition, valueWithSuggestion.cursorPosition);
            let currentCaretPosition = this.caretPosition();
            if(currentCaretPosition - currentScrollLeft > this.get(0).clientWidth){
                this.get(0).scrollLeft = this.caretPosition() - this.get(0).clientWidth;
            }
        }
        this.inputField.blur();
        this.inputField.focus();
    },

    //lvalue must be extracted from the conditions, not copied
    suggestLValue: function(searchConditions, lValue){
        let alreadyTyped = lValue.word.substr(0, lValue.beingTyped);
        let toSuggest = [];
        for(let searchField of this.searchFields){
            if(searchField.getFieldName().indexOf(alreadyTyped) >= 0){
                toSuggest.push(searchField.getFieldName());
            }
        }
        let _this = this;
        this.presentSuggestions(toSuggest, lValue, function(selectedSuggestion){
            _this.outputAppliedSuggestion(searchConditions, lValue, selectedSuggestion);
        });
    },

    suggestRValue: function(searchConditions, lValue, rValue){
        for(let searchField of this.searchFields) {
            if (searchField.getFieldName() !== lValue.word) continue;
            let toSuggest = rValue.word.substr(0, rValue.beingTyped);
            let _this = this;
            searchField.rValueAdvisor(toSuggest, function(suggestions){
                _this.presentSuggestions(suggestions, rValue, function(selectedSuggestion){
                    _this.outputAppliedSuggestion(searchConditions, rValue, selectedSuggestion);
                });
            })
        }
    },

    caretPosition: function(suggestedWord){
        //coordinates of the beginning of the word
        let cursorPosition = this.getCursorPosition() - (suggestedWord?suggestedWord.beingTyped:0);
        let measuredText = this.val().substr(0, cursorPosition);
        let measuredSpan = $('<span style="display:inline-block"></span>').text(measuredText);
        $('#callPodFilterSpan').append(measuredSpan);
        let caretOffset = measuredSpan.width() - this.get(0).scrollLeft;
        measuredSpan.remove();
        return caretOffset;
    },

    presentSuggestions: function(suggestions, suggestedWord, onSuggestionSelected){
        this.suggestionBox.empty();
        this.suggestionBox.hide();
        if(!suggestions || !suggestions.length || suggestions.length === 0){
            this.suggestionsPresented = false;
            return;
        }
        for(let i=0; i<suggestions.length; i++){
            let option = $('<div></div>').text(suggestions[i]).attr("suggestionIndex", i);
            this.suggestionBox.append(option);
        }
        let caretPosition = this.caretPosition(suggestedWord);
        this.suggestionBox.css({position: 'absolute', left: ($('#callPodFilter').offset().left + caretPosition) + 'px'});
        this.suggestionBox.show();
        this.suggestionsPresented = true;
        this.selectedSuggestionIndex = 0;
        this.moveSelectedSuggestion(0);
        this.applySuggestionsCallback = onSuggestionSelected;
        // this.suggestionBox.text(suggestions);
        let _this = this;
        this.suggestionBox.click(function(event){
            let index = event.target.getAttribute("suggestionindex");
            if(index) {
                _this.applySuggestion(index);
                event.preventDefault();
            }
        });
    },

    validateSearchSuggestion: function(searchConditions){
        //if this is a bracket and not a logical condition
        if(searchConditions.conditions && searchConditions.conditions.length > 0) {
            for(let condition of searchConditions.conditions){
                this.validateSearchSuggestion(condition);
            }
        }

        if(searchConditions.lValue && !requestCondition.includes(searchConditions.lValue.word)){
            throw new SearchParsingError("Unknown search condition: ".concat(searchConditions.lValue.word));
        }

        if(searchConditions.comparator && !compareCondition.includes(searchConditions.comparator)){
            throw new SearchParsingError("Unknown compare condition: ".concat(searchConditions.comparator));
        }
    },

    updateSuggestions: function(searchConditions){
        this.requestSkipPostLoad();
        $.bbq.pushState({searchConditions: JSON.stringify(searchConditions)});

        let beingTypedCondition = null;
        let toSearch = [searchConditions];
        let lValueBeingTyped = false;
        let rValueBeingTypedIndex = -1;
        do {
            let cur = toSearch.pop();

            if(cur.lValue && cur.lValue.beingTyped > 0){
                beingTypedCondition = cur;
                lValueBeingTyped = true;
                break;
            }
            if(cur.rValues){
                for(let i=0; i < cur.rValues.length; i++){
                    let rValue = cur.rValues[i];
                    if(rValue.beingTyped > 0){
                        beingTypedCondition = cur;
                        rValueBeingTypedIndex = i;
                        break;
                    }
                }
            }
            //this is a condition, not a logical bracket
            if(cur.comparator) {
                continue;
            }

            if(cur.conditions && cur.conditions.length > 0) {
                toSearch = toSearch.concat(cur.conditions);
            }
        } while(toSearch.length > 0);

        if(lValueBeingTyped){
            this.suggestLValue(searchConditions, beingTypedCondition.lValue);
        } else if(rValueBeingTypedIndex >= 0){
            this.suggestRValue(searchConditions, beingTypedCondition.lValue, beingTypedCondition.rValues[rValueBeingTypedIndex]);
        } else {
            this.hideSuggestions();
        }

        this.validateSearchSuggestion(searchConditions);
    },

    moveSelectedSuggestion: function(delta){
        if(!this.suggestionsPresented){
            return;
        }
        $(this.suggestionBox.children().get(this.selectedSuggestionIndex)).removeClass('selected');
        this.selectedSuggestionIndex += delta;
        if(this.selectedSuggestionIndex < 0){
            this.selectedSuggestionIndex += this.suggestionBox.children().length;
        }
        if(this.selectedSuggestionIndex >= this.suggestionBox.children().length){
            this.selectedSuggestionIndex -= this.suggestionBox.children().length;
        }
        $(this.suggestionBox.children().get(this.selectedSuggestionIndex)).addClass('selected');
    },

    applySuggestion: function(suggestionIndex){
        if(!this.suggestionsPresented) {
            return;
        }
        if(!suggestionIndex) {
            suggestionIndex = this.selectedSuggestionIndex;
        }
        let selectedSuggestion = $(this.suggestionBox.children().get(suggestionIndex));
        this.applySuggestionsCallback(selectedSuggestion);
        this.hideSuggestions();
    },

    hideSuggestions: function(){
        this.presentSuggestions();
    },

    showError: function(error){
        if(error) {
            this.inputField.attr('title', error);
            this.inputField.addClass('error');
        } else {
            this.inputField.removeAttr('title');
            this.inputField.removeClass('error');
        }
    },

    submitSearchRequest: function(){
        window.onSearchRequest();
    },

    getConditionsString: function(){
        let conditions = this.compileSearchConditions();

        this.validateSearchSuggestion(conditions);

        return JSON.stringify(conditions);
    },

    requestSkipPostLoad: function(){
        this.skipPostLoadRequest = true;
    },

    skipPostLoadRequested: function() {
        let result = this.skipPostLoadRequest;
        this.skipPostLoadRequest = false;
        return result;
    },

    //podName = "some name" and service_name=cpq or ()
    init: function(){
        var _this = this;
        this.suggestionBox = $('#suggestionsBox');
        this.inputField = $('#callPodFilter');
        this.keydown(function(event){
            if(_this.suggestionsPresented && [38, 40, 13, 27].indexOf(event.which) > -1) {
                event.preventDefault();
            }
            event.stopPropagation();
        });
        this.keyup(function(event){
            _this.requestSkipPostLoad();
            $.bbq.pushState({callPodFilter: _this.inputField.val()});
            try {
                let keyCode = event.which;
                switch(keyCode){
                    case 38:    //up
                        _this.moveSelectedSuggestion(-1);
                        return;
                    case 40:    //down
                        _this.moveSelectedSuggestion(1);
                        return;
                    case 27:    //esc
                        _this.hideSuggestions();
                        return;
                    case 13:    //return
                        if(_this.suggestionsPresented) {
                            _this.applySuggestion();
                        } else {
                            window.sumbitTimerangeDurationFilters();
                            _this.submitSearchRequest();
                        }
                        return;
                    default:
                        _this.showError();
                        _this.updateSuggestions(_this.compileSearchConditions());
                }
            } catch(e) {
                if(e instanceof SearchParsingError){
                    _this.showError(e.message);
                    if(e.partialResult){
                        _this.updateSuggestions(e.partialResult);
                    }
                } else {
                    throw e;
                }
            }
            event.stopPropagation();
        });
    }
});

let compareCondition = ["=", "!=", ">", ">=","<","<=","in","not in","like", "not like"];
let requestCondition = ["pod_name", "service_name", "namespace", "pod_info","rc_info","dc_info","date"];
callPodFilter.init();

export { callPodFilter };
