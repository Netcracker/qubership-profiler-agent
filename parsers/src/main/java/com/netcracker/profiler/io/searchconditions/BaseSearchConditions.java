package com.netcracker.profiler.io.searchconditions;

import static com.netcracker.profiler.io.searchconditions.ComparisonCondition.Comparator.*;
import static com.netcracker.profiler.io.searchconditions.LogicalCondition.Operation.*;

import com.netcracker.profiler.io.Call;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Scope("prototype")
@Profile("filestorage")
public class BaseSearchConditions {
    private static final Logger log = LoggerFactory.getLogger(BaseSearchConditions.class);

    private String conditionsStr;
    //root condition is normalized into normal disjunctive form: OR-> AND -> NOT structure
    private LogicalCondition rootCondition;
    protected Date globalDateFrom;
    protected Date globalDateTo;

    public static final String DATE_1 = "yyyy/MM/dd HH:mm";
    public static final String DATE_2 = "yyyy-MM-dd HH:mm";
    public static final String DATE_3 = "yyyy/MM/dd";
    public static final String DATE_4 = "yyyy-MM-dd";

    protected Map<String, List<String>> serviceNamesToPOD = new HashMap<>();
    protected Map<String, List<String>> podInfoToPOD = new HashMap<>();
    protected Map<String, List<String>> rcInfoToPOD = new HashMap<>();
    protected Map<String, List<String>> dcInfoToPOD = new HashMap<>();
    protected Map<String, List<String>> nameSpacesToPOD = new HashMap<>();
    protected Map<String, String> serviceNames = new HashMap<String, String>();
    protected Map<String, String> namespaces = new HashMap<String, String>();
    protected Map<String, Date[]> podLifetimes;


    protected BaseSearchConditions() {
        throw new RuntimeException("no-args not supported");
    }

    public BaseSearchConditions(String conditionsStr, Date globalDateFrom, Date globalDateTo) throws IOException {
        this.conditionsStr = conditionsStr;
        this.rootCondition = parseConditions(conditionsStr);
        this.globalDateFrom = globalDateFrom;
        this.globalDateTo = globalDateTo;
    }

    public BaseSearchConditions(String conditionsStr, long globalDateFrom, long globalDateTo) throws IOException {
        this(conditionsStr, new Date(globalDateFrom), new Date(globalDateTo));
    }

    public static<K,V> void putMultimap(Map<K, List<V>> map, K key, V value){
        List<V> toPut = map.get(key);
        if(toPut == null) {
            toPut = new LinkedList<>();
            map.put(key, toPut);
        }
        toPut.add(value);
    }

    protected void byLines(Map<String, List<String>> map, String info, String podName) { // user would search info by lines
        for (String line : info.split("\n")) {
            putMultimap(map, line.trim(), podName);
        }
    }

    protected void initServiceNamespaceMapping(){
    }

    private LogicalCondition parseConditions(String conditionsStr) throws IOException {
        JsonNode node = new ObjectMapper().readTree(conditionsStr);

        Condition temp = toCondition(node);
        temp = normalizeCondition(temp);
        LogicalCondition result ;
        if(! (temp instanceof LogicalCondition)){
            result = new LogicalCondition(OR, new LogicalCondition(AND, temp));
        } else if(!OR.equals(((LogicalCondition)temp).getOperation())) {
            result = new LogicalCondition(OR, temp);
        } else {
            result = (LogicalCondition) temp;
        }

        List<Condition> listOfAnds = new ArrayList<Condition>(result.getConditions().size());
        for(Condition c : result.getConditions()){
            if(! (c instanceof LogicalCondition) || !AND.equals(((LogicalCondition)c).getOperation())){
                listOfAnds.add(new LogicalCondition(AND, c));
            } else {
                listOfAnds.add(c);
            }
        }

        result.setConditions(listOfAnds);

        return result;
    }

    private Condition toCondition(JsonNode node){
        JsonNode comparator =node.get("comparator");
        JsonNode operation = node.get("operation");

        if(comparator != null){
            ComparisonCondition result = new ComparisonCondition();
            String lValue = node.get("lValue").get("word").asText();
            result.setlValue(lValue);

            JsonNode rValues = node.get("rValues");
            for (JsonNode rValue : rValues) {
                result.addRValue(rValue.get("word").asText());
            }

            result.setComparator(ComparisonCondition.Comparator.from(comparator.asText()));
            return result;
        }

        if(operation != null){
            LogicalCondition result = new LogicalCondition();
            result.setOperation(LogicalCondition.Operation.from(operation.asText()));
            JsonNode conditions = node.get("conditions");
            for (JsonNode condition : conditions) {
                result.addCondition(toCondition(condition));
            }
            return result;
        }

        throw new RuntimeException("Unknown json node " + node.asText());
    }

    /**
     * @param condition
     * @return normal disjunctive form of this logical expression
     */
    private Condition normalizeCondition(Condition condition){
        //NOT NOT (single condition -> single condition)
        //NOT AND (single NOT -> single OR)
        //NOT OR (single NOT -> single OR)
        //AND OR (single AND condition -> single OR condition)
        //AND AND (single AND condition -> single AND condition)
        //OR OR (single OR -> single OR)
        if(!(condition instanceof LogicalCondition)){
            return condition;
        }

        LogicalCondition lc = (LogicalCondition) condition;
        List<Condition> oldConditions = new ArrayList<Condition>(lc.getConditions());
        List<Condition> newConditions = new ArrayList<Condition>();
        for(int i=0; i < oldConditions.size(); i++) {
            Condition c = oldConditions.get(i);
            Condition normalized = normalizeCondition(c);
            if(!(normalized instanceof LogicalCondition)){
                newConditions.add(normalized);
                continue;
            }
            LogicalCondition child = (LogicalCondition)normalized;
            switch(lc.getOperation()){
                case NOT:
                    if(NOT.equals(child.getOperation()))
                        return normalizeCondition(child.getConditions().get(0));
                    //otherwise swap or -> and(not) and -> or(not)
                    LogicalCondition reverseOperation = new LogicalCondition();
                    reverseOperation.setOperation(OR.equals(child.getOperation())?AND:OR);

                    for(Condition toNegate: child.getConditions()){
                        reverseOperation.addCondition(normalizeCondition(new LogicalCondition(NOT, toNegate)));
                    }
                    reverseOperation = (LogicalCondition) normalizeCondition(reverseOperation);
                    newConditions.add(reverseOperation);
                    continue;
                case AND:
                    if(AND.equals(child.getOperation())){
                        oldConditions.addAll(child.getConditions());
                        continue;
                    }
                    if(OR.equals(child.getOperation())){
                        List<Condition> remaining = new ArrayList<Condition>();
                        for(int j=i+1; j < oldConditions.size(); j++){
                            Condition notYetNormalized = oldConditions.get(j);
                            remaining.add(normalizeCondition(notYetNormalized));
                        }

                        remaining.addAll(newConditions);
                        LogicalCondition newOr = new LogicalCondition();
                        newOr.setOperation(OR);
                        for(Condition childCondition: child.getConditions()){
                            LogicalCondition innerAnd = new LogicalCondition(AND, new ArrayList<Condition>(remaining));
                            innerAnd.addCondition(normalizeCondition(childCondition));
                            newOr.addCondition(normalizeCondition(innerAnd));
                        }
                        return normalizeCondition(newOr);
                    }
                    //else - not
                    newConditions.add(normalizeCondition(c));
                case OR:
                    if(OR.equals(child.getOperation())){
                        oldConditions.addAll(child.getConditions());
                    }

                    //else NOT or AND
                    newConditions.add(normalizeCondition(c));
            }
        }
        lc.setConditions(newConditions);
        return lc;
    }

    protected static class PODNameFilter{
        Set<String> podNames = null;
        Date dateFrom;
        Date dateTo;

        public PODNameFilter(Date dateFrom, Date dateTo) {
            this.dateFrom = dateFrom;
            this.dateTo = dateTo;
        }

        public void applyPODNameLimitation(Set<String> availablePODNames){
            if(podNames == null){
                podNames = new HashSet<String>(availablePODNames);
            } else {
                podNames.retainAll(availablePODNames);
            }
        }
    }

    public List<LoadRequest> loadRequests(){
        initServiceNamespaceMapping();
        List<LoadRequest> result = new ArrayList<LoadRequest>();
        if(!OR.equals(rootCondition.getOperation())){
            throw new RuntimeException("Expecting OR condition as a master condition of a normalized logical operation " + rootCondition);
        }
        for(Condition lc: rootCondition.getConditions()){
            if(!(lc instanceof LogicalCondition) || !AND.equals(((LogicalCondition)lc).getOperation())){
                throw new RuntimeException("expecting AND at the second level of logical expression " + lc);
            }
            LogicalCondition innerAnd = (LogicalCondition) lc;
            PODNameFilter filter = new PODNameFilter(globalDateFrom, globalDateTo);

            for(Condition insideAnd: innerAnd.getConditions()){
                ComparisonCondition cc;
                if(insideAnd instanceof LogicalCondition){
                    LogicalCondition innerNot = (LogicalCondition) insideAnd;
                    if(!NOT.equals(innerNot.getOperation())){
                        throw new RuntimeException("Expecting NOT or ComparisonCondition at the 3-rd level of " + innerNot);
                    }
                    if(innerNot.getConditions().size() != 1){
                        throw new RuntimeException("NOT should have exactly one condition inside it " + innerNot);
                    }
                    cc = (ComparisonCondition) innerNot.getConditions().get(0);
                } else {
                    cc = (ComparisonCondition)insideAnd;
                }

                applyToFilter(filter, cc);
            }

            removeInactivePODs(filter);
            for(String podName: filter.podNames) {
                LoadRequest request = new LoadRequest(podName, filter.dateFrom, filter.dateTo);
                result.add(request);
            }
        }
        return result;
    }

    protected boolean checkPodLifetime(){
        return false;
    }

    private void removeInactivePODs(PODNameFilter filter){
        Date dateFrom = filter.dateFrom;
        Date dateTo = filter.dateTo;

        if(dateFrom.compareTo(dateTo) > 0){
            throw new RuntimeException("Date from " + dateFrom + " is greater than date to " + dateTo);
        }

        if(!checkPodLifetime()){
            return;
        }

        for(Iterator<String> it = filter.podNames.iterator();it.hasNext();){
            String podName = it.next();
            Date[] bounds = podLifetimes.get(podName);
            //POD is not even listed in the global bounds
            if(bounds == null || bounds[0].compareTo(dateTo) > 0 || bounds[1].compareTo(dateFrom) < 0){
                it.remove();
            }
        }
    }



    protected void applyToFilter(PODNameFilter filter, ComparisonCondition cc){
        if("pod_name".equalsIgnoreCase(cc.getlValue())){
            Set<String> options = serviceNames.keySet();
            Set<String> okOptions = filterAvailableOptions(options, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okOptions);
        }
        if ("service_name".equalsIgnoreCase(cc.getlValue())) {
            Set<String> okPODNames = filterPODNamesByMultimapMapping(serviceNamesToPOD, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okPODNames);
        }
        if ("namespace".equalsIgnoreCase(cc.getlValue())) {
            Set<String> okPODNames = filterPODNamesByMultimapMapping(nameSpacesToPOD, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okPODNames);
        }
        if ("date".equalsIgnoreCase(cc.getlValue())) {
            if (cc.getrValues().size() != 1) {
                throw new RuntimeException("Expecting exactly one rValue for dates " + conditionsStr);
            }
            long newDate = parseDateTime(cc.getrValues().get(0));
            if(GT.equals(cc.getComparator()) || GE.equals(cc.getComparator())){
                filter.dateFrom = new Date(Math.max(filter.dateFrom.getTime(), newDate));
            } else if(LT.equals(cc.getComparator()) || LE.equals(cc.getComparator())){
                filter.dateTo = new Date(Math.max(filter.dateFrom.getTime(), newDate));
            } else {
                throw new RuntimeException("Comparator " + cc.getComparator() + " is not supported for dates");
            }
        }
    }

    private long parseDateTime(String dateStr){
        for(String dateFormat : Arrays.asList(DATE_1, DATE_2, DATE_3, DATE_4)){
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            try{
                return sdf.parse(dateStr).getTime();
            } catch (ParseException e) {
                //no luck
            };
        }

        int bracketOpen = StringUtils.indexOf(dateStr, '(');
        int bracketClose = StringUtils.lastIndexOf(dateStr, ')');
        String function = StringUtils.substring(dateStr, bracketOpen);
        String operand = StringUtils.substring(dateStr, bracketOpen + 1, bracketClose);

        long time = resolveDateFunction(function);
        long delta = resolveDateOperand(operand);

        return time - delta;
    }

    private long resolveDateFunction(String function){
        if("now".equalsIgnoreCase(function)){
            return System.currentTimeMillis();
        }
        throw new RuntimeException("Unsupported date function " + function);
    }

    private long resolveDateOperand(String operand){
        String amountStr = StringUtils.substring(operand, 0, operand.length()-1);
        long amount = Long.parseLong(amountStr);
        char unit = operand.charAt(operand.length()-1);
        switch(unit){
            case 'y': return amount * 1000L * 3600L * 24L * 365L;
            case 'M': return amount * 1000L * 3600L * 24L * 30L;
            case 'w': return amount * 1000L * 3600L * 24L * 7L;
            case 'd': return amount * 1000L * 3600L * 24L ;
            case 'H': return amount * 1000L * 3600L;
            case 'm': return amount * 1000L * 60L;
            default: throw new RuntimeException("Unsupported unit " + unit);
        }
    }

    protected Set<String> filterPODNamesByMultimapMapping(
            Map<String, List<String>> podNameAggregator,
            ComparisonCondition.Comparator cmp,
            Collection<String> compareWith) {
        Set<String> options = podNameAggregator.keySet();
        Set<String> okOptions = filterAvailableOptions(options, cmp, compareWith);
        Set<String> okPODNames = new HashSet<String>();
        for (String okOption : okOptions) {
            okPODNames.addAll(podNameAggregator.get(okOption));
        }
        return okPODNames;
    }

    private Set<String> filterAvailableOptions(Set<String> superset, ComparisonCondition.Comparator cmp, Collection<String> compareWith){
        Set<String> result ;
        switch(cmp){
            case EQ:
            case IN:
                result = new HashSet<String>(superset);
                result.retainAll(compareWith);
                return result;
            case NE:
            case NOTIN:
                result = new HashSet<String>(superset);
                result.removeAll(compareWith);
                return result;
            case LIKE:
                return filterByLike(superset, compareWith);
            case NOTLIKE:
                Set<String> toRemove = filterByLike(superset, compareWith);
                result = new HashSet<String>(superset);
                result.removeAll(toRemove);
                return result;
            default:
                throw new RuntimeException("Invalid comparator " + cmp);

        }
    }

    private Set<String> filterByLike(Set<String> superset, Collection<String> compareWith){
        Set<String> result = new HashSet<String>();
        for(String str: superset) {
            for (String toCompare : compareWith) {
                if(matchesPattern(str, toCompare)){
                    result.add(str);
                }
            }
        }
        return result;
    }

    private boolean matchesPattern(String word, String pattern){
        int patternIndex = 0;
        int wordIndex = 0;
        int patternPercentIndex;
        do {
            patternPercentIndex = pattern.indexOf('%', patternIndex);
            String patternWord = pattern.substring(patternIndex, patternPercentIndex >= 0 ? patternPercentIndex : pattern.length());
            //first word
            if(patternIndex == 0 && patternPercentIndex > 0) {
                if(!word.startsWith(patternWord)){
                    return false;
                }
            }

            //last word
            if(patternIndex > 0 && patternPercentIndex < 0) {
                if(!word.endsWith(patternWord)){
                    return false;
                }
            }

            wordIndex = word.indexOf(patternWord, wordIndex);
            if(wordIndex < 0) {
                return false;
            }
            wordIndex += patternWord.length();
            patternIndex += patternWord.length() + 1; //word plus percent symbol

        }while(patternPercentIndex >= 0);
        return true;
    }


    public List<Call> filterLoadedCalls(List<Call> toFilter){
        return toFilter;
    }
}
