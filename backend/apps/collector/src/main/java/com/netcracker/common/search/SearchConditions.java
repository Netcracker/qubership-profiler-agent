package com.netcracker.common.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcracker.common.models.pod.PodInfo;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SearchConditions {
    private static final String FIELD_NAMESPACE = "namespace";
    private static final String FIELD_SERVICE = "service_name";
    private static final String FIELD_POD = "pod_name";
    private static final String DATE_1 = "yyyy/MM/dd HH:mm";
    private static final String DATE_2 = "yyyy-MM-dd HH:mm";
    private static final String DATE_3 = "yyyy/MM/dd";
    private static final String DATE_4 = "yyyy-MM-dd";

    // root condition is normalized into normal disjunctive form: OR-> AND -> NOT structure
    private final LogicalCondition rootCondition;

    protected Map<String, List<String>> serviceNamesToPOD = new HashMap<>();
    protected Map<String, List<String>> nameSpacesToPOD = new HashMap<>();
    protected Map<String, String> serviceNames = new HashMap<>();
    protected Map<String, String> namespaces = new HashMap<>();
    protected Map<String, PodInfo> pods = new HashMap<>();

    public static List<PodInfo> filter(List<PodInfo> activePods, String conditionsStr) {
        SearchConditions conditions = new SearchConditions(activePods, conditionsStr);
        if (!conditions.isValid()) {
            return List.of(); // TODO: exception
        }
        return conditions.filteredPodList();
    }

    SearchConditions(List<PodInfo> activePods, String conditionsStr) {
        this.rootCondition = parseConditions(conditionsStr);
        if (this.rootCondition == null) return;

        for (PodInfo pod : activePods) {
            putMultimap(serviceNamesToPOD, pod.service(), pod.oldPodName());
            serviceNames.put(pod.oldPodName(), pod.service());

            putMultimap(nameSpacesToPOD, pod.namespace(), pod.oldPodName());
            namespaces.put(pod.oldPodName(), pod.namespace());

            pods.put(pod.oldPodName(), pod);
        }
    }

    boolean isValid() {
        return this.rootCondition != null;
    }

    List<PodInfo> filteredPodList(){
        var result = new TreeSet<PodInfo>();
        if(!LogicalCondition.Operation.OR.equals(rootCondition.getOperation())){
            throw new RuntimeException("Expecting OR condition as a master condition of a normalized logical operation " + rootCondition);
        }
        for(Condition lc: rootCondition.getConditions()){
            if(!(lc instanceof LogicalCondition) || !LogicalCondition.Operation.AND.equals(((LogicalCondition)lc).getOperation())){
                throw new RuntimeException("expecting AND at the second level of logical expression " + lc);
            }
            LogicalCondition innerAnd = (LogicalCondition) lc;

            PODNameFilter filter = new PODNameFilter();
            for(Condition insideAnd: innerAnd.getConditions()){
                ComparisonCondition cc;
                if(insideAnd instanceof LogicalCondition){
                    LogicalCondition innerNot = (LogicalCondition) insideAnd;
                    if(!LogicalCondition.Operation.NOT.equals(innerNot.getOperation())){
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
            // find by names
            filter.podNames.forEach(name -> result.add(pods.get(name)));
        }
        return new ArrayList<>(result);
    }

    public static<K,V> void putMultimap(Map<K, List<V>> map, K key, V value){
        List<V> toPut = map.get(key);
        if(toPut == null) {
            toPut = new LinkedList<>();
            map.put(key, toPut);
        }
        toPut.add(value);
    }


    private LogicalCondition parseConditions(String conditionsStr)  {
        JsonNode node = null;
        try {
            node = new ObjectMapper().readTree(conditionsStr);
        } catch (JsonProcessingException e) {
            return null;
        }

        Condition temp = toCondition(node);
        temp = normalizeCondition(temp);
        LogicalCondition result ;
        if(! (temp instanceof LogicalCondition)){
            result = new LogicalCondition(LogicalCondition.Operation.OR, new LogicalCondition(LogicalCondition.Operation.AND, temp));
        } else if(!LogicalCondition.Operation.OR.equals(((LogicalCondition)temp).getOperation())) {
            result = new LogicalCondition(LogicalCondition.Operation.OR, temp);
        } else {
            result = (LogicalCondition) temp;
        }

        List<Condition> listOfAnds = new ArrayList<Condition>(result.getConditions().size());
        for(Condition c : result.getConditions()){
            if(! (c instanceof LogicalCondition) || !LogicalCondition.Operation.AND.equals(((LogicalCondition)c).getOperation())){
                listOfAnds.add(new LogicalCondition(LogicalCondition.Operation.AND, c));
            } else {
                listOfAnds.add(c);
            }
        }

        result.setConditions(listOfAnds);
        return result;
    }

    private Condition toCondition(JsonNode node){
        JsonNode comparator = node.get("comparator");
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
        // NOT NOT  (single condition -> single condition)
        // NOT AND  (single NOT -> single OR)
        // NOT  OR  (single NOT -> single OR)
        // AND  OR  (single AND condition -> single OR condition)
        // AND AND  (single AND condition -> single AND condition)
        //  OR  OR  (single OR -> single OR)
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
                    if(LogicalCondition.Operation.NOT.equals(child.getOperation()))
                        return normalizeCondition(child.getConditions().get(0));
                    //otherwise swap or -> and(not) and -> or(not)
                    LogicalCondition reverseOperation = new LogicalCondition();
                    reverseOperation.setOperation(LogicalCondition.Operation.OR.equals(child.getOperation())? LogicalCondition.Operation.AND: LogicalCondition.Operation.OR);

                    for(Condition toNegate: child.getConditions()){
                        reverseOperation.addCondition(normalizeCondition(new LogicalCondition(LogicalCondition.Operation.NOT, toNegate)));
                    }
                    reverseOperation = (LogicalCondition) normalizeCondition(reverseOperation);
                    newConditions.add(reverseOperation);
                    continue;
                case AND:
                    if(LogicalCondition.Operation.AND.equals(child.getOperation())){
                        oldConditions.addAll(child.getConditions());
                        continue;
                    }
                    if(LogicalCondition.Operation.OR.equals(child.getOperation())){
                        List<Condition> remaining = new ArrayList<Condition>();
                        for(int j=i+1; j < oldConditions.size(); j++){
                            Condition notYetNormalized = oldConditions.get(j);
                            remaining.add(normalizeCondition(notYetNormalized));
                        }

                        remaining.addAll(newConditions);
                        LogicalCondition newOr = new LogicalCondition();
                        newOr.setOperation(LogicalCondition.Operation.OR);
                        for(Condition childCondition: child.getConditions()){
                            LogicalCondition innerAnd = new LogicalCondition(LogicalCondition.Operation.AND, new ArrayList<Condition>(remaining));
                            innerAnd.addCondition(normalizeCondition(childCondition));
                            newOr.addCondition(normalizeCondition(innerAnd));
                        }
                        return normalizeCondition(newOr);
                    }
                    // else - not
                    newConditions.add(normalizeCondition(c));
                case OR:
                    if(LogicalCondition.Operation.OR.equals(child.getOperation())){
                        oldConditions.addAll(child.getConditions());
                    }

                    // else NOT or AND
                    newConditions.add(normalizeCondition(c));
            }
        }
        lc.setConditions(newConditions);
        return lc;
    }

    protected static class PODNameFilter {
        Set<String> podNames = null;
        public void applyPODNameLimitation(Set<String> availablePODNames){
            if (podNames == null){
                podNames = new HashSet<>(availablePODNames);
            } else {
                podNames.retainAll(availablePODNames);
            }
        }
    }



    protected void applyToFilter(PODNameFilter filter, ComparisonCondition cc){
        if (FIELD_POD.equalsIgnoreCase(cc.getlValue())){
            Set<String> options = serviceNames.keySet();
            Set<String> okOptions = filterAvailableOptions(options, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okOptions);
        }
        if (FIELD_SERVICE.equalsIgnoreCase(cc.getlValue())) {
            Set<String> okPODNames = filterPODNamesByMultimapMapping(serviceNamesToPOD, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okPODNames);
        }
        if (FIELD_NAMESPACE.equalsIgnoreCase(cc.getlValue())) {
            Set<String> okPODNames = filterPODNamesByMultimapMapping(nameSpacesToPOD, cc.getComparator(), cc.getrValues());
            filter.applyPODNameLimitation(okPODNames);
        }
//        if ("date".equalsIgnoreCase(cc.getlValue())) { // skipped
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
        return switch (unit) {
            case 'y' -> amount * 1000L * 3600L * 24L * 365L;
            case 'M' -> amount * 1000L * 3600L * 24L * 30L;
            case 'w' -> amount * 1000L * 3600L * 24L * 7L;
            case 'd' -> amount * 1000L * 3600L * 24L;
            case 'H' -> amount * 1000L * 3600L;
            case 'm' -> amount * 1000L * 60L;
            default -> throw new RuntimeException("Unsupported unit " + unit);
        };
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
        switch (cmp) {
            case EQ, IN -> {
                result = new HashSet<>(superset);
                result.retainAll(compareWith);
                return result;
            }
            case NE, NOTIN -> {
                result = new HashSet<>(superset);
                result.removeAll(compareWith);
                return result;
            }
            case LIKE -> {
                return filterByLike(superset, compareWith);
            }
            case NOTLIKE -> {
                Set<String> toRemove = filterByLike(superset, compareWith);
                result = new HashSet<>(superset);
                result.removeAll(toRemove);
                return result;
            }
            default -> throw new RuntimeException("Invalid comparator " + cmp);
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

            // last word
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
            patternIndex += patternWord.length() + 1; // word plus percent symbol

        }while(patternPercentIndex >= 0);
        return true;
    }
}
