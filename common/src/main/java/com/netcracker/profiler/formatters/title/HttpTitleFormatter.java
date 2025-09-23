package com.netcracker.profiler.formatters.title;

import static com.netcracker.profiler.formatters.title.TitleCommonTools.*;

import com.netcracker.profiler.agent.ParameterInfo;

import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

public class HttpTitleFormatter extends AbstractTitleFormatter {

    public static final String DISABLE_DEFAULT_URL_REPLACE_PATTERNS = "disableDefaultUrlReplacePatterns";
    public static final String MIN_DIGITS_IN_ID = "minDigitsInId";
    public static final String URL_PATTERN_REPLACER = "urlPatternReplacer";

    private static final int DEFAULT_MIN_DIGITS_IN_ID = 4;

    private Set<String> SKIP_PARAMS = Collections.singleton("jmeter.step");

    private Pattern uiComponentPatternRegEx1 = Pattern.compile("/ \\S+ (?=/)");
    private Pattern uiComponentPatternRegEx2 = Pattern.compile("localValue=((?:\\S|(?!\\s(?:/|\\S+=))\\s)+)");

    @Override
    public ProfilerTitle formatTitle(String classMethod, Map<String, Integer> tagToIdMap, Object params, List<ParameterInfo> defaultListParams) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();

        String jmeterStep = getParameter(tagToIdMap, params, "jmeter.step");
        if (!jmeterStep.isEmpty()) {
            title.append("JMeter: ").appendHtml("<b>").append(jmeterStep).appendHtml("</b>").append(", ");
        }

        Collection<String> uiComponents = getParameterValues(tagToIdMap, params, "ui.component");
        if (!uiComponents.isEmpty()) {
            // CBTUI components
            // TODO: Check on actual values
            if (uiComponents.size() > 1) {
                title.append(uiComponents.size()).append(" CBTUI actions ");
                for(String s : uiComponents) {
                    title.append(formatUIComponent(s));
                }
            } else {
                title.append(formatUIComponent(uiComponents.iterator().next()));
            }
        } else {
            // Regular pages
            Collection<String> webUrls = getParameterValues(tagToIdMap, params, "web.url");
            Collection<String> webQueries = getParameterValues(tagToIdMap, params, "web.query");

            if (webUrls.size() == 1 && webQueries.size() <= 1) {
                String webMethod = getParameter(tagToIdMap, params, "web.method");
                if (!webMethod.isEmpty()) {
                    title.append(webMethod).append(' ');
                }
                if (webQueries.size() == 1) {
                    title.append(getURLPath(webUrls.iterator().next())).append('?').append(webQueries.iterator().next());
                } else {
                    title.append(getURLPath(webUrls.iterator().next()));
                }
            } else if (!webUrls.isEmpty()) {
                title.append(webUrls.size()).append(" pages: ");
                for(String s : webUrls) {
                    title.append(getURLPath(s));
                    title.append(", ");
                }
                title.deleteLastChars(2);
                if (!webQueries.isEmpty()) {
                    title.append(", ").append(webQueries.size()).append(" query strings: ").append(StringUtils.join(webQueries, ", "));
                }
            }
        }
        addParameter(title, tagToIdMap, params, ", client: ", "web.remote.addr",
                new Function<Collection<String>, String>() {
                    @Override
                    public String apply(Collection<String> remoteAddresses) {
                        return StringUtils.join(new HashSet(remoteAddresses), ", ");
                    }
                });
        addGenericParams(title, tagToIdMap, params, defaultListParams, SKIP_PARAMS);

        return title;
    }

    @Override
    public ProfilerTitle formatCommonTitle(String classMethod, Map<String, Integer> tagToIdMap, Map<Integer, List<String>> params, Map<String, Object> formatContext) {
        ProfilerTitleBuilder title = new ProfilerTitleBuilder();
        boolean disableDefaultUrlReplacePatterns = (boolean) formatContext.get(DISABLE_DEFAULT_URL_REPLACE_PATTERNS);
        int minDigitsInId = (int) formatContext.get(MIN_DIGITS_IN_ID);
        if(minDigitsInId == 0) {
            minDigitsInId = DEFAULT_MIN_DIGITS_IN_ID;
        }
        UrlPatternReplacer urlPatternReplacer = (UrlPatternReplacer) formatContext.get(URL_PATTERN_REPLACER);

        Collection<String> webUrlList = getParameterValues(tagToIdMap, params, "web.url");
        if(webUrlList.isEmpty()) {
            title.append(classMethod).setDefault(true);
            return title;
        }
        if(webUrlList.size() == 1) {
            String webUrlStr = webUrlList.iterator().next();
            Collection<String> webMethodList = getParameterValues(tagToIdMap, params, "web.method");
            String webMethodStr = (webMethodList.isEmpty()) ? "" : webMethodList.iterator().next();
            title.append("HTTP ").append(webMethodStr).append(" ").append(parseCommonURL(webUrlStr, disableDefaultUrlReplacePatterns, minDigitsInId, urlPatternReplacer));
        } else {
            title.append("HTTP ").append(webUrlList.size()).append(" pages: ");
            for(String webUrlStr : webUrlList) {
                title.append(parseCommonURL(webUrlStr, disableDefaultUrlReplacePatterns, minDigitsInId, urlPatternReplacer)).append(", ");
            }
            title.deleteLastChars(2);
        }
        return title;
    }

    private String getURLPath(String url) {
        int pathIndex = url.indexOf('/', url.indexOf("://") + 3);
        return pathIndex == -1 ? url : url.substring(pathIndex);
    }

    private String formatUIComponent(String uiComponent) {
        String result = uiComponentPatternRegEx1.matcher(uiComponent).replaceAll("");
        result = uiComponentPatternRegEx2.matcher(result).replaceAll("$1");
        return result;
    }

    private String parseCommonURL(String webUrlStr, boolean disableDefaultUrlReplacePatterns, int minDigitsInId, UrlPatternReplacer urlPatternReplacer) {
        String commonUrl = RegexpCommonCall.replaceUrl(webUrlStr);
        if(!disableDefaultUrlReplacePatterns) {
            commonUrl = RegexpCommonCall.replaceIds(RegexpCommonCall.replaceSalesOrderDataContext(commonUrl), minDigitsInId);
        }
        commonUrl = urlPatternReplacer.replace(commonUrl);
        return commonUrl;
    }
}
