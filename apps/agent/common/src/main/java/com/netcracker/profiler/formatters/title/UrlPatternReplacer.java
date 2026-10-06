package com.netcracker.profiler.formatters.title;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlPatternReplacer {

    private static Pattern SPECIALS = Pattern.compile("\\*\\*|\\*|\\$id\\$");
    private List<Pattern> regExPatterns;

    public UrlPatternReplacer(List<String> patterns) {
        regExPatterns = new ArrayList<>(patterns.size());
        for(String pattern : patterns) {
            regExPatterns.add(parsePattern(pattern));
        }
    }

    public String replace(String url){
        for(Pattern p : regExPatterns) {
            Matcher m = p.matcher(url);
            if(m.matches()) {
                StringBuilder resultSb = new StringBuilder();
                for(int i = 1; i <= m.groupCount(); i++) {
                    resultSb.append(m.group(i)).append("$id$");
                }
                resultSb.delete(resultSb.length()-4, resultSb.length());
                return resultSb.toString();
            }
        }
        return url;
    }

    private Pattern parsePattern(String p) {
        StringBuffer sb = new StringBuffer("(");
        Matcher m = SPECIALS.matcher(p);
        while (m.find()) {
            String s = m.group();
            if ("**".equals(s)) s = ".*";
            else if ("*".equals(s)) s = "[^/]*";
            else if ("$id$".equals(s)) s = ")[^/]*(";
            m.appendReplacement(sb, s);
        }
        m.appendTail(sb);
        sb.append(")");
        return Pattern.compile(sb.toString());
    }

}
