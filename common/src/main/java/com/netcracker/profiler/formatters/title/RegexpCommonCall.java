package com.netcracker.profiler.formatters.title;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RegexpCommonCall {
    private static final Pattern URL_MATCHER = Pattern.compile("https?://[^/]+(/.*)?");
    private static final Pattern SALES_ORDER_DATA_MATCHER = Pattern.compile("(.*[/])\\w+?-\\w+?-\\w+?-\\w+?-\\w+((/.*)|$)");
    private static final Pattern EXT_REGEXP = Pattern.compile("\\.\\w{1,5}$");

    public static String replaceIds(String line, int minDigitsInId) {
        if(minDigitsInId==-1) {
            return line;
        }
        String[] urlItems = line.split("/");
        if(urlItems.length == 0) {
            return line;
        }
        StringBuilder resultUrl = new StringBuilder();
        for(int i=0; i<urlItems.length; i++) {
            String urlItem = urlItems[i];
            if(i == urlItems.length-1 && EXT_REGEXP.matcher(urlItem).find()) {
                resultUrl.append(urlItem).append("/");
                break;
            }
            int digitCnt = 0;
            for(char c : urlItem.toCharArray()) {
                if(Character.isDigit(c)) {
                    digitCnt++;
                }
            }
            if(digitCnt >= minDigitsInId) {
                resultUrl.append("$id$");
            } else {
                resultUrl.append(urlItem);
            }
            resultUrl.append("/");
        }
        resultUrl.deleteCharAt(resultUrl.length()-1);
        return resultUrl.toString();
    }

    public static String replaceSalesOrderDataContext(String line) {
        Matcher matcher = SALES_ORDER_DATA_MATCHER.matcher(line);
        if (matcher.find()) {
            return matcher.group(1) + "$id$" + matcher.group(2);
        }
        return line;
    }

    public static String replaceUrl(String line) {
        Matcher matcher = URL_MATCHER.matcher(line);
        if (matcher.find()) {
            String result = matcher.group(1);
            if(result == null || result.isEmpty()) {
                result = "/";
            }
            return result;
        }
        return line;
    }

}
