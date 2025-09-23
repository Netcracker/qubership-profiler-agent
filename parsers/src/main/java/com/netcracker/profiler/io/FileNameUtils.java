package com.netcracker.profiler.io;

import java.util.ArrayDeque;
import java.util.ArrayList;

public class FileNameUtils {
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static boolean isSpaceFileNameChar(char c) {
        return Character.isWhitespace(c) || c == '"' || c == '\'';
    }

    public static String trimFileName(String fileName) {
        if (fileName == null) {
            return null;
        }
        int st = 0, len = fileName.length();

        while (st < len && isSpaceFileNameChar(fileName.charAt(st)))
            st++;

        while (st < len && isSpaceFileNameChar(fileName.charAt(len - 1)))
            len--;

        return fileName.substring(st, len);
    }

    public static boolean wildcardMatch(String fileName, String wildcardMatcher) {
        if (fileName == null && wildcardMatcher == null) {
            return true;
        } else if (fileName != null && wildcardMatcher != null) {

            String[] wcs = splitOnTokens(wildcardMatcher);
            boolean anyChars = false;
            int textIdx = 0;
            int wcsIdx = 0;
            ArrayDeque backtrack = new ArrayDeque(wcs.length);

            do {
                if (!backtrack.isEmpty()) {
                    int[] array = (int[])backtrack.pop();
                    wcsIdx = array[0];
                    textIdx = array[1];
                    anyChars = true;
                }

                for(; wcsIdx < wcs.length; ++wcsIdx) {
                    if (wcs[wcsIdx].equals("?")) {
                        ++textIdx;
                        if (textIdx > fileName.length()) {
                            break;
                        }

                        anyChars = false;
                    } else if (wcs[wcsIdx].equals("*")) {
                        anyChars = true;
                        if (wcsIdx == wcs.length - 1) {
                            textIdx = fileName.length();
                        }
                    } else {
                        if (anyChars) {
                            textIdx = checkIndexOf(fileName, textIdx, wcs[wcsIdx]);
                            if (textIdx == -1) {
                                break;
                            }

                            int repeat = checkIndexOf(fileName, textIdx + 1, wcs[wcsIdx]);
                            if (repeat >= 0) {
                                backtrack.push(new int[]{wcsIdx, repeat});
                            }
                        } else if (!checkRegionMatches(fileName, textIdx, wcs[wcsIdx])) {
                            break;
                        }

                        textIdx += wcs[wcsIdx].length();
                        anyChars = false;
                    }
                }

                if (wcsIdx == wcs.length && textIdx == fileName.length()) {
                    return true;
                }
            } while(!backtrack.isEmpty());

            return false;
        } else {
            return false;
        }
    }

    public static String[] splitOnTokens(String text) {
        if (text.indexOf(63) == -1 && text.indexOf(42) == -1) {
            return new String[]{text};
        } else {
            char[] array = text.toCharArray();
            ArrayList<String> list = new ArrayList();
            StringBuilder buffer = new StringBuilder();
            char prevChar = 0;
            char[] var5 = array;
            int var6 = array.length;

            for(int var7 = 0; var7 < var6; ++var7) {
                char ch = var5[var7];
                if (ch != '?' && ch != '*') {
                    buffer.append(ch);
                } else {
                    if (buffer.length() != 0) {
                        list.add(buffer.toString());
                        buffer.setLength(0);
                    }

                    if (ch == '?') {
                        list.add("?");
                    } else if (prevChar != '*') {
                        list.add("*");
                    }
                }

                prevChar = ch;
            }

            if (buffer.length() != 0) {
                list.add(buffer.toString());
            }

            return (String[])list.toArray(EMPTY_STRING_ARRAY);
        }
    }

    private static int checkIndexOf(String str, int strStartIndex, String search) {
        int endIndex = str.length() - search.length();
        if (endIndex >= strStartIndex) {
            for(int i = strStartIndex; i <= endIndex; ++i) {
                if (checkRegionMatches(str, i, search)) {
                    return i;
                }
            }
        }

        return -1;
    }

    private static boolean checkRegionMatches(String str, int strStartIndex, String search) {
        return str.regionMatches(true, strStartIndex, search, 0, search.length());
    }
}
