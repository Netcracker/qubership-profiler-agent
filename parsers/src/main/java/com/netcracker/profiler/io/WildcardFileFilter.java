package com.netcracker.profiler.io;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

public class WildcardFileFilter implements FileFilter {

    private static final String CURRENT_DIR = removeLastChar(new File(".").getAbsolutePath());
    private final List<String> wildcards;

    public WildcardFileFilter(List<String> wildcards) {
        if (wildcards == null) {
            throw new IllegalArgumentException("The wildcard array must not be null");
        } else {
            this.wildcards = new ArrayList<>(wildcards.size());
            for(String wildcard : wildcards) {
                if(wildcard.startsWith("/") || wildcard.startsWith(CURRENT_DIR)) {
                    this.wildcards.add(wildcard);
                } else {
                    this.wildcards.add(CURRENT_DIR + wildcard);
                }
            }
        }
    }

    @Override
    public boolean accept(File path) {
        if(path == null) {
            return false;
        }
        String pathName = path.getAbsolutePath();

        for(String wildcard : wildcards) {
            if (FileNameUtils.wildcardMatch(pathName, wildcard)) {
                return true;
            }
        }

        return false;
    }

    private static String removeLastChar(String str) {
        return str.substring(0, str.length()-1);
    }
}
