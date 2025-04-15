package org.qubership.profiler.config;

import org.qubership.profiler.io.WildcardFileFilter;
import org.qubership.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;

public class AnalyzerWhiteList {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzerWhiteList.class);
    private static final String CONFIG_FILE = System.getProperty("profiler.analyzer_white_list",
            Boolean.getBoolean("profiler_standalone_mode") ?
                    "config/analyzer_white_list.cfg" :
                    "applications/execution-statistics-collector/config/analyzer_white_list.cfg");
    private static FileFilter fileFilter;
    static {
        try {
            if(Boolean.getBoolean("profiler_local_start_mode")) {
                fileFilter = new TrueToALlFileFiler();
            } else {
                List<String> wildcards = loadWildcardsFromConfig();
                if(wildcards == null || wildcards.isEmpty()) {
                    fileFilter = new FalseToALlFileFiler();
                } else {
                    fileFilter = new WildcardFileFilter(wildcards);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in AnalyzerWhiteList: ", e);
            fileFilter = new FalseToALlFileFiler();
        }
    }

    private static List<String> loadWildcardsFromConfig() throws IOException {
        File configFile = new File(CONFIG_FILE);
        if(!configFile.exists()) {
            return null;
        }
        return IOHelper.readAllLinesFromFile(configFile);
    }

    private static class FalseToALlFileFiler implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return false;
        }
    }

    private static class TrueToALlFileFiler implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return true;
        }
    }

    public static boolean checkAccess(File file) {
        return fileFilter.accept(file);
    }

}
