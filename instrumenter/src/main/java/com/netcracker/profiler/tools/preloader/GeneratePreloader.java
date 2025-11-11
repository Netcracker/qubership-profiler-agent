package com.netcracker.profiler.tools.preloader;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.commons.ClassRemapper;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class gathers all the classes used in the jar
 */
public class GeneratePreloader {
    HashSet<String> classNames = new HashSet<String>();
    private final String jarName;
    private final String destName;

    public GeneratePreloader(String jarName, String destName) {
        this.jarName = jarName;
        this.destName = destName;
    }

    public static void main(String[] args) throws FileNotFoundException {
        GeneratePreloader preloader = new GeneratePreloader(args[0], args[1]);
        preloader.run();
    }

    public void run() {
        gatherClassNames();
        generatePreloader();
    }

    private void generatePreloader() {
        String[] classes = classNames.toArray(new String[classNames.size()]);
        Arrays.sort(classes);
        try (PrintWriter w = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(destName), "UTF-8")))) {
            for (String s : classes) {
                if (s.length() > 3 && s.charAt(0) == 'L' && s.charAt(s.length() - 1) == ';')
                    s = s.substring(1, s.length() - 1);
                w.println(s.replace('/', '.'));
            }
        } catch (IOException e) {
            System.err.println("Unable to generate preloader " + destName);
            e.printStackTrace();
        }
    }

    private void gatherClassNames() {
        ZipEntry entry;
        try (ZipInputStream is = new ZipInputStream(new FileInputStream(jarName))) {
            GatherClassNamesFromClass remapper = new GatherClassNamesFromClass(classNames);
            ClassRemapper remap = new ClassRemapper(null, remapper);
            while ((entry = is.getNextEntry()) != null) {
                if (entry.isDirectory()) continue;
                if (entry.getName() == null || !entry.getName().endsWith(".class")) continue;
                ClassReader cr = new ClassReader(is);
                cr.accept(remap, ClassReader.SKIP_DEBUG);
            }
        } catch (IOException e) {
            System.err.println("Unable to process jar " + jarName);
            e.printStackTrace();
        }
    }
}
