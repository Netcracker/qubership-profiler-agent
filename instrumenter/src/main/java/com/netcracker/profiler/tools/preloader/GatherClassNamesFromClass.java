package com.netcracker.profiler.tools.preloader;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.Remapper;

import java.util.HashSet;

/**
 * The class gathers all the types.
 * It is based on ASM remapper that traverses class files
 */
public class GatherClassNamesFromClass extends Remapper {
    private HashSet<String> classNames;

    public GatherClassNamesFromClass(HashSet<String> classNames) {
        super(Opcodes.ASM9);
        this.classNames = classNames;
    }

    @Override
    public String map(String typeName) {
        classNames.add(typeName);
        return super.map(typeName);
    }
}
