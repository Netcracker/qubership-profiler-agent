package org.qubership.profiler.agent;

import java.lang.instrument.ClassDefinition;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;

public class InstrumentationImplForJava14 implements Instrumentation {
    private final ArrayList<ClassFileTransformer> transformers = new ArrayList<ClassFileTransformer>();

    public synchronized void addTransformer(ClassFileTransformer transformer, boolean canRetransform) {
        transformers.add(transformer);
    }

    public void addTransformer(ClassFileTransformer transformer) {
        addTransformer(transformer, false);
    }

    public boolean removeTransformer(ClassFileTransformer transformer) {
        return transformers.remove(transformer);
    }

    public ArrayList<ClassFileTransformer> getTransformers() {
        return transformers;
    }

    public boolean isRetransformClassesSupported() {
        return false;
    }

    public void retransformClasses(Class<?>... classes) throws UnmodifiableClassException {
        throw new IllegalStateException("Not implemented");
    }

    public boolean isRedefineClassesSupported() {
        return false;
    }

    public void redefineClasses(ClassDefinition... definitions) throws ClassNotFoundException, UnmodifiableClassException {
        throw new IllegalStateException("Not implemented");
    }

    public boolean isModifiableClass(Class<?> theClass) {
        return false;
    }

    public Class[] getAllLoadedClasses() {
        return new Class[0];
    }

    public Class[] getInitiatedClasses(ClassLoader loader) {
        return new Class[0];
    }

    public long getObjectSize(Object objectToSize) {
        return 0;
    }

    public void appendToBootstrapClassLoaderSearch(JarFile jarfile) {
        throw new IllegalStateException("Not implemented");
    }

    public void appendToSystemClassLoaderSearch(JarFile jarfile) {
        throw new IllegalStateException("Not implemented");
    }

    public boolean isNativeMethodPrefixSupported() {
        return false;
    }

    public void setNativeMethodPrefix(ClassFileTransformer transformer, String prefix) {
        throw new IllegalStateException("Not implemented");
    }

    //compilation errors are actually okay. If you build it with JDK11 and source level 1.7, it builds
    //and it doesn't build when you build it without these 2 methods
    @Override
    public void redefineModule(Module module, Set<Module> extraReads, Map<String, Set<Module>> extraExports, Map<String, Set<Module>> extraOpens, Set<Class<?>> extraUses, Map<Class<?>, List<Class<?>>> extraProvides) {

    }

    @Override
    public boolean isModifiableModule(Module module) {
        return false;
    }
}
