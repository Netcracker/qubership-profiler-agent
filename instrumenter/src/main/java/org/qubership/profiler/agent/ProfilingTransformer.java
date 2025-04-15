package org.qubership.profiler.agent;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.qubership.profiler.agent.plugins.ConfigurationSPI;
import org.qubership.profiler.configuration.Rule;
import org.qubership.profiler.instrument.EnhancingClassVisitor;
import org.qubership.profiler.instrument.GatherRulesForMethodVisitor;
import org.qubership.profiler.instrument.ProfileClassAdapter;
import org.qubership.profiler.instrument.TypeUtils;
import org.qubership.profiler.instrument.custom.util.DefaultMethodAdder;
import org.qubership.profiler.instrument.enhancement.*;
import org.qubership.profiler.util.MethodInstrumentationInfo;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.SerialVersionUIDAdder;
import org.objectweb.asm.util.CheckClassAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class ProfilingTransformer implements ClassFileTransformer {
    private final static Logger log = LoggerFactory.getLogger(ProfilingTransformer.class);
    private volatile ConfigurationSPI conf;

    public ProfilingTransformer(ConfigurationSPI conf) {
        this.conf = conf;
    }

    public ConfigurationSPI getConfiguration() {
        return conf;
    }

    public void setConfiguration(ConfigurationSPI conf) {
        this.conf = conf;
    }

    public boolean transformRequired(String className) {
        Collection<Rule> rules = conf.getRulesForClass(className, null);
        final EnhancementRegistry enhancementRegistry = conf.getEnhancementRegistry();
        List<FilteredEnhancer> enhancers = enhancementRegistry.getEnhancers(className);
        return !rules.isEmpty() || !enhancers.isEmpty();
    }

    public byte[] transform(ClassLoader loader, String name, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        try {
            log.trace("Transformer called for class {}", name);
            Collection<Rule> rules = conf.getRulesForClass(name, null);
            final EnhancementRegistry enhancementRegistry = conf.getEnhancementRegistry();
            List<FilteredEnhancer> enhancers = enhancementRegistry.getEnhancers(name);
            List<DefaultMethodImplInfo> defaultMethods = conf.getDefaultMethods(name);

            if (rules.isEmpty() && enhancers.isEmpty() && defaultMethods.isEmpty())
                return null;
            ClassInfo classInfo = new ClassInfoImpl();
            classInfo.setClassName(name);
            classInfo.setProtectionDomain(protectionDomain);
            for (Iterator<Rule> it = rules.iterator(); it.hasNext(); ) {
                Rule rule = it.next();
                String ifEnhancer = rule.getIfEnhancer();
                if (ifEnhancer == null) {
                    continue;
                }
                EnhancerPlugin filter = (EnhancerPlugin) enhancementRegistry.getFilter(ifEnhancer);
                if (filter == null) {
                    log.warn("Filter {} is not found. Please check if-enhancer attributes", ifEnhancer);
                } else if (!filter.accept(classInfo)){
                    log.trace("Skipping rule {} since filter {} does not match class {}", new Object[]{rule, ifEnhancer, classInfo.getClassName()});
                    it.remove();
                }
            }

            ClassReader cr = new ClassReader(classfileBuffer);
            final HashMap<String, MethodInstrumentationInfo> selectedRules = new HashMap<String, MethodInstrumentationInfo>();

            if (!rules.isEmpty()) {
                ClassVisitor gatherVisitor = new GatherRulesForMethodVisitor(selectedRules, rules);
                gatherVisitor = addDefaultMethods(gatherVisitor, defaultMethods, enhancementRegistry, classInfo);

                cr.accept(gatherVisitor, ClassReader.SKIP_FRAMES);
            }

            ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);

            ClassVisitor cv = cw;

            // This will add $profiler methods and fields to the class
            if (!enhancers.isEmpty()) {
                // Merge static initializers
                cv = new StaticInitMerger("clinit$merger$profiler", cv);

                log.debug("Class {} will be updated by {} enhancers", name, enhancers.size());

                cv = new EnhancingClassVisitor(cv, enhancers, classInfo);
            }

            // This will actually insert enter/exit calls and execute-before/after/etc
            if (selectedRules.isEmpty()) {
                log.debug("No profiling rules match class {}", name);
            } else {
                cv = new ProfileClassAdapter(cv, name, selectedRules, TypeUtils.getJarName(protectionDomain));
            }

            cv = addDefaultMethods(cv, defaultMethods, enhancementRegistry, classInfo);

            // SVUID adder should receive original class as is to compute SVUID without $profiler methods and fields
            if (!enhancers.isEmpty()) {
                log.debug("Adding serialVersionUID to class {}", name);
                cv = new SerialVersionUIDAdder(cv);
            }

            // Frames are mandatory for java 1.7
            cr.accept(cv, ClassReader.EXPAND_FRAMES);

            final byte[] bytes = cw.toByteArray();
            final String path = conf.getStoreTransformedClassesPath();
            if (path != null) {
                storeTransformationResult(name, bytes, path);
                storeTransformationResult(name + "$$ESC$$ORIGINAL", classfileBuffer, path);
            }

            if (conf.isVerifyClassEnabled()) {
                ClassVisitor checker = new CheckClassAdapter(new ClassVisitor(OPCODES_VERSION) {
                    @Override
                    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                        return new MethodVisitor(OPCODES_VERSION) {
                        };
                    }
                });
                ClassReader reader = new ClassReader(bytes);
                reader.accept(checker, 0);
            }

            return bytes;
        } catch (RuntimeException e) {
            // logback might want to print jar name and version, so we convert throwable to string manually
            // avoid unexpected class loading
            log.warn("Unable to instrument class {}, {}", name, StringUtils.throwableToString(e));
            throw e;
        }
    }

    private static ClassVisitor addDefaultMethods(ClassVisitor cv, List<DefaultMethodImplInfo> defaultMethods, EnhancementRegistry enhancementRegistry, ClassInfo classInfo) {
        if (defaultMethods.isEmpty())
            return cv;
        for (DefaultMethodImplInfo defaultMethod : defaultMethods) {
            String ifEnhancer = defaultMethod.ifEnhancer;
            if (ifEnhancer != null) {
                EnhancerPlugin filter = (EnhancerPlugin) enhancementRegistry.getFilter(ifEnhancer);
                if (filter == null) {
                    log.warn("Filter {} is not found. Please check if-enhancer attributes", ifEnhancer);
                } else if (!filter.accept(classInfo)) {
                    log.debug("Skipped adding method {}{} to class {} since filter {} does not match", new Object[]{defaultMethod.methodName, defaultMethod.methodDescr, classInfo.getClassName(), ifEnhancer});
                    continue;
                }
            }
            cv = new DefaultMethodAdder(cv, defaultMethod);
        }
        return cv;
    }

    private void storeTransformationResult(String name, byte[] bytes, String path) {
        File out = new File(path, name + ".class");
        log.trace("Storing class {} to {}", name, out);
        final File parentFile = out.getParentFile();
        if (!parentFile.exists() && !parentFile.mkdirs())
            log.warn("Unable to create folders for class {}", name);

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(out);
            fos.write(bytes);
        } catch (FileNotFoundException e) {
            log.error("Unable to save class {}", name, e);
        } catch (IOException e) {
            log.error("Unable to save class {}", name, e);
        } finally {
            if (fos != null)
                try {
                    fos.close();
                } catch (IOException e) {
                     /**/
                }
        }
    }
}
