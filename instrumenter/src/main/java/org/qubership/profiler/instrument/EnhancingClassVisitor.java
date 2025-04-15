package org.qubership.profiler.instrument;

import static org.qubership.profiler.instrument.enhancement.EnhancerConstants.OPCODES_VERSION;

import org.qubership.profiler.instrument.enhancement.ClassInfo;
import org.qubership.profiler.instrument.enhancement.FilteredEnhancer;

import org.objectweb.asm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This visitor is used to enhance classes right after class was started.
 * ASM does support visitMethod/visitField calls outside of class body, however it is not compatible with COMPUTE_FRAMES
 */
public class EnhancingClassVisitor extends ClassVisitor {
    private final static Logger log = LoggerFactory.getLogger(EnhancingClassVisitor.class);

    private final List<FilteredEnhancer> enhancers;
    private final ClassInfo classInfo;

    public EnhancingClassVisitor(ClassVisitor cv, List<FilteredEnhancer> enhancers, ClassInfo classInfo) {
        super(OPCODES_VERSION, cv);
        this.enhancers = enhancers;
        this.classInfo = classInfo;
    }

    @Override
    public void visitEnd() {
        log.trace("visitEnd class={}", classInfo.getClassName());
        for (FilteredEnhancer filter : enhancers)
            if (filter.getFilter().accept(classInfo)) {
                if (log.isTraceEnabled())
                    log.trace("Applying enhancer {} to class {}, enhancer loaded in {}", new Object[]{filter, classInfo.getClassName(), filter.getFilter().getStackTraceAtCreate()});
                else
                    log.debug("Applying enhancer {} to class {}", filter, classInfo.getClassName());
                filter.enhance(cv);
            } else {
                if (log.isTraceEnabled())
                    log.trace("Enhancer {} does not match class {}, enhancer loaded in {}", new Object[]{filter, classInfo.getClassName(), filter.getFilter().getStackTraceAtCreate()});
                else
                    log.debug("Enhancer {} does not match class {}", filter, classInfo.getClassName());
            }
        super.visitEnd();
    }
}
