package com.netcracker.jcstress;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;

class JCStressClassDescriptor extends AbstractTestDescriptor {
    static final String SEGMENT_TYPE = "class";

    private JCStressClassDescriptor(UniqueId uniqueId, Class<?> testClass) {
        super(uniqueId, determineDisplayName(testClass), ClassSource.from(testClass));
        // Gradle expects Engine -> Container -> Test hierarchy, so we add a dummy "run" descriptor here.
        // See https://github.com/junit-team/junit-framework/discussions/4825
        // It would be great to get "run" descriptors from JCStress itself, but it does not support custom listeners yet
        addChild(JCStressRunDescriptor.of(uniqueId));
    }

    static JCStressClassDescriptor of(TestDescriptor parent, Class<?> testClass) {
        UniqueId uniqueId = parent.getUniqueId().append(JCStressClassDescriptor.SEGMENT_TYPE, testClass.getName());
        return new JCStressClassDescriptor(uniqueId, testClass);
    }

    private static String determineDisplayName(Class<?> testClass) {
        String simpleName = testClass.getSimpleName();
        return simpleName.isEmpty() ? testClass.getName() : simpleName;
    }

    Class<?> getTestClass() {
        return ((ClassSource) getSource().get()).getJavaClass();
    }

    @Override
    public Type getType() {
        return Type.CONTAINER;
    }
}
