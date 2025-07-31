package org.qubership.jcstress;

import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.ClassSource;

class ClassDescriptor extends AbstractTestDescriptor {
    static final String SEGMENT_TYPE = "class";
    private final Class<?> testClass;

    ClassDescriptor(UniqueId uniqueId, Class<?> testClass) {
        super(uniqueId, determineDisplayName(testClass), ClassSource.from(testClass));
        this.testClass = testClass;
    }

    private static String determineDisplayName(Class<?> testClass) {
        String simpleName = testClass.getSimpleName();
        return simpleName.isEmpty() ? testClass.getName() : simpleName;
    }

    @Override
    public String getLegacyReportingName() {
        return testClass.getName();
    }

    Class<?> getTestClass() {
        return testClass;
    }

    @Override
    public Type getType() {
        return Type.TEST;
    }
}
