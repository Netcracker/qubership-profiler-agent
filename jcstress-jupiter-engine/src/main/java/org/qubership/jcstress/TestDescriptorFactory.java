package org.qubership.jcstress;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;

class TestDescriptorFactory {
    ClassDescriptor createClassDescriptor(TestDescriptor parent, Class<?> testClass) {
        UniqueId uniqueId = parent.getUniqueId().append(ClassDescriptor.SEGMENT_TYPE, testClass.getName());
        return new ClassDescriptor(uniqueId, testClass);
    }
}
