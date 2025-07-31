package org.qubership.jcstress;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class JCStressEngineDescriptor extends EngineDescriptor {
    private final TestDescriptorFactory testDescriptorFactory = new TestDescriptorFactory();
    private final Map<Class<?>, ClassDescriptor> classDescriptorsByTestClass = new HashMap<>();

    public JCStressEngineDescriptor(UniqueId uniqueId) {
        super(uniqueId, "jcstress");
    }

    TestDescriptorFactory getTestDescriptorFactory() {
        return testDescriptorFactory;
    }

    @Override
    public void addChild(TestDescriptor child) {
        ClassDescriptor classDescriptor = (ClassDescriptor) child;
        classDescriptorsByTestClass.put(classDescriptor.getTestClass(), classDescriptor);
        super.addChild(child);
    }

    @Override
    public void removeChild(TestDescriptor child) {
        classDescriptorsByTestClass.remove(((ClassDescriptor) child).getTestClass());
        super.removeChild(child);
    }

    private Stream<ClassDescriptor> classDescriptors() {
        return getChildren().stream().map(child -> (ClassDescriptor) child);
    }

    Class<?>[] getTestClasses() {
        return classDescriptors()
                .map(ClassDescriptor::getTestClass)
                .toArray(Class[]::new);
    }
}
