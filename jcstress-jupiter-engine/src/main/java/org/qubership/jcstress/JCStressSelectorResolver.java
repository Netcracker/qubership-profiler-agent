package org.qubership.jcstress;

import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.junit.platform.engine.discovery.ClassSelector;
import org.junit.platform.engine.discovery.UniqueIdSelector;
import org.junit.platform.engine.support.discovery.SelectorResolver;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Collections.singleton;
import static org.junit.platform.engine.discovery.DiscoverySelectors.*;

class JCStressSelectorResolver implements SelectorResolver {
    private final Predicate<String> classNameFilter;
    private final TestDescriptorFactory testDescriptorFactory;

    JCStressSelectorResolver(Predicate<String> classNameFilter, TestDescriptorFactory testDescriptorFactory) {
        this.classNameFilter = classNameFilter;
        this.testDescriptorFactory = testDescriptorFactory;
    }

    @Override
    public Resolution resolve(ClassSelector selector, Context context) {
        if (!classNameFilter.test(selector.getClassName())) {
            return Resolution.unresolved();
        }
        return context.addToParent(
                        parent -> Optional.of(testDescriptorFactory.createClassDescriptor(parent, selector.getJavaClass())))
                .map(classDescriptor -> Match.exact(classDescriptor, Collections::emptySet))
                .map(Resolution::match)
                .orElse(Resolution.unresolved());
    }

    @Override
    public Resolution resolve(UniqueIdSelector selector, Context context) {
        UniqueId uniqueId = selector.getUniqueId();
        Segment lastSegment = uniqueId.getLastSegment();
        if (ClassDescriptor.SEGMENT_TYPE.equals(lastSegment.getType())) {
            return Resolution.selectors(singleton(selectClass(lastSegment.getValue())));
        }
        return Resolution.unresolved();
    }
}
