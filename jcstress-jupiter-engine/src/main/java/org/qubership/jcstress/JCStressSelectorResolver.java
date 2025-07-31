package org.qubership.jcstress;

import static java.util.Collections.singleton;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.junit.platform.engine.discovery.ClassSelector;
import org.junit.platform.engine.discovery.UniqueIdSelector;
import org.junit.platform.engine.support.discovery.SelectorResolver;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

class JCStressSelectorResolver implements SelectorResolver {
    private final Predicate<String> classNameFilter;

    JCStressSelectorResolver(Predicate<String> classNameFilter) {
        this.classNameFilter = classNameFilter;
    }

    @Override
    public Resolution resolve(ClassSelector selector, Context context) {
        if (!classNameFilter.test(selector.getClassName())) {
            return Resolution.unresolved();
        }
        if (!JCStressClassFilter.INSTANCE.test(selector.getJavaClass())) {
            // Gradle might supply extra classes, so we need to keep only jcstress-compatible classes here
            return Resolution.unresolved();
        }
        JCStressClassDescriptor classDescriptor =
                context.addToParent(
                                parent -> Optional.of(JCStressClassDescriptor.of(parent, selector.getJavaClass())))
                        .orElseThrow(IllegalStateException::new);
        return Resolution.match(Match.exact(classDescriptor));
    }

    @Override
    public Resolution resolve(UniqueIdSelector selector, Context context) {
        UniqueId uniqueId = selector.getUniqueId();
        List<Segment> segments = uniqueId.getSegments();
        for (int i = segments.size() - 1; i >= 0; i--) {
            Segment segment = segments.get(i);
            if (JCStressClassDescriptor.SEGMENT_TYPE.equals(segment.getType())) {
                return Resolution.selectors(singleton(selectClass(segment.getValue())));
            }
        }
        return Resolution.unresolved();
    }
}
