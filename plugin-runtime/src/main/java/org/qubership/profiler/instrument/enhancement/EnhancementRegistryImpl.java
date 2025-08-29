package org.qubership.profiler.instrument.enhancement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EnhancementRegistryImpl implements org.qubership.profiler.agent.EnhancementRegistry {
    public static final Logger log = LoggerFactory.getLogger(EnhancementRegistryImpl.class);
    Map<String, Object> enhancers = new HashMap<String, Object>();
    Map<String, Object/*EnhancerPlugin*/> filtersByName = new HashMap<String, Object>();

    public List getEnhancers(String className) {
        final Object o = enhancers.get(className);
        if (o == null) return Collections.EMPTY_LIST;
        if (o instanceof List) return (List) o;
        return Collections.singletonList(o);
    }

    public void addEnhancer(String className, Object filteredEnhancer) {
        FilteredEnhancer enhancer = (FilteredEnhancer) filteredEnhancer;
        Object o = enhancers.get(className);
        if (o == null) {
            enhancers.put(className, enhancer);
            return;
        }
        if (o instanceof List) {
            List list = (List) o;
            if (list.contains(filteredEnhancer)) {
                if (log.isDebugEnabled())
                    log.debug("Attempt to put the same enhancer twice for the class {}", className, new Throwable());
                else
                    log.debug("Attempt to put the same enhancer twice for the class {}", className);
                return;
            }
            list.add(filteredEnhancer);
            return;
        }
        if (o == filteredEnhancer) {
            if (log.isDebugEnabled())
                log.debug("Attempt to put the same enhancer twice for the class {}", className, new Throwable());
            else
                log.debug("Attempt to put the same enhancer twice for the class {}", className);
            return;
        }

        List<FilteredEnhancer> list = new ArrayList<FilteredEnhancer>();
        list.add((FilteredEnhancer) o);
        list.add(enhancer);
        enhancers.put(className, list);
    }

    public Object/*EnhancerPlugin*/ getFilter(String name) {
        return filtersByName.get(name);
    }

    public void addFilter(String name, Object/*EnhancerPlugin*/ filter) {
        filtersByName.put(name, filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EnhancementRegistryImpl that = (EnhancementRegistryImpl) o;

        if (enhancers != null ? !enhancers.equals(that.enhancers) : that.enhancers != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return enhancers != null ? enhancers.hashCode() : 0;
    }
}
