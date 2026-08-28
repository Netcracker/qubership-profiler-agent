package com.netcracker.jcstress;

import org.junit.platform.commons.support.AnnotationSupport;
import org.openjdk.jcstress.annotations.JCStressTest;

import java.util.function.Predicate;

class JCStressClassFilter implements Predicate<Class<?>> {
    public static final Predicate<Class<?>> INSTANCE = new JCStressClassFilter();

    @Override
    public boolean test(Class<?> candidateClass) {
        return AnnotationSupport.findAnnotation(candidateClass, JCStressTest.class).isPresent();
    }
}
