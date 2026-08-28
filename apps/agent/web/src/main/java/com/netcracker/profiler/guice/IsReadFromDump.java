package com.netcracker.profiler.guice;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Qualifier annotation for the isReadFromDump configuration String.
 * Replaces @Named("IS_READ_FROM_DUMP")
 */
@BindingAnnotation
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface IsReadFromDump {
}
