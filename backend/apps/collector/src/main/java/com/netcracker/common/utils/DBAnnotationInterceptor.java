package com.netcracker.common.utils;

import io.micrometer.core.instrument.*;
import io.quarkus.logging.Log;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import java.time.Duration;
import java.util.Collection;

@DB
@Priority(2020)
@Interceptor
public class DBAnnotationInterceptor {

    @Inject
    MeterRegistry registry;

    @AroundInvoke
    Object logInvocation(InvocationContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        String clazz = context.getMethod().getDeclaringClass().getSimpleName();
        String method = context.getMethod().getName();
        String name = clazz+"."+method;

        var tag = context.getMethod().getAnnotation(DB.class);
        if (tag != null && tag.value().length() > 0) {
            name = tag.value();
        }

        var resSummary = DistributionSummary
                .builder("db.results.summary")
                .tags("name", name, "class", clazz, "method", method)
                .serviceLevelObjectives(1, 10, 100, 1000, 10000) // items
                .register(registry);

        Timer timer = Timer.builder("db.timer")
                .tags("name", name, "class", clazz, "method", method)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram(false)
                .serviceLevelObjectives(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(500),
                        Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10))
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(60))
                .register(registry);
        LongTaskTimer.Sample ls = registry.more().longTaskTimer("db.timer.current",
                "name", name, "class", clazz, "method", method).start();

//        Class<?> returnType = context.getMethod().getReturnType();
//        if (Uni.class.isAssignableFrom(returnType)) {
////            try {
////                return ((Uni<Object>) context.proceed()).onTermination().invoke(
////                        new Functions.TriConsumer<>() {
////                            @Override
////                            public void accept(Object o, Throwable throwable, Boolean cancelled) {
////                                stop(samples, exception(throwable));
////                            }
////                        });
////            } catch (Exception ex) {
////                ls.stop();
////                stop(samples, exception(ex));
////                throw ex;
////            }
//        }

        Log.tracef("[DB][%s.%s()] executing...", clazz, method);
        Object ret;
        try {
            ret = timer.recordCallable(context::proceed);

            String val = "1 item";
            if (ret == null) {
                val = "void";
            } else if (ret instanceof Collection<?> c) {
                int size = c.size();
                val = String.format("%d items", size);
                resSummary.record(size);
            } else {
                resSummary.record(1);
            }

            var ts = System.currentTimeMillis() - startTime;
            if (ts > 800) { // 0.8s
                Log.debugf("[DB][%s.%s()] Result (%s) in %d ms", clazz, method, val, ts);
            } else if (ts > 0) {
                Log.tracef("[DB][%s.%s()] Result (%s) in %d ms", clazz, method, val, ts);
            }
        } catch (Exception e) {
            var ts = System.currentTimeMillis() - startTime;
            String errorName = e.getClass().getSimpleName();

            Log.errorf(e, "[DB][%s.%s()] Exception (%s) in %d ms", clazz, method, errorName, ts); // for debug

            var c = Counter.builder("db.errors").
                    tags("name", name, "class", clazz, "method", method, "exception", errorName)
                    .register(registry);
            c.increment();

            throw e;
        } finally {
            ls.stop();
        }
        return ret;
    }

}