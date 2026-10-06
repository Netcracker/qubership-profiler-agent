package com.netcracker.jcstress;

import org.junit.platform.engine.*;
import org.junit.platform.engine.support.discovery.EngineDiscoveryRequestResolver;
import org.openjdk.jcstress.JCStress;
import org.openjdk.jcstress.Options;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class JCStressTestEngine implements TestEngine {
    private static final EngineDiscoveryRequestResolver<JCStressEngineDescriptor> DISCOVERY_REQUEST_RESOLVER =
            EngineDiscoveryRequestResolver.<JCStressEngineDescriptor>builder()
                    .addClassContainerSelectorResolver(JCStressClassFilter.INSTANCE)
                    .addSelectorResolver(ctx ->
                            new JCStressSelectorResolver(ctx.getClassNameFilter()))
                    .build();

    @Override
    public String getId() {
        return "jcstress";
    }

    @Override
    public TestDescriptor discover(EngineDiscoveryRequest request, UniqueId uniqueId) {
        JCStressEngineDescriptor engineDescriptor = new JCStressEngineDescriptor(uniqueId);
        DISCOVERY_REQUEST_RESOLVER.resolve(request, engineDescriptor);
        return engineDescriptor;
    }

    @Override
    public void execute(ExecutionRequest request) {
        EngineExecutionListener listener = request.getEngineExecutionListener();
        JCStressEngineDescriptor engineDescriptor = (JCStressEngineDescriptor) request.getRootTestDescriptor();
        listener.executionStarted(engineDescriptor);

        try {
            // Start execution for each test class "container"
            for (TestDescriptor classDescriptor : engineDescriptor.getChildren()) {
                listener.executionStarted(classDescriptor);
                try {
                    // We have a stub "run" under each test container, otherwise Gradle doesn't recognize the test
                    for (TestDescriptor run : classDescriptor.getChildren()) {
                        listener.executionStarted(run);
                    }
                    try {
                        Class<?> testClass = ((JCStressClassDescriptor) classDescriptor).getTestClass();
                        executeJCStress(Pattern.quote(testClass.getName()));
                        for (TestDescriptor run : classDescriptor.getChildren()) {
                            listener.executionFinished(run, TestExecutionResult.successful());
                        }
                    } catch (Throwable e) {
                        for (TestDescriptor run : classDescriptor.getChildren()) {
                            listener.executionFinished(run, TestExecutionResult.failed(e));
                        }
                    }
                    listener.executionFinished(classDescriptor, TestExecutionResult.successful());
                } catch (Throwable e) {
                    listener.executionFinished(classDescriptor, TestExecutionResult.failed(e));
                }
            }
            // Individual test failures are propagate automatically, so we just need to confirm the engine completed
            listener.executionFinished(engineDescriptor, TestExecutionResult.successful());
        } catch (Throwable e) {
            listener.executionFinished(engineDescriptor, TestExecutionResult.failed(e));
        }
    }

    private void executeJCStress(String classNamePattern) throws Exception {
        List<String> opts = new ArrayList<>();
        Path resultsDir = Paths.get("build", "jcstress", "results");
        resultsDir.toFile().mkdirs();
        opts.add("-m");
        opts.add("quick");
        opts.add("-r");
        opts.add(resultsDir.toString());
        opts.add("-t");
        opts.add(classNamePattern);
        Options options = new Options(opts.toArray(new String[0]));
        options.parse();
        // There's no way to configure the results directory for now, let's use reflection for it
        Field resultFile = options.getClass().getDeclaredField("resultFile");
        resultFile.setAccessible(true);
        resultFile.set(options, resultsDir.resolve(options.getResultFile()).toString());
        JCStress jcStress = new JCStress(options);
        try {
            jcStress.run();
        } finally {
            // Delete the result for now. We do not use it, and we do not want for the files to pile up
            Files.deleteIfExists(Paths.get(options.getResultFile()));
        }
    }
}
