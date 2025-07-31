package org.qubership.jcstress;

import org.junit.platform.engine.*;
import org.junit.platform.engine.support.discovery.EngineDiscoveryRequestResolver;
import org.openjdk.jcstress.JCStress;
import org.openjdk.jcstress.Options;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public class JCStressTestEngine implements TestEngine {
    private static final EngineDiscoveryRequestResolver<JCStressEngineDescriptor> DISCOVERY_REQUEST_RESOLVER =
            EngineDiscoveryRequestResolver.<JCStressEngineDescriptor> builder()
            .addClassContainerSelectorResolver(new IsJCStressTestClass())
            .addSelectorResolver(ctx -> new JCStressSelectorResolver(ctx.getClassNameFilter(),
                    ctx.getEngineDescriptor().getTestDescriptorFactory()))
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
            List<String> opts = new ArrayList<>();
            opts.add("-t");
            StringJoiner sj = new StringJoiner("|", "(?>", ")");
            for (TestDescriptor child : engineDescriptor.getChildren()) {
                if (child instanceof ClassDescriptor) {
                    sj.add(Pattern.quote(((ClassDescriptor) child).getTestClass().getName()));
                }
            }
            opts.add(sj.toString());
            Options options = new Options(opts.toArray(new String[0]));
            options.parse();
            JCStress jcStress = new JCStress(options);
            jcStress.run();
        } catch (Exception e) {
            listener.executionFinished(engineDescriptor, TestExecutionResult.failed(e));
        }
    }
}
