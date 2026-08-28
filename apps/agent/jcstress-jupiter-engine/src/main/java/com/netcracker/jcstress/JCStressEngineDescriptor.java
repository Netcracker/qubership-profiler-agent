package com.netcracker.jcstress;

import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.EngineDescriptor;

public class JCStressEngineDescriptor extends EngineDescriptor {
    public JCStressEngineDescriptor(UniqueId uniqueId) {
        super(uniqueId, "jcstress");
    }
}
