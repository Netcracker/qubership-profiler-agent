package com.netcracker.jcstress;

import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;

class JCStressRunDescriptor extends AbstractTestDescriptor {
    static final String SEGMENT_TYPE = "run";

    JCStressRunDescriptor(UniqueId uniqueId) {
        super(uniqueId, "run");
    }

    static TestDescriptor of(UniqueId uniqueId) {
        return new JCStressRunDescriptor(uniqueId.append(SEGMENT_TYPE, "run"));
    }

    @Override
    public Type getType() {
        return Type.TEST;
    }
}
