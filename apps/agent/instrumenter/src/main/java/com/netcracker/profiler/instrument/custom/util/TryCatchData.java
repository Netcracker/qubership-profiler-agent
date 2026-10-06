package com.netcracker.profiler.instrument.custom.util;

import org.objectweb.asm.Label;

public class TryCatchData {
    private final Label startTry = new Label();
    private final Label endTry = new Label();
    private final Label startCatch = new Label();
    private final Label endCatch = new Label();

    public Label getStartTry() {
        return startTry;
    }

    public Label getEndTry() {
        return endTry;
    }

    public Label getStartCatch() {
        return startCatch;
    }

    public Label getEndCatch() {
        return endCatch;
    }
}
