plugins {
    id("build-logic.profiler-published-plugin")
}

// No instrumentation test here: the Java agent stores its SDK classes as .classdata entries, which
// the test kit does not read. installer-zip-test runs this plugin with both agents attached.
