# Installer ZIP Test

This module verifies the functionality of the `installer.zip` distribution by running tests with the
`-javaagent:.../qubership-profiler-agent.jar` parameter. It validates:

- Proper extraction and packaging of profiler components from `installer.zip`
- Correct loading of configuration files and jar files in a production-like environment with Java agent enabled

The tests ensure the profiler works correctly when loaded as a Java agent, simulating real application usage.
