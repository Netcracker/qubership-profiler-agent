package com.netcracker.profiler.cli;

import net.sourceforge.argparse4j.inf.Namespace;

/**
 * CLI command that parses arguments.
 */
public interface Command {
    int accept(Namespace args);
}
