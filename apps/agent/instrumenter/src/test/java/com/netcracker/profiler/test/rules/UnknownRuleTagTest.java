package com.netcracker.profiler.test.rules;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.agent.plugins.EnhancerRegistryPluginImpl;
import com.netcracker.profiler.configuration.ConfigurationImpl;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Fails when a child of {@code <rule>} that names no selector and no action is dropped without a
 * word in the log.
 *
 * <p>A rule keeps every selector the parser did recognize, so a misspelled condition leaves the rule
 * matching more than its author asked for: {@code <if-class-doesnt-declare>} instead of
 * {@code <if-class-does-not-declare>} instruments the classes the condition was written to exclude.
 * Nothing else reports it, since the configuration is not validated against a schema.</p>
 */
public class UnknownRuleTagTest {
    @TempDir
    Path directory;

    @Test
    public void unknownChildOfARuleIsReported() throws Exception {
        Path config = write("<rule>\n"
                + "  <class>com.example.Sample</class>\n"
                + "  <if-class-doesnt-declare>run()</if-class-doesnt-declare>\n"
                + "</rule>\n");

        List<String> warnings = parseAndCollectWarnings(config);

        assertEquals(
                1,
                warnings.stream().filter(w -> w.contains("if-class-doesnt-declare")).count(),
                () -> "warnings naming the misspelled tag, among " + warnings);
    }

    @Test
    public void aRuleOfKnownTagsIsParsedWithoutWarnings() throws Exception {
        Path config = write("<rule>\n"
                + "  <class>com.example.Sample</class>\n"
                + "  <if-class-does-not-declare>run()</if-class-does-not-declare>\n"
                + "  <method>run()</method>\n"
                + "  <do-not-profile/>\n"
                + "</rule>\n");

        List<String> warnings = parseAndCollectWarnings(config);

        assertEquals(0, warnings.size(), () -> "warnings while parsing a rule of known tags: " + warnings);
    }

    private Path write(String rule) throws IOException {
        Path target = directory.resolve("rule.xml");
        Files.write(target, ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<profiler-configuration>\n<ruleset>\n" + rule + "</ruleset>\n</profiler-configuration>\n")
                .getBytes(UTF_8));
        return target;
    }

    private List<String> parseAndCollectWarnings(Path config) throws Exception {
        new EnhancerRegistryPluginImpl();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ConfigurationImpl.class);
        appender.start();
        logger.addAppender(appender);
        try {
            new ConfigurationImpl(config.toString());
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
        return appender.list.stream()
                .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                .map(ILoggingEvent::getFormattedMessage)
                .collect(toList());
    }
}
