package org.qubership.profiler.diagtools

import com.netcracker.profiler.test.agent.LogToConsolePrinter
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration

class ThreadDumpTest {
    @Test
    fun threadDump() {
        val binary = File(System.getProperty("diagtools.binary"))
        require(binary.isFile) { "diagtools binary not found at $binary" }

        GenericContainer("eclipse-temurin:21-jdk")
            .withLogConsumer(LogToConsolePrinter("[diagtools] "))
            .withEnv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "true")
            .withEnv("NC_DIAGNOSTIC_AGENT_SERVICE", "http://127.0.0.1:18080")
            .withEnv("NAMESPACE", "test-namespace")
            .withEnv("LOG_TO_CONSOLE", "true")
            .withStartupCheckStrategy(
                OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(30))
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath(binary.absolutePath, 0b111_101_101),
                "/usr/local/bin/diagtools"
            )
            .withCommand(
                "sh", "-c",
                // language=bash
                """
                mkdir -p /tmp/diagnostic/log /app/ncdiag
                cat > /tmp/UploadServer.java << 'JAVA'
                import com.sun.net.httpserver.HttpExchange;
                import com.sun.net.httpserver.HttpServer;
                import java.io.IOException;
                import java.net.InetSocketAddress;
                import java.nio.charset.StandardCharsets;

                public class UploadServer {
                    public static void main(String[] args) throws Exception {
                        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 18080), 0);
                        server.createContext("/", UploadServer::handle);
                        server.start();
                    }

                    private static void handle(HttpExchange exchange) throws IOException {
                        byte[] body = exchange.getRequestBody().readAllBytes();
                        System.out.println("===UPLOAD_BODY_START===");
                        System.out.print(new String(body, StandardCharsets.UTF_8));
                        System.out.println();
                        System.out.println("===UPLOAD_BODY_END===");
                        exchange.sendResponseHeaders(200, -1);
                        exchange.close();
                    }
                }
                JAVA
                cat > /tmp/Sleep.java << 'JAVA'
                public class Sleep {
                    public static void main(String[] args) throws Exception {
                        Thread.sleep(300000);
                    }
                }
                JAVA
                javac /tmp/UploadServer.java -d /tmp
                javac /tmp/Sleep.java -d /tmp
                java -cp /tmp UploadServer &
                java -cp /tmp Sleep &
                sleep 3
                diagtools dump
                if ls /tmp/diagnostic/*.td.txt >/dev/null 2>&1; then
                    echo '===LOCAL_FILE_EXISTS==='
                else
                    echo '===LOCAL_FILE_MISSING==='
                fi
                """.trimIndent()
            )
            .use { container ->
                container.start()

                val logs = container.logs
                val fileContent =
                    logs.substringAfter("===UPLOAD_BODY_START===").substringBefore("===UPLOAD_BODY_END===")
                assertTrue(fileContent.contains("Full thread dump")) {
                    """
                    Uploaded payload should contain a valid thread dump.
                    Upload content:
                    $fileContent
                    """.trimIndent()
                }
                assertTrue(fileContent.contains("JVM response code = 0")) {
                    """
                    Uploaded payload should contain successful jattach output.
                    Upload content: $fileContent
                    """.trimIndent()
                }
                assertTrue(logs.contains("===LOCAL_FILE_MISSING===")) {
                    "Thread dump file should not be created locally. Container logs:\n$logs"
                }
            }
    }
}
