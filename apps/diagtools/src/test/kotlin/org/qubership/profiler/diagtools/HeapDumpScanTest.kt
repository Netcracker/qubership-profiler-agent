package org.qubership.profiler.diagtools

import com.netcracker.profiler.test.agent.LogToConsolePrinter
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration

class HeapDumpScanTest {
    @Test
    fun hprofRemovedAfterScan() {
        val binary = File(System.getProperty("diagtools.binary"))
        require(binary.isFile) { "diagtools binary not found at $binary" }

        GenericContainer("eclipse-temurin:21-jdk")
            .withLogConsumer(LogToConsolePrinter("[diagtools] "))
            .withEnv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "true")
            .withEnv("NC_DIAGNOSTIC_AGENT_SERVICE", "http://127.0.0.1:18080")
            .withEnv("NAMESPACE", "test-namespace")
            .withEnv("LOG_TO_CONSOLE", "true")
            .withStartupCheckStrategy(
                OneShotStartupCheckStrategy().withTimeout(Duration.ofSeconds(120))
            )
            .withCopyFileToContainer(
                MountableFile.forHostPath(binary.absolutePath, 0b111_101_101),
                "/usr/local/bin/diagtools"
            )
            .withCopyToContainer(
                Transferable.of(
                    // language=java
                    """
                    import com.sun.net.httpserver.HttpExchange;
                    import com.sun.net.httpserver.HttpServer;
                    import java.io.IOException;
                    import java.net.InetSocketAddress;
                    import java.util.concurrent.atomic.AtomicInteger;

                    public class UploadServer {
                        static final AtomicInteger uploadCount = new AtomicInteger();
                        public static void main(String[] args) throws Exception {
                            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 18080), 0);
                            server.createContext("/", UploadServer::handle);
                            server.start();
                        }

                        private static void handle(HttpExchange exchange) throws IOException {
                            byte[] body = exchange.getRequestBody().readAllBytes();
                            int n = uploadCount.incrementAndGet();
                            System.out.println("===UPLOAD_" + n + "_SIZE=" + body.length + "===");
                            exchange.sendResponseHeaders(200, -1);
                            exchange.close();
                        }
                    }
                    """.trimIndent(),
                ),
                "/tmp/UploadServer.java"
            )
            .withCopyToContainer(
                Transferable.of(
                    // language=java
                    """
                    public class AllocateMemory {
                        // Hold a reference so objects are not collected before heap dump
                        static byte[][] holder;
                        public static void main(String[] args) throws Exception {
                            // Allocate ~2MB so the heap dump has some content
                            holder = new byte[20][];
                            for (int i = 0; i < holder.length; i++) {
                                holder[i] = new byte[100_000];
                            }
                            Thread.sleep(300000);
                        }
                    }
                    """.trimIndent(),
                ),
                "/tmp/AllocateMemory.java"
            )
            .withCommand(
                "sh", "-c",
                // language=bash
                """
                mkdir -p /tmp/diagnostic/log /app/ncdiag
                javac /tmp/UploadServer.java -d /tmp
                javac /tmp/AllocateMemory.java -d /tmp
                java -cp /tmp UploadServer &
                java -Xmx32m -cp /tmp AllocateMemory &
                JAVA_PID=$!
                sleep 3

                # Take heap dump via diagtools
                diagtools heap zip upload

                echo "===AFTER_HEAP_DUMP==="
                echo "hprof_count=$(ls /tmp/diagnostic/*.hprof 2>/dev/null | wc -l | tr -d ' ')"
                echo "zip_count=$(ls /tmp/diagnostic/*.hprof.zip 2>/dev/null | wc -l | tr -d ' ')"

                # Run scan to verify it does not re-upload the same dump
                diagtools scan "/tmp/diagnostic/*.hprof*"

                echo "===AFTER_SCAN==="
                echo "hprof_count=$(ls /tmp/diagnostic/*.hprof 2>/dev/null | wc -l | tr -d ' ')"
                echo "zip_count=$(ls /tmp/diagnostic/*.hprof.zip 2>/dev/null | wc -l | tr -d ' ')"
                """.trimIndent()
            )
            .use { container ->
                container.start()

                val logs = container.logs

                // After heap zip upload: .hprof should be removed (ZipDump deletes it),
                // .hprof.zip should also be removed (heapdump.go deletes after upload)
                val afterHeapDump = logs.substringAfter("===AFTER_HEAP_DUMP===")
                    .substringBefore("===AFTER_SCAN===")
                assertTrue(afterHeapDump.contains("hprof_count=0")) {
                    "After 'heap zip upload', .hprof original should be deleted.\n$afterHeapDump"
                }
                assertTrue(afterHeapDump.contains("zip_count=0")) {
                    "After 'heap zip upload', .hprof.zip should be deleted.\n$afterHeapDump"
                }

                // Verify at least one upload happened
                assertTrue(logs.contains("===UPLOAD_1_SIZE=")) {
                    "Expected at least one upload to the server.\nContainer logs:\n$logs"
                }

                // After scan: nothing to find, no re-upload
                val afterScan = logs.substringAfter("===AFTER_SCAN===")
                assertTrue(afterScan.contains("hprof_count=0")) {
                    "After scan, no .hprof files should remain.\n$afterScan"
                }
                assertTrue(afterScan.contains("zip_count=0")) {
                    "After scan, no .hprof.zip files should remain.\n$afterScan"
                }

                // Verify no second upload happened (scan should find nothing)
                assertTrue(!logs.contains("===UPLOAD_2_SIZE=")) {
                    "Scan should not cause a second upload — all files were already cleaned up.\nContainer logs:\n$logs"
                }
            }
    }
}
