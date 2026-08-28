package com.netcracker.profiler.test.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.netcracker.profiler.util.VariableFinder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.stream.Stream;


public class TestVariableFinder {
    String cloudNamespace;
    String namespace;
    String microServiceName;
    String serviceName;

    @BeforeEach
    public void beforeTest() {
        cloudNamespace = System.getProperty("CLOUD_NAMESPACE");
        namespace = System.getProperty("NAMESPACE");
        microServiceName = System.getProperty("MICROSERVICE_NAME");
        serviceName = System.getProperty("SERVICE_NAME");
    }

    //============================= NameSpace Tests ========================================
    // To receive namespace property the following order for fallback should be followed:
    // 1. Try to get namespace from CLOUD_NAMESPACE system property for backward compatibility
    // 2. Try to get namespace from NAMESPACE system property
    // 3. Try to get namespace from kubernetes namespace file

    // Test for 1-st step in fallback order
    @Test
    public void testGetCloudNamespace() {
        System.setProperty("CLOUD_NAMESPACE", "cloud-namespace-profiler");
        System.clearProperty("NAMESPACE");

        assertEquals("cloud-namespace-profiler", VariableFinder.getNamespace());
    }

    // Test for 2-nd step in fallback order
    @Test
    public void testGetNamespace() {
        System.clearProperty("CLOUD_NAMESPACE");
        System.setProperty("NAMESPACE", "namespace-profiler");

        assertEquals("namespace-profiler", VariableFinder.getNamespace());
    }

    // Test for check if order is being correctly applied
    @Test
    public void testGetCloudNamespaceWithNamespace() {
        System.setProperty("CLOUD_NAMESPACE", "cloud-namespace-profiler");
        System.setProperty("NAMESPACE", "namespace-profiler");

        assertEquals("cloud-namespace-profiler", VariableFinder.getNamespace());
    }

    // Test for 3-rd step in fallback order
    @Test
    public void testGetNamespaceFile() {
        System.clearProperty("CLOUD_NAMESPACE");
        System.clearProperty("NAMESPACE");

        // Mocked getNamespaceFromFile method to avoid test from interact with file system of environment
        try (MockedStatic<VariableFinder> variableFinderMockedStatic = Mockito.mockStatic(VariableFinder.class)) {
            MockedStatic.Verification getNamespaceMethod = new MockedStatic.Verification() {
                @Override
                public void apply() {
                    VariableFinder.getNamespace();
                }
            };
            MockedStatic.Verification getNamespaceFromFile = new MockedStatic.Verification() {
                @Override
                public void apply() {
                    VariableFinder.getNamespaceFromFile();
                }
            };
            variableFinderMockedStatic.when(getNamespaceMethod).thenCallRealMethod();
            variableFinderMockedStatic.when(getNamespaceFromFile).thenReturn("namespace-file-profiler");

            assertEquals("namespace-file-profiler", VariableFinder.getNamespace());
        }

    }

    //============================= ServiceName Tests ========================================
    // To receive service_name property the following order for fallback should be followed:
    // 1. Try to get service_name from MICROSERVICE_NAME system property for backward compatibility
    // 2. Try to get service_name from SERVICE_NAME system property
    // 3. Try to get service_name from pod_name through regular expression


    // Test for 1-st step in fallback order
    @Test
    public void testGetMicroserviceName() {
        System.setProperty("MICROSERVICE_NAME", "microservice-name-profiler");
        System.clearProperty("SERVICE_NAME");

        assertEquals("microservice-name-profiler", VariableFinder.getServicename());
    }

    // Test for 2-nd step in fallback order
    @Test
    public void testGetServiceName() {
        System.clearProperty("MICROSERVICE_NAME");
        System.setProperty("SERVICE_NAME", "service-name-profiler");

        assertEquals("service-name-profiler", VariableFinder.getServicename());
    }

    // Test for check if order is being correctly applied
    @Test
    public void testGetMicroServiceNameWithServiceName() {
        System.setProperty("MICROSERVICE_NAME", "microservice-name-profiler");
        System.setProperty("SERVICE_NAME", "service-name-profiler");

        assertEquals("microservice-name-profiler", VariableFinder.getServicename());
    }

    // Test for 3-rd step in fallback order
    @Test
    public void testGetServiceNameFromRegEx() {
        System.clearProperty("MICROSERVICE_NAME");
        System.clearProperty("SERVICE_NAME");
        // Mocked getServiceName method to avoid interact with logic behind getting SERVER_NAME property
        try (MockedStatic<VariableFinder> variableFinderMockedStatic = Mockito.mockStatic(VariableFinder.class)) {
            MockedStatic.Verification getServiceNameMethod = new MockedStatic.Verification() {
                @Override
                public void apply() {
                    VariableFinder.getServicename();
                }
            };
            MockedStatic.Verification getServicenameFromRegEx = new MockedStatic.Verification() {
                @Override
                public void apply() {
                    VariableFinder.getServicenameFromRegEx(VariableFinder.getServerName());
                }
            };
            MockedStatic.Verification getServerName = new MockedStatic.Verification() {
                @Override
                public void apply() {
                    VariableFinder.getServerName();
                }
            };

            variableFinderMockedStatic.when(getServerName).thenReturn("events-worker-7c9b7bdc55-f7sgc");
            variableFinderMockedStatic.when(getServiceNameMethod).thenCallRealMethod();
            variableFinderMockedStatic.when(getServicenameFromRegEx).thenCallRealMethod();

            assertEquals("events-worker", VariableFinder.getServicename());
        }
    }

    static Stream<Arguments> testRegExParserData() {
        return Stream.of(
                arguments("events-worker-7c9b7bdc55-f7sgc", "events-worker"),
                arguments("notification-585f6b94b8-t4jjc", "notification"),
                arguments("report-generator-749ccf648d-gd9j7", "report-generator"),
                arguments("notification-585f6b94b8-t4jjc", "notification"),
                arguments("planning-manager-v1-76c876bf-pfmcq", "planning-manager-v1"),
                arguments("sink-processor-ci-cdi-v1-9c48bfb97-f99lp", "sink-processor-ci-cdi-v1"),
                arguments("number-manager-v1-c45bfb5d6-w5xcp", "number-manager-v1"),
                arguments("resource-discrepancy-manager-v1-96f78c559-pwzxl", "resource-discrepancy-manager-v1"),
                arguments("static-content-8445d7f556-wbxvp", "static-content"),
                arguments("grafana-deployment-7778574c6c-lz7tb", "grafana-deployment"),
                arguments("kube-state-metrics-7dc5684445-dr528", "kube-state-metrics"),
                arguments("bgp-ls-provider-v1-6bd677bbd6-hm2rt", "bgp-ls-provider-v1"),
                arguments("cassandra-operator-6df6797d96-xfqds", "cassandra-operator"),
                arguments("idp-76c9bd9f99-5hdxt", "idp"),
                arguments("dbaas-opensearch-adapter-64ffbff5dd-rtb5n", "dbaas-opensearch-adapter"),
                arguments("opensearch-integration-tests-66546c5956-c7mrv", "opensearch-integration-tests"),
                // Job/CronJob names
                arguments("init-database-fm44h", "init-database"),
                // StatefulSet names
                arguments("prometheus-k8s-0", "prometheus-k8s"),
                arguments("cassandra0-0", "cassandra0"),
                arguments("opensearch-0", "opensearch"),
                // DaemonSet names
                arguments("node-exporter-vgbx5", "node-exporter")
        );
    }

    // Regular expression part testing
    @ParameterizedTest
    @MethodSource("testRegExParserData")
    public void testRegExParser(String input, String expected) {
        assertEquals(expected, VariableFinder.getServicenameFromRegEx(input));
    }

    static Stream<Arguments> testRegExParserInvalidData() {
        return Stream.of(
                arguments("events", null),
                arguments("notification-585f6b94b8", "notification"),
                arguments("report-generator-v1-gd9j7", "report-generator-v1"),
                arguments("test.example.com:17004", null)
        );
    }

    // Testing invalid data for Regular expression
    @ParameterizedTest
    @MethodSource("testRegExParserInvalidData")
    public void testRegExParserInvalid(String input, String expected) {
        assertEquals(expected, VariableFinder.getServicenameFromRegEx(input));
    }

    @AfterEach
    public void afterTest() {
        if (cloudNamespace != null) System.setProperty("CLOUD_NAMESPACE", cloudNamespace);
        if (namespace != null) System.setProperty("NAMESPACE", namespace);
        if (microServiceName != null) System.setProperty("MICROSERVICE_NAME", microServiceName);
        if (serviceName != null) System.setProperty("SERVICE_NAME", serviceName);
    }
}
