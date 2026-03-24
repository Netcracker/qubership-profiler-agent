package dictionary

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {
	t.Run("line", func(t *testing.T) {
		t.Run("parameters", func(t *testing.T) {
			s := "log.generated"
			data, ok := Parse(s)
			assert.True(t, ok)
			assert.False(t, data.isMethod)

			s = "brave.trace_id"
			data, ok = Parse(s)
			assert.True(t, ok)
			assert.False(t, data.isMethod)

			s = "x-request-id"
			data, ok = Parse(s)
			assert.True(t, ok)
			assert.False(t, data.isMethod)

			s = "sql"
			data, ok = Parse(s)
			assert.True(t, ok)
			assert.False(t, data.isMethod)
		})

		t.Run("methods", func(t *testing.T) {
			t.Run("in libraries jars", func(t *testing.T) {

				s := "java.lang.Object org.springframework.beans.factory.support.DefaultListableBeanFactory.getBean(java.lang.Class,java.lang.Object[]) (DefaultListableBeanFactory.java:348) [BOOT-INF/lib/spring-beans-5.3.27.jar]"
				data, ok := Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "spring-beans-5.3.27.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "DefaultListableBeanFactory.java", data.fileName)
				assert.Equal(t, 348, data.lineNumber)
				assert.Equal(t, "org.springframework.beans.factory.support.DefaultListableBeanFactory", data.className)
				assert.Equal(t, "o.s.b.f.s.DefaultListableBeanFactory", data.shortClassName)
				assert.Equal(t, "Object o.s.b.f.s.DefaultListableBeanFactory.getBean(Class,Object[])", data.methodName)

				s = "void com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra.lambda$getParamsFromDB$5(java.util.Map,com.datastax.oss.driver.api.core.cql.Row) (StreamFacadeCassandra.java:69) [BOOT-INF/lib/cassandra-dao-9.3.2.64.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "cassandra-dao-9.3.2.64.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "StreamFacadeCassandra.java", data.fileName)
				assert.Equal(t, 69, data.lineNumber)
				assert.Equal(t, "com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra", data.className)
				assert.Equal(t, "c.n.c.c.s.m.StreamFacadeCassandra", data.shortClassName)
				assert.Equal(t, "void c.n.c.c.s.m.StreamFacadeCassandra.lambda$getParamsFromDB$5(Map,c.d.o.d.a.c.c.Row)", data.methodName)

				s = "org.springframework.cglib.proxy.MethodProxy com.netcracker.mano.diagnostic.configuration.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration$$EnhancerBySpringCGLIB$$e3f0bbd2.CGLIB$findMethodProxy(org.springframework.cglib.core.Signature) (<generated>:0) [BOOT-INF/lib/diagnostic-info-java-library-20.4.0.0.9.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "diagnostic-info-java-library-20.4.0.0.9.jar", data.jarName)
				assert.True(t, data.isGenerated)
				assert.Equal(t, "<generated>", data.fileName)
				assert.Equal(t, 0, data.lineNumber)
				assert.Equal(t, "com.netcracker.mano.diagnostic.configuration.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration", data.className)
				assert.Equal(t, "c.n.m.d.c.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration", data.shortClassName)
				assert.Equal(t, "o.s.c.p.MethodProxy c.n.m.d.c.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration.CGLIB$findMethodProxy(o.s.c.c.Signature)", data.methodName)

				s = "com.netcracker.profiler.io.Hotspot com.netcracker.profiler.io.Hotspot.getOrCreateChild(int) (Hotspot.java:65) [BOOT-INF/lib/parsers-9.3.2.64.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "parsers-9.3.2.64.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "Hotspot.java", data.fileName)
				assert.Equal(t, 65, data.lineNumber)

				s = "boolean com.netcracker.mano.dim.services.ArtifactsHolder.cleanupWorkingDirectoryIfOlderThan(long) (ArtifactsHolder.java:116) [BOOT-INF/lib/diagnostic-info-manager-22.1.1.0.0.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "diagnostic-info-manager-22.1.1.0.0.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "ArtifactsHolder.java", data.fileName)
				assert.Equal(t, 116, data.lineNumber)

				s = "int com.netcracker.cloud.collector.CassandraConfig$$EnhancerBySpringCGLIB$$6a0c90f5$$FastClassBySpringCGLIB$$1eaba724.getIndex(java.lang.String,java.lang.Class[]) (<generated>:0) [BOOT-INF/lib/cassandra-dao-9.3.2.64.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "cassandra-dao-9.3.2.64.jar", data.jarName)
				assert.True(t, data.isGenerated)
				assert.Equal(t, "<generated>", data.fileName)
				assert.Equal(t, 0, data.lineNumber)

				s = "void com.netcracker.mano.diagnostic.configuration.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration$$EnhancerBySpringCGLIB$$e3f0bbd2.setBeanFactory(org.springframework.beans.factory.BeanFactory) (<generated>:0) [BOOT-INF/lib/diagnostic-info-java-library-20.4.0.0.9.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "BOOT-INF/lib", data.jarPath)
				assert.Equal(t, "diagnostic-info-java-library-20.4.0.0.9.jar", data.jarName)
				assert.True(t, data.isGenerated)
				assert.Equal(t, "<generated>", data.fileName)
				assert.Equal(t, 0, data.lineNumber)

			})
			t.Run("in app jars", func(t *testing.T) {

				s := "void com.netcracker.profiler.sax.builders.SuspendLogBuilder.compress() (SuspendLogBuilder.java:86) [ncdiag/lib/runtime.jar]"
				data, ok := Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "ncdiag/lib", data.jarPath)
				assert.Equal(t, "runtime.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "SuspendLogBuilder.java", data.fileName)
				assert.Equal(t, 86, data.lineNumber)

				s = "int com.netcracker.profiler.GCDumper.getNumGCLogFiles() (GCDumper.java:64) [ncdiag/lib/runtime.jar]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "ncdiag/lib", data.jarPath)
				assert.Equal(t, "runtime.jar", data.jarName)
				assert.False(t, data.isGenerated)
				assert.Equal(t, "GCDumper.java", data.fileName)
				assert.Equal(t, 64, data.lineNumber)

				s = "void com.netcracker.cdt.uiservice.DIMConfiguration$$EnhancerBySpringCGLIB$$b7ca1149.CGLIB$STATICHOOK5() (<generated>:0) [escui.jar!/BOOT-INF/classes]"
				data, ok = Parse(s)
				assert.True(t, ok)
				assert.True(t, data.isMethod)
				assert.Equal(t, "/BOOT-INF/classes", data.jarPath)
				assert.Equal(t, "escui.jar", data.jarName)
				assert.True(t, data.isGenerated)
				assert.Equal(t, "<generated>", data.fileName)
				assert.Equal(t, 0, data.lineNumber)

			})

		})

		t.Run("common library", func(t *testing.T) {

		})
	})
}
