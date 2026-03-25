package dictionary

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	rePluginParameter *regexp.Regexp
	reCGlibId         *regexp.Regexp
	reSignature       *regexp.Regexp
)

type (
	DictHash struct {
		hash      uint64 // hashed id by jar/className ? // hash/crc32 ? // FNV32, FNV32a, FNV64, FNV64a, MD5, SHA1, SHA256 and SHA512 ?
		serviceId int64  //
		jarId     int64  //
		classId   int64  //
		classType string // nc, jdk, spring, db, kafka, drivers, etc...
	}

	DictRecord struct {
		original       string // original string
		title          string // clean title for UI
		isMethod       bool   // method or param
		fullClass      string // full class name,    like 'com.netcracker.mano.diagnostic.configuration.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration$$EnhancerBySpringCGLIB$$e3f0bbd2'
		className      string // clean class name,   like 'StreamFacadeCassandra' or 'DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration'
		shortClassName string // short class name,   like 'c.n.c.c.s.m.StreamFacadeCassandra' or 'c.n.m.d.c.DiagnosticInfoAutoConfiguration$SpringBoot2EnvironmentBlockCalculatorConfiguration'
		methodName     string // method signature,   like 'void setBeanFactory(o.s.b.f.BeanFactory)'
		isGenerated    bool   // '<generated>:0' instead of source line
		fileName       string // 'StreamFacadeCassandra.java'
		lineNumber     int    // 67 for 'StreamFacadeCassandra.java:67'
		jarName        string // 'cassandra-dao-9.3.2.64.jar'
		jarPath        string // 'BOOT-INF/lib/cassandra-dao-9.3.2.64.jar'
	}
)

func init() {
	rePluginParameter, _ = regexp.Compile("^[a-z\\-_.]+$")               // parameters from Profiler plugins, like 'log.generated', 'trace_id', 'x-request-id'
	reCGlibId, _ = regexp.Compile("\\$\\$[a-z0-9]{3,8}")                 // uniq ids from CGLib like '$$e3f0bbd2'
	reSignature, _ = regexp.Compile("^(([^(]+)\\.[^(.]+)\\(([^(]*)\\)$") // method names, like 'com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra.lambda$getParamsFromDB$5(java.util.Map,com.datastax.oss.driver.api.core.cql.Row)'
}

func Parse(s string) (*DictRecord, bool) {
	m := &DictRecord{
		original: s,
		title:    s,
	}

	arr := strings.Split(s, " ")
	if len(arr) <= 1 {
		f := rePluginParameter.MatchString(s)
		return m, f
	}
	m.isMethod = true

	fmt.Println("\n --- ")
	//fmt.Println(arr)
	//fmt.Println(strings.Join(arr, "|"))

	if path, jarPath, ok := isJar(arr[len(arr)-1]); ok {
		fmt.Println(" * ", path, jarPath)
		m.jarPath = path
		m.jarName = jarPath
		arr = arr[:len(arr)-1]
	}
	if generated, class, line, ok := isLine(arr[len(arr)-1]); ok {
		fmt.Println(" * ", class, line)
		m.isGenerated = generated
		m.fileName = class
		m.lineNumber = line
		arr = arr[:len(arr)-1]
	}
	if len(arr) != 2 { // should have only signature left: 'type method(params)'
		return m, false
	}

	res := parseSignature(m, arr[0], arr[1])

	//fmt.Println("\n --- ", res, m)
	return m, res
}

func parseSignature(m *DictRecord, returnType string, methodName string) bool {
	returnType = shortClass(returnType)

	methodName = strings.ReplaceAll(methodName, "$$EnhancerBySpringCGLIB", "")
	methodName = strings.ReplaceAll(methodName, "$$FastClassBySpringCGLIB", "")
	methodName = strings.ReplaceAll(methodName, "$STATICHOOK", "")
	methodName = reCGlibId.ReplaceAllString(methodName, "")

	if !reSignature.MatchString(methodName) {
		return false // strange
	}
	method := reSignature.FindStringSubmatch(methodName)
	fmt.Println(strings.Join(method, " +++ "))
	if len(method) != 4 {
		return false
	}
	methodName = shorten(method[1], 2)

	m.className = method[2]
	m.shortClassName = shortClass(m.className)

	args, _ := prepareArgs(method[3])

	m.methodName = fmt.Sprintf("%s %s(%s)", returnType, methodName, args)
	fmt.Println(m.methodName)
	return true
}

func prepareArgs(methodArgs string) (string, []string) {
	args := strings.Split(methodArgs, ",")
	for i := 0; i < len(args); i++ {
		args[i] = shortClass(args[i])
	}
	return strings.Join(args, ","), args
}

func isJar(s string) (string, string, bool) {
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		return "", "", false
	}
	s = s[1 : len(s)-1]
	if strings.Contains(s, ".jar!/") { // spring all-in-all jar ? 'escui.jar!/BOOT-INF/classes'
		path := strings.Split(s, "!")
		return path[1], path[0], true
	}
	path := strings.Split(s, "/")
	jar := path[len(path)-1]
	if strings.Contains(jar, "jar") {
		return strings.Join(path[0:len(path)-1], "/"), jar, true
	} else {
		return s, "", true // unparsed
	}
}

func isLine(s string) (bool, string, int, bool) {
	if strings.Contains(s, "<generated>") {
		return true, "<generated>", 0, true
	}
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		arr := strings.Split(s[1:len(s)-1], ":")
		if len(arr) == 2 {
			line, err := strconv.Atoi(arr[1])
			return false, arr[0], line, err == nil
		}
	}
	return false, "", 0, false
}

func shortClass(s string) string {
	return shorten(s, 1)
}

func shorten(s string, left int) string {
	s = strings.ReplaceAll(s, "java.lang.", "")
	s = strings.ReplaceAll(s, "java.util.", "")
	arr := strings.Split(s, ".")
	for i := 0; i < len(arr)-left; i++ {
		arr[i] = arr[i][:1]
	}
	return strings.Join(arr, ".")
}
