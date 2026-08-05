plugins {
    id("java-base")
    id("jacoco")
}

jacoco {
    toolVersion = "0.8.15"
    providers.gradleProperty("jacoco.version")
        .takeIf { it.isPresent }
        ?.let { toolVersion = it.get() }
}

val testTasks = tasks.withType<Test>()
val javaExecTasks = tasks.withType<JavaExec>()

// This configuration must be postponed since JacocoTaskExtension might be added inside
// configure block of a task (== before this code is run)
afterEvaluate {
    for (t in arrayOf(testTasks, javaExecTasks)) {
        t.configureEach {
            extensions.findByType<JacocoTaskExtension>()?.apply {
                // We want collect code coverage for com.netcracker classes only
                includes?.add("com.netcracker.*")
            }
        }
    }
}

val jacocoReport by rootProject.tasks.existing(JacocoReport::class)
val mainCode = sourceSets["main"]

// The aggregate report puts every module's classes into one flat namespace, and JaCoCo
// aborts with "Can't add different class with same name" when two modules ship the same
// fully qualified name compiled from different sources. This build has two such pairs:
// ClassInfoImpl in plugin-generator and plugin-runtime, and Pair in parsers and common.
// Dropping both copies costs two classes' worth of coverage and keeps the report
// building. JaCoCo names the offender in the failure message, so a new collision is easy
// to spot — de-duplicate the source if you can, and only extend this list if you cannot.
val classesDuplicatedAcrossProjects = listOf(
    "com/netcracker/profiler/instrument/enhancement/ClassInfoImpl.class",
    "com/netcracker/profiler/instrument/enhancement/ClassInfoImpl\$*.class",
    "com/netcracker/profiler/io/Pair.class",
    "com/netcracker/profiler/io/Pair\$*.class",
)

// TODO: rework with provide-consume configurations
jacocoReport {
    // Note: this creates a lazy collection
    // Some projects might fail to create a file (e.g. no tests or no coverage),
    // So we check for file existence. Otherwise, JacocoMerge would fail
    //
    // Test tasks only, deliberately. files(task) contributes the task's outputs *and* a
    // dependency on the task itself, and JavaExec covers :profiler:runWar and
    // :it-e2e:runProfiler — tasks that start a server and never return. Listing them here
    // made the aggregate report hang the build instead of producing a report. Their
    // coverage is still written under build/jacoco when they are run by hand; it just is
    // not aggregated.
    val execFiles =
        files(testTasks).filter { it.exists() && it.name.endsWith(".exec") }
    executionData(execFiles)
    additionalSourceDirs.from(mainCode.allJava.srcDirs)
    sourceDirectories.from(mainCode.allSource.srcDirs)
    classDirectories.from(
        mainCode.output.asFileTree.matching { exclude(classesDuplicatedAcrossProjects) }
    )
}

// TODO: check which reports do we need
//tasks.configureEach<JacocoReport> {
//    reports {
//        html.required.set(reportsForHumans())
//        xml.required.set(!reportsForHumans())
//    }
//}
