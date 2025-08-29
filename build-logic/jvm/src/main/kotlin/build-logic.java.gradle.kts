import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.dsl.configureEach
import java.time.LocalDate

plugins {
    id("java")
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.gradle-extensions")
    id("build-logic.repositories")
    id("build-logic.test-base")
    id("build-logic.build-params")
    id("build-logic.style")
    id("build-logic.toolchains")
    // id("com.github.vlsi.jandex")
}

java {
    toolchain {
        configureToolchain(buildParameters.buildJdk)
    }
}

tasks.configureEach<JavaExec> {
    buildParameters.testJdk?.let {
        javaLauncher.set(javaToolchains.launcherFor(it))
    }
}

// IDEA reconfigures javaexec.executable, so we adjust it in afterEvaluate
afterEvaluate {
    tasks.configureEach<JavaExec> {
        executable(javaLauncher.get().executablePath.asFile.absolutePath)
    }
}

sourceSets {
    main {
        resources {
            // TODO: remove when LICENSE is removed (it is used by Maven build for now)
            exclude("META-INF/LICENSE")
        }
    }
}

dependencies {
    findProject(":bom-thirdparty")?.let {
        api(platform(it))
        compileOnlyApi(platform(it))
    }
}

//project.configure<com.github.vlsi.jandex.JandexExtension> {
//    skipIndexFileGeneration()
//}

if (!buildParameters.enableGradleMetadata) {
    tasks.configureEach<GenerateModuleMetadata> {
        enabled = false
    }
}

if (buildParameters.coverage || gradle.startParameter.taskNames.any { it.contains("jacoco") }) {
    apply(plugin = "build-logic.jacoco")
}

tasks.configureEach<JavaCompile> {
    buildParameters.buildJdk?.let {
        javaCompiler.set(javaToolchains.compilerFor(it))
    }
    options.apply {
        encoding = "UTF-8"
        release = buildParameters.targetJavaVersion
        if (!buildParameters.enableCheckerframework) {
            compilerArgs.add("-Xlint:deprecation")
        } else {
            // We use checkerframework for nullability mostly, so we don't want to see deprecation warnings
            compilerArgs.add("-Xmaxerrs")
            compilerArgs.add("1")
        }
        if (buildParameters.failOnJavacWarning && !name.contains("Test")) {
            compilerArgs.add("-Werror")
        }
    }
}

tasks.configureEach<Javadoc> {
    (options as StandardJavadocDocletOptions).apply {
        // Please refrain from using non-ASCII chars below since the options are passed as
        // javadoc.options file which is parsed with "default encoding"
        noTimestamp.value = true
        showFromProtected()
        if (buildParameters.failOnJavadocWarning) {
            // See JDK-8200363 (https://bugs.openjdk.java.net/browse/JDK-8200363)
            // for information about the -Xwerror option.
            addBooleanOption("Xwerror", true)
        }
        // There are too many missing javadocs, so failing the build on missing comments seems to be not an option
        addBooleanOption("Xdoclint:all,-missing", true)
        // javadoc: error - The code being documented uses modules but the packages
        // defined in https://docs.oracle.com/javase/9/docs/api/ are in the unnamed module
        source = "1.8"
        docEncoding = "UTF-8"
        charSet = "UTF-8"
        encoding = "UTF-8"
        docTitle = "Qubership Profiler Agent ${project.name} version ${project.version}"
        windowTitle = "Qubership Profiler Agent ${project.name} version ${project.version}"
        header = "<b>Qubership Profiler Agent</b>"
        val lastEditYear = providers.gradleProperty("lastEditYear")
            .getOrElse(LocalDate.now().year.toString())
        bottom =
            "Copyright &copy; 1997-$lastEditYear Netcracker Qubership Development Group. All Rights Reserved."
        if (buildParameters.buildJdkVersion >= 17) {
            addBooleanOption("html5", true)
        } else if (buildParameters.buildJdkVersion >= 9) {
            addBooleanOption("html5", true)
            links("https://docs.oracle.com/en/java/javase/11/docs/api/")
        } else {
            links("https://docs.oracle.com/javase/8/docs/api/")
        }
    }
}

// Add default license/notice when missing (e.g. see :src:config that overrides LICENSE)

afterEvaluate {
    tasks.configureEach<Jar> {
        CrLfSpec(LineEndings.LF).run {
            into("META-INF") {
                filteringCharset = "UTF-8"
                duplicatesStrategy = DuplicatesStrategy.EXCLUDE
                from("$rootDir/LICENSE")
                from("$rootDir/NOTICE")
            }
        }
    }
}

tasks.configureEach<Jar> {
    manifest {
        attributes["Bundle-License"] = "Apache-2.0"
        attributes["Implementation-Title"] = "Qubership Profiler Agent"
        attributes["Implementation-Version"] = project.version
        attributes["Implementation-Vendor"] = "Netcracker Qubership"
        attributes["Implementation-Vendor-Id"] = "org.qubership"
    }
}
