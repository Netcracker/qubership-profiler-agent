import org.gradle.kotlin.dsl.dependencies

plugins {
    id("build-logic.build-params")
    id("org.checkerframework")
}

dependencies {
    providers.gradleProperty("checkerframework.version")
        .takeIf { it.isPresent }
        ?.let {
            val checkerframeworkVersion = it.get()
            "checkerFramework"("org.checkerframework:checker:$checkerframeworkVersion")
        } ?: run {
            val checkerframeworkVersion = "3.49.4"
            "checkerFramework"("org.checkerframework:checker:$checkerframeworkVersion")
        }
}

checkerFramework {
    skipVersionCheck = true
    excludeTests = true
    // See https://checkerframework.org/manual/#introduction
    checkers.add("org.checkerframework.checker.nullness.NullnessChecker")
    checkers.add("org.checkerframework.checker.optional.OptionalChecker")
    // checkers.add("org.checkerframework.checker.index.IndexChecker")
    checkers.add("org.checkerframework.checker.regex.RegexChecker")
    extraJavacArgs.add("-Astubs=" +
            fileTree("$rootDir/config/checkerframework") {
                include("*.astub")
            }.asPath
    )
    // The below produces too many warnings :(
    // extraJavacArgs.add("-Alint=redundantNullComparison")
}
