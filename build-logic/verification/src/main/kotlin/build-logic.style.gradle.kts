import com.github.autostyle.gradle.AutostyleTask
import org.gradle.kotlin.dsl.apply
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
    id("build-logic.build-params")
}

if (!buildParameters.skipAutostyle) {
    apply(plugin = "build-logic.autostyle")
    if (!buildParameters.skipCheckstyle) {
        tasks.withType<Checkstyle>().configureEach {
            mustRunAfter(tasks.withType<AutostyleTask>())
        }
    }
}

if (!buildParameters.skipCheckstyle) {
    apply(plugin = "build-logic.checkstyle")
}

if (!buildParameters.skipForbiddenApis) {
    apply(plugin = "build-logic.forbidden-apis")
}

plugins.withId("java-base") {
    // Enable errorprone when executing ./gradlew style, ./gradlew :style, etc
    val styleCheckRequested = gradle.startParameter.taskNames.any {
        it == "style" || it == "styleCheck" || it == ":style" || it == ":styleCheck"
    }
    if (buildParameters.enableErrorprone || styleCheckRequested) {
        apply(plugin = "build-logic.errorprone")
    }
}

if (!buildParameters.skipAutostyle || !buildParameters.skipCheckstyle || !buildParameters.skipForbiddenApis) {
    tasks.register("style") {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Formats code (license header, import order, whitespace at end of line, ...) and executes Checkstyle verifications"
        if (!buildParameters.skipAutostyle) {
            dependsOn("autostyleApply")
        }
        if (!buildParameters.skipCheckstyle) {
            dependsOn("checkstyleAll")
        }
        if (!buildParameters.skipForbiddenApis) {
            dependsOn("forbiddenApis")
        }
    }
    tasks.register("styleCheck") {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Report code style violations (license header, import order, whitespace at end of line, ...)"
        if (!buildParameters.skipAutostyle) {
            dependsOn("autostyleCheck")
        }
        if (!buildParameters.skipCheckstyle) {
            dependsOn("checkstyleAll")
        }
        if (!buildParameters.skipForbiddenApis) {
            dependsOn("forbiddenApis")
        }
    }
}

if (!buildParameters.skipAutostyle) {
    tasks.withType<Checkstyle>().configureEach {
        mustRunAfter(tasks.withType<AutostyleTask>())
    }
}
