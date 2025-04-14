import org.apache.tools.ant.taskdefs.condition.Os

plugins {
    id("com.github.autostyle")
}

autostyle {
    kotlinGradle {
        ktlint()
        trimTrailingWhitespace()
        endWithNewline()
    }
    // TODO: the task fails on Windows as follows:
    if (!Os.isFamily(Os.FAMILY_WINDOWS)) {
        format("markdown") {
            target("**/*.md")
            endWithNewline()
        }
    }
}

plugins.withId("java") {
    autostyle {
        java {
            // targetExclude("**/test/java/*.java")
            // licenseHeaderFile(licenseHeaderFile)
            importOrder(
                "static ",
                "org.qubership.",
                "",
                "java.",
                "javax."
            )
            removeUnusedImports()
            trimTrailingWhitespace()
            indentWithSpaces(4)
            endWithNewline()
        }
    }
}
