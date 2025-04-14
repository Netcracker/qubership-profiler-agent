plugins {
    id("de.thetaphi.forbiddenapis")
}

forbiddenApis {
    failOnUnsupportedJava = false
    signaturesFiles = files(layout.settingsDirectory.files(".github/linters/forbidden-apis.txt"))
    bundledSignatures.addAll(
        listOf(
            // "jdk-deprecated",
            "jdk-internal",
            "jdk-non-portable"
            // "jdk-system-out"
            // "jdk-unsafe"
        )
    )
}
