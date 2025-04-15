import com.github.gradle.node.yarn.task.YarnTask

plugins {
    id("com.github.node-gradle.node")
}

node {
    version = "22.14.0"
    download = true
}

tasks.named("yarn_install") {
    inputs.file("package.json").withPropertyName("package_json").withPathSensitivity(PathSensitivity.NONE)
    outputs.file("yarn.lock").withPropertyName("yarn.lock")
}

fun YarnTask.commonInputs() {
    inputs.dir("public").withPropertyName("public_assets").withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.dir("src").withPropertyName("script_sources").withPathSensitivity(PathSensitivity.RELATIVE)
    inputs.file("package.json").withPropertyName("package_json").withPathSensitivity(PathSensitivity.NONE)
    inputs.file("yarn.lock").withPropertyName("yarn.lock").withPathSensitivity(PathSensitivity.NONE)
}

val buildUi by tasks.registering(YarnTask::class) {
    commonInputs()
    inputs.files("index.html", "tree.html")
        .withPropertyName("html_files").withPathSensitivity(PathSensitivity.NONE)
    outputs.dir(layout.buildDirectory.file("dist/es6"))
    dependsOn("yarn_install")
    args.add("build")
    environment.put("VITE_PROJECT_VERSION", project.version.toString())
}

val buildTreeSinglepage by tasks.registering(YarnTask::class) {
    commonInputs()
    inputs.file("tree.html")
        .withPropertyName("html_files").withPathSensitivity(PathSensitivity.NONE)
    outputs.dir(layout.buildDirectory.file("dist/tree-singlepage"))
    dependsOn("yarn_install")
    args.addAll("run", "vite", "--config", "vite.config.tree.mjs", "build")
}

tasks.register("build") {
    dependsOn(buildUi)
    dependsOn(buildTreeSinglepage)
}

// https://github.com/gradle/gradle/pull/16627
inline fun <reified T: Named> AttributeContainer.attribute(attr: Attribute<T>, value: String) =
    attribute(attr, objects.named<T>(value))

val jsResources = configurations.consumable("jsResources") {
    attributes {
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, LibraryElements.RESOURCES)
    }
    outgoing {
        variants {
            create("prodResources") {
                attributes {
                    attribute(Attribute.of("org.qubership.js.optimization", String::class.java), "prod")
                }
                artifact(buildUi)
            }
            create("singlePage") {
                attributes {
                    attribute(Attribute.of("org.qubership.js.optimization", String::class.java), "single-page")
                }
                artifact(buildTreeSinglepage)
            }
        }
    }
}
