import com.github.vlsi.gradle.dsl.configureEach
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.dependencies

plugins {
    id("java")
    id("build-logic.repositories")
}

if (!project.hasProperty("skipErrorprone")) {
    apply(plugin = "net.ltgt.errorprone")

    dependencies {
        "errorprone"("com.google.errorprone:error_prone_core:2.42.0")
        "annotationProcessor"("com.google.guava:guava-beta-checker:1.0")
    }

    tasks.configureEach<JavaCompile> {
        if ("Test" in name) {
            // Ignore warnings in test code
            options.errorprone.isEnabled.set(false)
        } else {
            options.compilerArgs.addAll(listOf("-Xmaxerrs", "10000", "-Xmaxwarns", "10000"))
            options.errorprone {
                disableWarningsInGeneratedCode.set(true)
                error(
                    "PackageLocation",
                )
                enable(
                )
                disable(
                    "ArgumentSelectionDefectChecker",
                    "AssignmentExpression",
                    "BanJNDI",
                    "BigDecimalEquals",
                    "ChainingConstructorIgnoresParameter",
                    "ClassCanBeStatic",
                    "ClassNewInstance",
                    "ComparisonOutOfRange",
                    "DefaultCharset",
                    "DoNotCallSuggester",
                    "EmptyBlockTag",
                    "EmptyCatch",
                    "FallThrough",
                    "HidingField",
                    "ImmutableEnumChecker",
                    "InconsistentCapitalization",
                    "InputStreamSlowMultibyteRead",
                    "IntLongMath",
                    "JavaUtilDate",
                    "JdkObsolete",
                    "LongFloatConversion",
                    "LoopOverCharArray",
                    "MathRoundIntLong",
                    "MethodCanBeStatic",
                    "MissingCasesInEnumSwitch",
                    "MissingOverride",
                    "MixedMutabilityReturnType",
                    "MutablePublicArray",
                    "NarrowCalculation",
                    "NarrowingCompoundAssignment",
                    "NonApiType",
                    "NotJavadoc",
                    "OverridingMethodInconsistentArgumentNamesChecker",
                    "RedundantControlFlow",
                    "ReturnAtTheEndOfVoidFunction",
                    "ReturnValueIgnored",
                    "SelfComparison",
                    "StaticAssignmentInConstructor",
                    "StringCaseLocaleUsage",
                    "StringCharset",
                    "ThreadPriorityCheck",
                    "UnnecessaryAsync",
                    "UnsynchronizedOverridesSynchronized",
                    "UnusedMethod",
                    "UnusedVariable",
                    "EqualsGetClass",
                    "InlineMeSuggester",
                    "MissingSummary",
                    "OperatorPrecedence",
                    "StringConcatToTextBlock",
                    "StringSplitter",
                    "UnnecessaryParentheses",
                )
            }
        }
    }
}
