package com.netcracker.profiler.tools

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.path

class GenerateInjectorCommand : CliktCommand() {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            GenerateInjectorCommand().main(args)
        }
    }

    val inputClassDirectory by option().path(canBeFile = false, canBeDir = true).multiple().help("Input directory that contains the class files to process")
    val outputFile by option().path(canBeFile = true, canBeDir = false).help("Output file name to generate")

    override fun run() {
        GenerateInjector(inputClassDirectory, outputFile).run()
    }
}
