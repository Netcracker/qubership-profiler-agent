@file:JvmName("URLExtensions")
package com.netcracker.profiler.testkit.resources

import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.net.URISyntaxException
import java.net.URL
import java.nio.file.Paths
import kotlin.jvm.Throws

/**
 * Retrieves a resource by its name and converts it to a File object. Throws an exception
 * if the resource is not found.
 *
 * @param clazz the class whose class loader is used to locate the resource
 * @param resourceName the name of the resource to be retrieved
 * @return the File object corresponding to the located resource
 * @throws FileNotFoundException when the resource is not found
 */
@Throws(FileNotFoundException::class)
fun getResourceAsFile(clazz: Class<*>, resourceName: String): File {
    return clazz.getResource(resourceName)
        ?.urlToFile()
        ?: throw FileNotFoundException("File $resourceName not found in $clazz")
}

/**
 * Converts a given URL to a File object if the URL uses the "file" protocol.
 *
 * @param url the URL to convert to a File object
 * @return the File object corresponding to the given URL, or null if the URL does not use the "file" protocol
 * @throws IllegalArgumentException if the URL cannot be converted to a URI
 */
fun URL.urlToFile(): File {
    if ("file" != protocol) {
        throw IllegalArgumentException("URL must use the 'file' protocol, got $protocol")
    }
    val uri: URI
    try {
        uri = toURI()
    } catch (e: URISyntaxException) {
        throw IllegalArgumentException("Unable to convert URL $this to URI", e)
    }
    if (uri.isOpaque) {
        // It is like file:test%20file.c++
        // getSchemeSpecificPart would return "test file.c++"
        return File(uri.getSchemeSpecificPart())
    }
    // See https://stackoverflow.com/a/17870390/1261287
    return Paths.get(uri).toFile()
}
