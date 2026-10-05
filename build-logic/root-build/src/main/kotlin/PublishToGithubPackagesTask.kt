import gratatouille.tasks.FileWithPath
import java.io.File
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Base64
import javax.inject.Inject
import nmcp.transport.Content
import nmcp.transport.Transport
import nmcp.transport.publishFileByFile
import okio.Buffer
import okio.BufferedSource
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.logging.Logging
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
import org.gradle.workers.WorkAction
import org.gradle.workers.WorkParameters
import org.gradle.workers.WorkerExecutor

/**
 * Uploads Maven repository directories, such as the ones in `nmcpAggregation.allFiles`, to a GitHub Packages
 * Maven registry.
 *
 * Every version directory is uploaded by its own work item, so `--max-workers` bounds the number of concurrent
 * uploads. For a snapshot, the nmcp library reads the last build number from the registry and renames the files to
 * the next build number and a new timestamp.
 *
 * The upload is not atomic: when one version directory fails, the ones uploaded before it stay in the registry.
 */
@UntrackedTask(because = "Publishes to a remote repository")
abstract class PublishToGithubPackagesTask : DefaultTask() {
    /** Directories laid out as a Maven repository. */
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val repositoryDirectories: ConfigurableFileCollection

    /** Registry URL, such as `https://maven.pkg.github.com/<owner>/<repository>`. */
    @get:Input
    abstract val repositoryUrl: Property<String>

    @get:Internal
    abstract val username: Property<String>

    @get:Internal
    abstract val password: Property<String>

    /** Runtime classpath of `com.gradleup.nmcp:nmcp-tasks`, the library [PublishVersionDirectoryAction] calls. */
    @get:Classpath
    abstract val publisherClasspath: ConfigurableFileCollection

    @get:Inject
    abstract val workerExecutor: WorkerExecutor

    @TaskAction
    fun publish() {
        val username = username.orNull
            ?: throw GradleException("Set the githubPackagesUsername Gradle property to publish to GitHub Packages")
        val password = password.orNull
            ?: throw GradleException("Set the githubPackagesPassword Gradle property to publish to GitHub Packages")

        val filesByVersionDirectory = sortedMapOf<String, MutableMap<String, String>>()
        repositoryDirectories.asFileTree.visit {
            if (!isDirectory && isUploaded(name)) {
                val path = relativePath.pathString
                filesByVersionDirectory.getOrPut(path.substringBeforeLast('/')) { mutableMapOf() }[path] =
                    file.absolutePath
            }
        }
        if (filesByVersionDirectory.isEmpty()) {
            throw GradleException("No files to publish found in ${repositoryDirectories.files}")
        }

        logger.lifecycle(
            "Uploading {} files in {} version directories to {}",
            filesByVersionDirectory.values.sumOf { it.size },
            filesByVersionDirectory.size,
            repositoryUrl.get(),
        )
        val queue = workerExecutor.classLoaderIsolation {
            classpath.from(publisherClasspath)
        }
        filesByVersionDirectory.values.forEach { files ->
            queue.submit(PublishVersionDirectoryAction::class.java) {
                repositoryUrl.set(this@PublishToGithubPackagesTask.repositoryUrl)
                this.username.set(username)
                this.password.set(password)
                this.files.set(files)
            }
        }
    }

    /**
     * Leaves out checksum files and `maven-metadata.xml`. GitHub Packages ignores uploaded copies of both: it serves
     * checksums computed from the artifact and metadata it builds from the uploaded file names.
     */
    private fun isUploaded(name: String): Boolean =
        !name.startsWith("maven-metadata.xml") && name.substringAfterLast('.') !in CHECKSUM_EXTENSIONS

    private companion object {
        val CHECKSUM_EXTENSIONS = setOf("md5", "sha1", "sha256", "sha512")
    }
}

abstract class PublishVersionDirectoryAction : WorkAction<PublishVersionDirectoryAction.Parameters> {
    interface Parameters : WorkParameters {
        val repositoryUrl: Property<String>
        val username: Property<String>
        val password: Property<String>

        /** Maps the path in the repository to the absolute path of the file. */
        val files: MapProperty<String, String>
    }

    override fun execute() {
        val credentials = "${parameters.username.get()}:${parameters.password.get()}"
        val transport = GithubPackagesTransport(
            baseUrl = parameters.repositoryUrl.get(),
            authorization = "Basic " + Base64.getEncoder().encodeToString(credentials.toByteArray()),
        )
        val files = parameters.files.get().map { (path, file) -> FileWithPath(File(file), path) }
        publishFileByFile(transport, files, 1)
    }
}

/**
 * Sends the requests of [publishFileByFile] to a GitHub Packages Maven registry.
 *
 * GitHub Packages requires the token for downloads too, so every request carries [authorization], except a
 * redirect to another host.
 * The transport retries network errors, `5xx`, `429`, and a `403` that carries `Retry-After`, the responses the
 * GitHub REST API documents for a secondary rate limit.
 */
internal class GithubPackagesTransport(
    baseUrl: String,
    private val authorization: String,
) : Transport {
    private val logger = Logging.getLogger(GithubPackagesTransport::class.java)
    private val baseUri = URI.create(baseUrl.removeSuffix("/") + "/")
    private val client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build()

    override fun get(path: String): BufferedSource? {
        var uri = baseUri.resolve(path)
        repeat(MAX_REDIRECTS + 1) {
            val response = send(uri, "GET", HttpRequest.BodyPublishers.noBody())
            when (response.statusCode()) {
                in 200..299 -> return Buffer().write(response.body())
                404 -> return null
                301, 302, 303, 307, 308 -> {
                    val location = response.headers().firstValue("Location")
                        .orElseThrow { IOException("GET $uri returned ${response.statusCode()} with no Location") }
                    uri = uri.resolve(location)
                }
                else -> throw failure("GET", uri, response)
            }
        }
        throw IOException("GET ${baseUri.resolve(path)} redirected more than $MAX_REDIRECTS times")
    }

    override fun put(path: String, body: Content) {
        val bytes = Buffer().also { body.writeTo(it) }.readByteArray()
        val uri = baseUri.resolve(path)
        val response = send(uri, "PUT", HttpRequest.BodyPublishers.ofByteArray(bytes))
        if (response.statusCode() !in 200..299) {
            throw failure("PUT", uri, response)
        }
    }

    private fun send(uri: URI, method: String, body: HttpRequest.BodyPublisher): HttpResponse<ByteArray> {
        val request = HttpRequest.newBuilder(uri)
            .method(method, body)
            .timeout(Duration.ofMinutes(5))
            .apply {
                if (uri.host == baseUri.host) {
                    header("Authorization", authorization)
                }
            }
            .build()
        var attempt = 1
        while (true) {
            logger.info("{} {} (attempt {})", method, uri, attempt)
            val response = try {
                client.send(request, HttpResponse.BodyHandlers.ofByteArray())
            } catch (e: IOException) {
                if (attempt == MAX_ATTEMPTS) {
                    throw IOException("$method $uri failed after $attempt attempts", e)
                }
                waitBeforeRetry(method, uri, e.toString(), null, attempt++)
                continue
            }
            val status = response.statusCode()
            val retryAfter = response.headers().firstValue("Retry-After").orElse(null)?.toLongOrNull()
            val retryable = status >= 500 || status == 429 || (status == 403 && retryAfter != null)
            if (!retryable || attempt == MAX_ATTEMPTS) {
                return response
            }
            waitBeforeRetry(method, uri, "HTTP $status", retryAfter, attempt++)
        }
    }

    private fun waitBeforeRetry(method: String, uri: URI, reason: String, retryAfter: Long?, attempt: Int) {
        val delaySeconds = (retryAfter ?: (1L shl attempt)).coerceIn(0, MAX_DELAY_SECONDS)
        logger.lifecycle(
            "{} {} failed with {}, retrying in {} s (attempt {} of {})",
            method, uri, reason, delaySeconds, attempt, MAX_ATTEMPTS,
        )
        Thread.sleep(Duration.ofSeconds(delaySeconds).toMillis())
    }

    private fun failure(method: String, uri: URI, response: HttpResponse<ByteArray>) =
        IOException("$method $uri returned ${response.statusCode()}: ${String(response.body()).take(1000)}")

    private companion object {
        const val MAX_ATTEMPTS = 5
        const val MAX_REDIRECTS = 5
        const val MAX_DELAY_SECONDS = 120L
    }
}
