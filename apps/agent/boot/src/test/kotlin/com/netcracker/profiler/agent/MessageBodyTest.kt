package com.netcracker.profiler.agent

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.ByteBuffer

/**
 * A body whose content encoding marks it compressed is rendered as
 * `hex[<whole length>, type=<content type>, encoding=<content encoding>]: <hex of the head>`. Any
 * other body is text when its content type declares text, or when its head decodes cleanly and
 * holds no control character other than tab, line feed, and carriage return; otherwise it is
 * rendered as hex too. A hex value leaves out a header that is absent, and the `identity` content
 * encoding.
 */
class MessageBodyTest {
    private fun bytes(vararg values: Int) = ByteArray(values.size) { values[it].toByte() }

    /** "Привет" in UTF-8: six two-byte characters. */
    private val privetUtf8 = bytes(0xd0, 0x9f, 0xd1, 0x80, 0xd0, 0xb8, 0xd0, 0xb2, 0xd0, 0xb5, 0xd1, 0x82)

    /** "Привет" in windows-1251, which is malformed as UTF-8. */
    private val privetCp1251 = bytes(0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2)

    @Test
    fun `a UTF-8 body with no headers is rendered as text`() {
        assertEquals("Привет", MessageBody.describe(privetUtf8, 100, null, null))
    }

    @Test
    fun `tab, line feed, and carriage return keep a body text`() {
        assertEquals("a\tb\r\nc", MessageBody.describe("a\tb\r\nc".toByteArray(), 100, null, null))
    }

    @Test
    fun `an empty body is rendered as empty text`() {
        assertEquals("", MessageBody.describe(ByteArray(0), 100, null, null))
    }

    @Test
    fun `a head that ends between two characters is rendered whole`() {
        assertEquals("Привет", MessageBody.describe(privetUtf8, 12, null, null))
    }

    @Test
    fun `a character cut by the limit is dropped`() {
        assertEquals("Приве", MessageBody.describe(privetUtf8, 11, null, null))
    }

    @Test
    fun `a character cut by the limit is dropped from a declared text body`() {
        assertEquals("Приве", MessageBody.describe(privetUtf8, 11, "text/plain", "UTF-8"))
    }

    @Test
    fun `DEL marks a body binary`() {
        assertEquals("hex[3]: 617f62", MessageBody.describe(bytes(0x61, 0x7f, 0x62), 100, null, null))
    }

    @Test
    fun `a C1 control character marks a body binary`() {
        // U+0085, next line, in UTF-8
        assertEquals("hex[2]: c285", MessageBody.describe(bytes(0xc2, 0x85), 100, null, null))
    }

    @Test
    fun `a complete body that ends inside a character is rendered as hex`() {
        assertEquals("hex[3]: d09fd1", MessageBody.describe(bytes(0xd0, 0x9f, 0xd1), 100, null, null))
    }

    @Test
    fun `a Java serialization stream is rendered as hex`() {
        assertEquals(
            "hex[4, type=application/x-java-serialized-object]: aced0005",
            MessageBody.describe(bytes(0xac, 0xed, 0x00, 0x05), 100, "application/x-java-serialized-object", null)
        )
    }

    @Test
    fun `protobuf that is valid UTF-8 is rendered as hex for its control bytes`() {
        // Field 2, length 4, "test"
        assertEquals(
            "hex[6]: 120474657374",
            MessageBody.describe(bytes(0x12, 0x04, 0x74, 0x65, 0x73, 0x74), 100, null, null)
        )
    }

    @Test
    fun `hex counts the whole body and shows only the head`() {
        assertEquals(
            "hex[10]: 1f8b0800",
            MessageBody.describe(bytes(0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03), 4, null, null)
        )
    }

    @Test
    fun `a compressing content encoding renders a printable body as hex`() {
        assertEquals(
            "hex[2, type=application/json, encoding=gzip:UTF-8]: 7b7d",
            MessageBody.describe("{}".toByteArray(), 100, "application/json", "gzip:UTF-8")
        )
    }

    @Test
    fun `an illegal charset name in the content encoding renders the body as hex`() {
        assertEquals("hex[2, encoding=not a charset!]: 7b7d", MessageBody.describe("{}".toByteArray(), 100, null, "not a charset!"))
    }

    @Test
    fun `the identity content encoding leaves a body text`() {
        assertEquals("{}", MessageBody.describe("{}".toByteArray(), 100, null, "identity"))
    }

    @Test
    fun `hex omits the identity content encoding`() {
        assertEquals("hex[2]: 1f8b", MessageBody.describe(bytes(0x1f, 0x8b), 100, null, "identity"))
    }

    @Test
    fun `hex carries a charset content encoding the bytes do not decode in`() {
        assertEquals("hex[3, encoding=UTF-8]: d09fd1", MessageBody.describe(bytes(0xd0, 0x9f, 0xd1), 100, null, "UTF-8"))
    }

    @Test
    fun `a charset in the content encoding decodes the body`() {
        assertEquals("Привет", MessageBody.describe(privetCp1251, 100, null, "windows-1251"))
    }

    @Test
    fun `the charset parameter of the content type wins over the content encoding`() {
        assertEquals(
            "Привет",
            MessageBody.describe(privetCp1251, 100, "text/plain; charset=\"windows-1251\"", "UTF-8")
        )
    }

    @Test
    fun `an unquoted charset parameter decodes the body`() {
        assertEquals("Привет", MessageBody.describe(privetCp1251, 100, "text/plain;charset=windows-1251", null))
    }

    @Test
    fun `an unsupported charset parameter falls back to UTF-8`() {
        assertEquals("Привет", MessageBody.describe(privetUtf8, 100, "text/plain; charset=x-no-such-charset", null))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "text/csv",
            "application/json",
            "application/xml",
            "application/yaml",
            "application/x-yaml",
            "application/javascript",
            "application/x-www-form-urlencoded",
            "application/vnd.api+json",
            "application/atom+xml",
            "application/vnd.oai.openapi+yaml",
        ]
    )
    fun `a declared text type keeps a control character`(contentType: String) {
        assertEquals("\u001b[31mred", MessageBody.describe("\u001b[31mred".toByteArray(), 100, contentType, null))
    }

    @Test
    fun `an octet-stream body is rendered as text when it decodes cleanly`() {
        assertEquals(
            "{\"id\":1}",
            MessageBody.describe("{\"id\":1}".toByteArray(), 100, "application/octet-stream", null)
        )
    }

    @Test
    fun `a control character renders a body of another type as hex`() {
        assertEquals(
            "hex[8, type=application/x-protobuf]: 1b5b33316d726564",
            MessageBody.describe("\u001b[31mred".toByteArray(), 100, "application/x-protobuf", null)
        )
    }

    @Test
    fun `a declared text type replaces a malformed byte`() {
        assertEquals("{\uFFFD}", MessageBody.describe(bytes(0x7b, 0xff, 0x7d), 100, "Text/Plain", null))
    }

    @Test
    fun `a ByteBuffer is read from its position and left unmoved`() {
        val buffer = ByteBuffer.wrap(bytes(0x00, 0x01, 0xac, 0xed, 0x00, 0x05))
        buffer.position(2)

        val described = MessageBody.describe(buffer, 3, null, null)

        assertEquals("hex[4]: aced00", described)
        assertEquals(2, buffer.position(), "position after describe")
        assertEquals(6, buffer.limit(), "limit after describe")
    }

    @Test
    fun `a ByteBuffer shorter than the limit is read whole`() {
        assertEquals("{}", MessageBody.describe(ByteBuffer.wrap("{}".toByteArray()), 100, null, null))
    }
}
