package com.netcracker.profiler.agent;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Renders the head of a message body as a parameter value: text as the decoded characters, any
 * other body as {@code hex[<length>]: <hex digits>}, where the length counts the whole body.
 *
 * <p>A hex value carries the content type and the content encoding after the length, as in
 * {@code hex[1834, type=application/json, encoding=gzip:UTF-8]: 1f8b0800}. Each is left out when it
 * is absent, and the content encoding also when it is {@code identity}.</p>
 *
 * <p>The message headers decide first. A content encoding other than {@code identity} that names
 * no supported charset, such as {@code gzip} or {@code gzip:UTF-8}, marks a compressed body, which
 * is rendered as hex. A textual content type marks text, and bytes the charset cannot decode become
 * U+FFFD. The textual types are {@code text/*} and the subtypes {@code json}, {@code xml},
 * {@code yaml}, {@code x-yaml}, {@code javascript}, and {@code x-www-form-urlencoded}, alone or as a
 * {@code +} suffix. Any other body, including one sent as {@code application/octet-stream} or with
 * no content type, is text only when it decodes without error and contains no ISO control
 * character other than tab, line feed, and carriage return. Protobuf fails that check wherever it
 * holds a tag of fields 1 to 3 or a length below 32, since those bytes are control characters.</p>
 *
 * <p>The charset is a supported {@code charset} parameter of the content type, else a content
 * encoding that names a supported charset, else UTF-8. When the head is shorter than the body, a
 * multibyte character cut at its end is dropped rather than treated as malformed.</p>
 */
public final class MessageBody {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static final int MAX_CACHED_NAMES = 64;

    /** Charset names mapped to their {@link Charset}, or to {@link #UNSUPPORTED}. */
    private static final ConcurrentHashMap<String, Object> CHARSETS = new ConcurrentHashMap<>();

    private static final Object UNSUPPORTED = new Object();

    private MessageBody() {
    }

    /**
     * Renders at most {@code limit} leading bytes of {@code body}.
     *
     * @param contentType     the content type header, or {@code null}
     * @param contentEncoding the content encoding header, or {@code null}
     */
    public static String describe(byte[] body, int limit, String contentType, String contentEncoding) {
        ByteBuffer head = ByteBuffer.wrap(body, 0, Math.min(body.length, limit));
        return render(head, body.length, contentType, contentEncoding);
    }

    /**
     * Renders at most {@code limit} bytes of {@code body}, starting at its position, and leaves
     * the position and the limit of {@code body} unchanged.
     *
     * @param contentType     the content type header, or {@code null}
     * @param contentEncoding the content encoding header, or {@code null}
     */
    public static String describe(ByteBuffer body, int limit, String contentType, String contentEncoding) {
        ByteBuffer head = body.duplicate();
        int length = head.remaining();
        if (length > limit) {
            head.limit(head.position() + limit);
        }
        return render(head, length, contentType, contentEncoding);
    }

    private static String render(ByteBuffer head, int length, String contentType, String contentEncoding) {
        String encoding = contentEncoding == null ? "" : contentEncoding.trim();
        Charset encodingCharset = encoding.isEmpty() ? null : lookup(encoding);
        if ("identity".equalsIgnoreCase(encoding)) {
            encoding = "";
        }
        boolean compressed = !encoding.isEmpty() && encodingCharset == null;
        if (!compressed) {
            Charset charset = lookup(charsetParameter(contentType));
            if (charset == null) {
                charset = encodingCharset != null ? encodingCharset : StandardCharsets.UTF_8;
            }
            boolean declaredText = isTextType(contentType);
            boolean truncated = head.remaining() < length;
            String text = decode(head.duplicate(), charset,
                    declaredText ? CodingErrorAction.REPLACE : CodingErrorAction.REPORT, truncated);
            if (text != null && (declaredText || !containsControlCharacter(text))) {
                return text;
            }
        }
        return hex(head, length, contentType == null ? "" : contentType.trim(), encoding);
    }

    /** Returns the decoded characters, or {@code null} when {@code onError} reports an error. */
    private static String decode(ByteBuffer bytes, Charset charset, CodingErrorAction onError, boolean truncated) {
        CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(onError)
                .onUnmappableCharacter(onError);
        CharBuffer chars = CharBuffer.allocate((int) (bytes.remaining() * (double) decoder.maxCharsPerByte()) + 1);
        // With endOfInput false, the decoder leaves an incomplete trailing sequence in the buffer
        CoderResult result = decoder.decode(bytes, chars, !truncated);
        if (!result.isUnderflow()) {
            return null;
        }
        if (!truncated && !decoder.flush(chars).isUnderflow()) {
            return null;
        }
        chars.flip();
        return chars.toString();
    }

    private static boolean containsControlCharacter(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') {
                return true;
            }
        }
        return false;
    }

    private static String hex(ByteBuffer head, int length, String contentType, String encoding) {
        StringBuilder sb = new StringBuilder(32 + contentType.length() + encoding.length() + 2 * head.remaining());
        sb.append("hex[").append(length);
        if (!contentType.isEmpty()) {
            sb.append(", type=").append(contentType);
        }
        if (!encoding.isEmpty()) {
            sb.append(", encoding=").append(encoding);
        }
        sb.append("]: ");
        for (int i = head.position(); i < head.limit(); i++) {
            int b = head.get(i) & 0xff;
            sb.append(HEX[b >>> 4]).append(HEX[b & 0xf]);
        }
        return sb.toString();
    }

    private static boolean isTextType(String contentType) {
        if (contentType == null) {
            return false;
        }
        int semicolon = contentType.indexOf(';');
        String mimeType = (semicolon < 0 ? contentType : contentType.substring(0, semicolon))
                .trim().toLowerCase(Locale.ROOT);
        if (mimeType.startsWith("text/")) {
            return true;
        }
        int slash = mimeType.indexOf('/');
        if (slash < 0) {
            return false;
        }
        String subtype = mimeType.substring(Math.max(slash, mimeType.lastIndexOf('+')) + 1);
        switch (subtype) {
            case "json":
            case "xml":
            case "yaml":
            case "x-yaml":
            case "javascript":
            case "x-www-form-urlencoded":
                return true;
            default:
                return false;
        }
    }

    private static String charsetParameter(String contentType) {
        if (contentType == null) {
            return null;
        }
        String[] parts = contentType.split(";");
        for (int i = 1; i < parts.length; i++) {
            String parameter = parts[i].trim();
            if (parameter.regionMatches(true, 0, "charset=", 0, "charset=".length())) {
                String value = parameter.substring("charset=".length()).trim();
                if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
                    value = value.substring(1, value.length() - 1);
                }
                return value;
            }
        }
        return null;
    }

    /**
     * Returns the charset, or {@code null} for a name that is illegal or unsupported.
     *
     * <p>Results are cached, failures included, because {@link Charset#forName(String)} scans the
     * charset providers on every miss, which takes about 70 µs, and a compressed message names an
     * unsupported charset on every publish. The cache stops growing at {@value #MAX_CACHED_NAMES}
     * names, since the names come from message headers.</p>
     */
    private static Charset lookup(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        Object cached = CHARSETS.get(name);
        if (cached == null) {
            try {
                cached = Charset.forName(name);
            } catch (IllegalArgumentException e) {
                cached = UNSUPPORTED;
            }
            if (CHARSETS.size() < MAX_CACHED_NAMES) {
                CHARSETS.putIfAbsent(name, cached);
            }
        }
        return cached instanceof Charset ? (Charset) cached : null;
    }
}
