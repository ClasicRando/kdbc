package io.github.clasicrando.kdbc.core.buffer

import io.ktor.utils.io.core.writeText
import kotlinx.io.Sink
import java.nio.charset.Charset

/**
 * Write the [text] to the buffer by encoding the string using the specified [charset] and
 * finishing with a null terminator (zero byte). By default, the [text] is written using
 * [Charsets.UTF_8].
 */
fun Sink.writeCString(
    text: CharSequence,
    charset: Charset = Charsets.UTF_8,
) {
    writeText(text, charset = charset)
    writeByte(0)
}
