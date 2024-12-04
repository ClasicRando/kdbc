package io.github.clasicrando.kdbc.core.buffer

import kotlinx.io.Sink
import kotlinx.io.writeString

/**
 * Write the [text] to the buffer by encoding the string using the specified [charset] and finishing
 * with a null terminator (zero byte). By default, the [text] is written using as UTF-8
 */
public fun Sink.writeCString(text: CharSequence?) {
    if (!text.isNullOrEmpty()) {
        writeString(text)
    }
    writeByte(0)
}
