package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.ZERO_BYTE
import io.ktor.utils.io.core.discard
import io.ktor.utils.io.core.takeWhile
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readString
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

/**
 * Read bytes until 0 is found indicating the end of a CString (null terminated char array). These
 * bytes are then converted to a [String] using the specified [charset]. By default, the bytes are
 * read using as UTF-8.
 */
public fun Source.readCString(): String {
    var length = 0L
    this.peek().takeWhile {
        length++
        it.readByte() != ZERO_BYTE
    }
    val result = this.readString(byteCount = length - 1)
    this.discard(count = 1)
    return result
}
