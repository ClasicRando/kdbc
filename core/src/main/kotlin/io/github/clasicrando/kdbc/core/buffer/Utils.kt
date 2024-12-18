package io.github.clasicrando.kdbc.core.buffer

import kotlinx.io.Buffer
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

public fun Source.readCString(): String {
    var byteCount = 0L
    val peek = this.peek()
    while (peek.readByte() != 0.toByte()) {
        byteCount++
    }
    val result = this.readString(byteCount)
    skip(1)
    return result
}

public fun ByteArray.intoBuffer(): Buffer {
    return Buffer().apply { write(this@intoBuffer) }
}

public fun Source.readByteAsInt(): Int {
    return readByte().toInt() and 0xff
}

public fun Source.peekNextAsInt(): Int {
    return this.peek().readByteAsInt()
}
