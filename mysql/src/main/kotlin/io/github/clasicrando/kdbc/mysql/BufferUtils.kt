package io.github.clasicrando.kdbc.mysql

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException

fun ByteReadBuffer.readLongLengthEncoded(): Long =
    when (val length = readByteAsInt()) {
        0xFC -> readShortLe().toLong()
        0xFD -> {
            val bytes = readBytes(3).plus(0)
            (
                (bytes[0].toInt() and 0xff shl 24)
                    or (bytes[1].toInt() and 0xff shl 16)
                    or (bytes[2].toInt() and 0xff shl 8)
                    or (bytes[3].toInt() and 0xff)
            ).toLong()
        }
        0xFE -> readLongLe()
        else -> length.toLong()
    }

fun ByteReadBuffer.readStringLengthEncoded(): String =
    readBytesLengthEncoded().toString(Charsets.UTF_8)

fun ByteReadBuffer.readBytesLengthEncoded(): ByteArray {
    val length = readLongLengthEncoded()
    checkOrKdbcException(length in Int.MIN_VALUE..Int.MAX_VALUE) {
        "Length encoded value exceeds Int.MAX_VALUE"
    }
    return readBytes(length.toInt())
}
