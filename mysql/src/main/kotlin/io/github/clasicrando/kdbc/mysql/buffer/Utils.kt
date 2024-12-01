package io.github.clasicrando.kdbc.mysql.buffer

import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import kotlinx.io.Buffer
import kotlinx.io.DelicateIoApi
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlinx.io.readLongLe
import kotlinx.io.readShortLe
import kotlinx.io.readString
import kotlinx.io.writeLongLe
import kotlinx.io.writeShortLe
import kotlinx.io.writeToInternalBuffer

internal fun Source.readByteAsInt(): Int = this.readByte().toInt() and 0xff

internal fun Source.read3ByteIntLe(): Int {
    return (this.readByteAsInt() or (this.readByteAsInt() shl 8) or (this.readByteAsInt() shl 16))
}

/**
 * Read a [Long] as a variable number of bytes where the first byte prescribes how many subsequent
 * bytes (if any) are needed to decode the value. The header byte is:
 * - 0xfc, read 2 more bytes
 * - 0xfd, read 3 more bytes
 * - 0xfe, read 8 more bytes
 * - other values indicate that the first byte is the length
 */
internal fun Source.readLongLengthEncoded(): Long {
    val length = readByteAsInt()
    return when (length) {
        0xfc -> readShortLe().toLong() and 0xff_ff
        0xfd -> read3ByteIntLe().toLong()
        0xfe -> readLongLe()
        else -> length.toLong() and 0xff
    }
}

/**
 * Read a group bytes as a UTF-8 encoded [String] where the length of the bytes to decode is encoded
 * as a prefix to the bytes. The method is equivalent to:
 * ```
 * readString(byteCount = readLongLengthEncoded())
 * ```
 */
internal fun Source.readStringLengthEncoded(): String {
    val length = readLongLengthEncoded()
    return readString(byteCount = length)
}

/**
 * Read a group of bytes where the number of bytes is length encoded. This is equivalent to:
 * ```
 * readByteArray(readLongLengthEncoded())
 * ```
 *
 * with an extra check to ensure the length is an int since a [ByteArray]'s size is capped to
 * [Int.MAX_VALUE].
 */
internal fun Source.readBytesLengthEncoded(): ByteArray {
    val length = readLongLengthEncoded()
    return readByteArray(validateInt(length))
}

/**
 * Write a length encode integer where the number of bytes written depends on the size value.
 * - 0..250 -> write single byte as the length
 * - 251..0xff_ff -> write 0xfc then the length as a [Short]
 * - 0x010000..0xffffff -> write 0xfd then the length as 3 [Byte]s
 * - other values are written with 0xfe as the header and the entire [Long] value
 */
internal fun Sink.writeLongLengthEncoded(value: Long) {
    when (value) {
        in 0..250 -> writeByte(value.toByte())
        in 251..0xff_ff -> {
            writeByte(0xfc.toByte())
            writeShortLe(value.toShort())
        }
        in 0x01_00_00..0xff_ff_ff -> {
            writeByte(0xfd.toByte())
            write3ByteIntLe(value.toInt())
        }
        else -> {
            writeByte(0xfe.toByte())
            writeLongLe(value)
        }
    }
}

/**
 * Write a group of bytes where the size of the group is encoded using [writeLongLengthEncoded] and
 * the actual bytes are written as is.
 */
internal fun Sink.writeLengthEncoded(bytes: ByteArray) {
    writeLongLengthEncoded(bytes.size.toLong())
    write(bytes)
}

/** Write a [String] as a group of bytes */
internal fun Sink.writeStringLengthEncoded(string: String) {
    writeLengthEncoded(string.toByteArray())
}

internal fun Sink.write3ByteIntLe(int: Int) {
    writeByte((int and 0xff).toByte())
    writeByte((int shr 8 and 0xff).toByte())
    writeByte((int shr 16 and 0xff).toByte())
}

private const val MAX_PACKET_DATA_LENGTH = MySqlStream.MAX_PACKET_SIZE.toLong() - 4

@OptIn(DelicateIoApi::class)
internal inline fun Sink.writeLengthEncoded(crossinline block: Sink.() -> Unit) {
    val tempBuffer = Buffer()
    block(tempBuffer)
    this.writeToInternalBuffer { buf ->
        buf.writeLongLengthEncoded(tempBuffer.size)
        buf.write(tempBuffer, tempBuffer.size)
    }
}

/**
 * Write data contents to [Sink], batching the written contents into packets that meet the size of
 * the MySQL spec. Returns the new packet sequence ID
 */
@OptIn(DelicateIoApi::class)
internal inline fun Sink.writePackets(
    currentSequenceId: Int,
    crossinline block: Sink.() -> Unit,
): Int {
    var tempSequenceId = currentSequenceId
    val tempBuffer = Buffer()
    block(tempBuffer)
    do {
        val length = minOf(tempBuffer.size, MAX_PACKET_DATA_LENGTH)
        this.writeToInternalBuffer { buf ->
            buf.write3ByteIntLe(length.toInt())
            buf.writeByte(tempSequenceId.toByte())
            buf.write(tempBuffer, length)
        }
        tempSequenceId = if (tempSequenceId >= 255) 1 else tempSequenceId + 1
    } while (!tempBuffer.exhausted())
    return tempSequenceId
}
