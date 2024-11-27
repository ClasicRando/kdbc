package io.github.clasicrando.kdbc.mysql.buffer

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import kotlinx.io.Buffer
import kotlinx.io.DelicateIoApi
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlinx.io.readLongLe
import kotlinx.io.readShortLe
import kotlinx.io.writeLongLe
import kotlinx.io.writeShortLe
import kotlinx.io.writeToInternalBuffer

internal fun Source.readByteAsInt(): Int = this.readByte().toInt() and 0xff

internal fun Source.read3ByteIntLe(): Int {
    return (this.readByteAsInt() or (this.readByteAsInt() shl 8) or (this.readByteAsInt() shl 16))
}

internal fun Source.readLongLengthEncoded(): Long {
    val length = readByteAsInt()
    return when (length) {
        0xfc -> readShortLe().toLong() and 0xff_ff
        0xfd -> read3ByteIntLe().toLong()
        0xfe -> readLongLe()
        else -> length.toLong() and 0xff
    }
}

internal fun Source.readStringLengthEncoded(): String =
    readBytesLengthEncoded().toString(Charsets.UTF_8)

internal fun Source.readBytesLengthEncoded(): ByteArray {
    val length = readLongLengthEncoded()
    checkOrKdbcException(length in Int.MIN_VALUE..Int.MAX_VALUE) {
        "Length encoded value exceeds Int.MAX_VALUE"
    }
    return readByteArray(length.toInt())
}

internal fun Sink.writeLongLengthEncoded(value: Long) {
    when (value) {
        in 0..250 -> writeByte(value.toByte())
        in 251..0xff_ff -> {
            writeByte(0xfc.toByte())
            writeShortLe(value.toShort())
        }
        in 0x1_00_00..0xff_ff_ff -> {
            writeByte(0xfd.toByte())
            writeByte((value shl 8 and 0xff).toByte())
            writeByte((value shl 16 and 0xff).toByte())
            writeByte((value shl 24 and 0xff).toByte())
        }
        else -> {
            writeByte(0xfe.toByte())
            writeLongLe(value)
        }
    }
}

internal fun Sink.writeLengthEncoded(bytes: ByteArray) {
    writeLongLengthEncoded(bytes.size.toLong())
    write(bytes)
}

internal fun Sink.writeStringLengthEncoded(string: String) {
    writeLengthEncoded(string.toByteArray())
}

internal fun Sink.write3ByteIntLe(int: Int) {
    writeByte((int and 0xff).toByte())
    writeByte((int shr 8 and 0xff).toByte())
    writeByte((int shr 16 and 0xff).toByte())
}

private const val MAX_PACKET_DATA_LENGTH = 0xff_ff_ffL

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
    while (!tempBuffer.exhausted()) {
        val length = minOf(tempBuffer.size, MAX_PACKET_DATA_LENGTH)
        this.writeToInternalBuffer { buf ->
            buf.write3ByteIntLe(length.toInt())
            buf.writeByte(tempSequenceId.toByte())
            buf.write(tempBuffer, length)
        }
        tempSequenceId = if (tempSequenceId >= 255) 1 else tempSequenceId + 1
    }
    return tempSequenceId
}
