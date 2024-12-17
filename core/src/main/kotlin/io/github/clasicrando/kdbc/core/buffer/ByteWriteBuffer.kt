package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlinx.io.Buffer
import kotlinx.io.readTo

public class ByteWriteBuffer(@PublishedApi internal val capacity: Int) {
    @PublishedApi internal val innerBuffer: ByteArray = ByteArray(capacity)
    @PublishedApi internal var offset: Int = 0

    public inline val isEmpty: Boolean
        get() = offset == 0

    public inline val bytesWritten: Int
        get() = offset

    public inline val remaining: Int
        get() = capacity - offset

    private fun checkRemaining(required: Int) {
        if (remaining < required) {
            throw BufferOverflow(remaining, required)
        }
    }

    public fun writeByte(byte: Byte) {
        checkRemaining(1)
        innerBuffer[offset++] = byte
    }

    public fun writeShort(short: Short) {
        checkRemaining(2)
        innerBuffer[offset++] = (short.toInt() ushr 8 and 0xff).toByte()
        innerBuffer[offset++] = (short.toInt() and 0xff).toByte()
    }

    public fun writeShortLe(short: Short) {
        checkRemaining(2)
        innerBuffer[offset++] = (short.toInt() and 0xff).toByte()
        innerBuffer[offset++] = (short.toInt() ushr 8 and 0xff).toByte()
    }

    public fun writeInt(int: Int) {
        checkRemaining(4)
        innerBuffer[offset++] = (int ushr 24 and 0xff).toByte()
        innerBuffer[offset++] = (int ushr 16 and 0xff).toByte()
        innerBuffer[offset++] = (int ushr 8 and 0xff).toByte()
        innerBuffer[offset++] = (int and 0xff).toByte()
    }

    public fun writeIntLe(int: Int) {
        checkRemaining(4)
        innerBuffer[offset++] = (int and 0xff).toByte()
        innerBuffer[offset++] = (int ushr 8 and 0xff).toByte()
        innerBuffer[offset++] = (int ushr 16 and 0xff).toByte()
        innerBuffer[offset++] = (int ushr 24 and 0xff).toByte()
    }

    public fun writeLong(long: Long) {
        checkRemaining(8)
        innerBuffer[offset++] = (long ushr 56 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 48 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 40 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 32 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 24 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 16 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 8 and 0xffL).toByte()
        innerBuffer[offset++] = (long and 0xffL).toByte()
    }

    public fun writeLongLe(long: Long) {
        checkRemaining(8)
        innerBuffer[offset++] = (long and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 8 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 16 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 24 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 32 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 40 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 48 and 0xffL).toByte()
        innerBuffer[offset++] = (long ushr 56 and 0xffL).toByte()
    }

    public fun writeBytes(bytes: ByteArray) {
        if (bytes.isEmpty()) {
            return
        }
        checkRemaining(bytes.size)
        bytes.copyInto(innerBuffer, destinationOffset = offset)
        offset += bytes.size
    }

    public fun writeBuffer(buffer: Buffer) {
        if (buffer.exhausted()) {
            return
        }
        if (buffer.size > Int.MAX_VALUE) {
            throw KdbcException("Buffer supplied has a length larger than Int.MAX_VALUE")
        }

        checkRemaining(buffer.size.toInt())
        buffer.readTo(sink = this.innerBuffer, startIndex = offset, endIndex = offset + buffer.size.toInt())
        offset += buffer.size.toInt()
    }

    public inline fun writeLengthPrefixedAsInt(
        includeLength: Boolean = false,
        block: ByteWriteBuffer.() -> Unit,
    ) {
        val start = offset
        writeInt(0)
        block(this)
        val end = offset
        val length = end - start - if (includeLength) 0 else 4
        offset = start
        writeInt(length)
        offset = end
    }

    public inline fun <T> useAsReadBuffer(block: (ByteReadBuffer) -> T): T {
        return try {
            block(ByteReadBuffer(innerBuffer = innerBuffer, offset = 0, size = offset))
        } finally {
            reset()
        }
    }

    public fun toReadBuffer(): ByteReadBuffer {
        val result = ByteReadBuffer(innerBuffer.copyOfRange(fromIndex = 0, toIndex = offset))
        reset()
        return result
    }

    public fun copy(other: ByteWriteBuffer) {
        checkRemaining(other.bytesWritten)
        other.innerBuffer.copyInto(
            destination = this.innerBuffer,
            destinationOffset = this.offset,
            startIndex = 0,
            endIndex = other.offset,
        )
        offset += other.bytesWritten
    }

    public fun writeEmpty(byteCount: Int) {
        checkRemaining(byteCount)
        offset += byteCount
    }

    public fun reset() {
        offset = 0
    }
}

public fun ByteWriteBuffer.writeFloat(float: Float) {
    writeInt(float.toBits())
}

public fun ByteWriteBuffer.writeFloatLe(float: Float) {
    writeIntLe(float.toBits())
}

public fun ByteWriteBuffer.writeDouble(double: Double) {
    writeLong(double.toBits())
}

public fun ByteWriteBuffer.writeDoubleLe(double: Double) {
    writeLongLe(double.toBits())
}

public fun ByteWriteBuffer.writeString(string: String) {
    if (string.isEmpty()) {
        return
    }
    writeBytes(string.toByteArray())
}

public fun ByteWriteBuffer.writeCString(string: String) {
    writeString(string)
    writeByte(0)
}
