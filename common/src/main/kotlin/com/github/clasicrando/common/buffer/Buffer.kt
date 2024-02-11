package com.github.clasicrando.common.buffer

import java.io.OutputStream
import java.nio.charset.Charset

interface Buffer {
    fun writeByte(byte: Byte)

    fun toByteArray(): ByteArray

    fun release()
}

inline fun <B : Buffer> B.buildBytes(block: (B) -> Unit): ByteArray {
    var cause: Throwable? = null
    return try {
        block(this)
        this.toByteArray()
    } catch (ex: Throwable) {
        cause = ex
        throw cause
    } finally {
        try {
            this.release()
        } catch (ex2: Throwable) {
            cause?.addSuppressed(ex2)
        }
    }
}

fun Buffer.writeMany(vararg bytes: Byte) {
    for (i in bytes.indices) {
        writeByte(bytes[i])
    }
}

fun Buffer.writeShort(short: Short) {
    this.writeMany(
        (short.toInt() ushr 8 and 0xff).toByte(),
        (short.toInt() and 0xff).toByte(),
    )
}

fun Buffer.writeInt(int: Int) {
    this.writeMany(
        (int ushr 24 and 0xff).toByte(),
        (int ushr 16 and 0xff).toByte(),
        (int ushr 8 and 0xff).toByte(),
        (int and 0xff).toByte(),
    )
}

fun Buffer.writeLong(long: Long) {
    this.writeMany(
        (long ushr 56 and 0xffL).toByte(),
        (long ushr 48 and 0xffL).toByte(),
        (long ushr 40 and 0xffL).toByte(),
        (long ushr 32 and 0xffL).toByte(),
        (long ushr 24 and 0xffL).toByte(),
        (long ushr 16 and 0xffL).toByte(),
        (long ushr 8 and 0xffL).toByte(),
        (long and 0xffL).toByte(),
    )
}

fun Buffer.writeFloat(float: Float) {
    this.writeInt(float.toBits())
}

fun Buffer.writeDouble(double: Double) {
    this.writeLong(double.toBits())
}

fun Buffer.writeFully(byteArray: ByteArray, offset: Int = 0, length: Int = byteArray.size) {
    for (i in offset..<(offset + length)) {
        this.writeByte(byteArray[i])
    }
}

fun Buffer.writeText(text: String, charset: Charset = Charsets.UTF_8) {
    this.writeFully(text.toByteArray(charset = charset))
}

fun Buffer.outputStream(): OutputStream = object : OutputStream() {
    override fun write(b: Int) {
        this@outputStream.writeByte(b.toByte())
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        this@outputStream.writeFully(byteArray = b, offset = off, length = len)
    }

    override fun close() {}
}
