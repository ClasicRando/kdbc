package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import java.io.OutputStream
import java.nio.charset.Charset

interface WriteBuffer : AutoRelease {
    fun writeByte(byte: Byte)

    fun writeShort(short: Short) {
        this.writeByte((short.toInt() ushr 8 and 0xff).toByte())
        this.writeByte((short.toInt() and 0xff).toByte())
    }

    fun writeInt(int: Int) {
        this.writeByte((int ushr 24 and 0xff).toByte())
        this.writeByte((int ushr 16 and 0xff).toByte())
        this.writeByte((int ushr 8 and 0xff).toByte())
        this.writeByte((int and 0xff).toByte())
    }

    fun writeLong(long: Long) {
        this.writeByte((long ushr 56 and 0xffL).toByte())
        this.writeByte((long ushr 48 and 0xffL).toByte())
        this.writeByte((long ushr 40 and 0xffL).toByte())
        this.writeByte((long ushr 32 and 0xffL).toByte())
        this.writeByte((long ushr 24 and 0xffL).toByte())
        this.writeByte((long ushr 16 and 0xffL).toByte())
        this.writeByte((long ushr 8 and 0xffL).toByte())
        this.writeByte((long and 0xffL).toByte())
    }

    fun writeFloat(float: Float) {
        this.writeInt(float.toBits())
    }

    fun writeDouble(double: Double) {
        this.writeLong(double.toBits())
    }

    fun writeFully(byteArray: ByteArray, offset: Int, length: Int) {
        for (i in offset..<(offset + length)) {
            this.writeByte(byteArray[i])
        }
    }
}

fun WriteBuffer.writeFully(byteArray: ByteArray) {
    for (i in byteArray.indices) {
        this.writeByte(byteArray[i])
    }
}

fun WriteBuffer.writeText(text: String, charset: Charset = Charsets.UTF_8) {
    this.writeFully(text.toByteArray(charset = charset))
}

fun WriteBuffer.outputStream(): OutputStream = object : OutputStream() {
    override fun write(b: Int) {
        this@outputStream.writeByte(b.toByte())
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        this@outputStream.writeFully(byteArray = b, offset = off, length = len)
    }

    override fun close() {}
}
