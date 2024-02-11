package com.github.clasicrando.postgresql.statement

import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.type.PgJson
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeShort
import java.io.OutputStream

class PgArguments internal constructor(
    private val typeRegistry: PgTypeRegistry,
    private val parameters: List<Any?>,
    private val statement: PgPreparedStatement,
) {
    private val buffer = mutableListOf<Byte>()
    private var parameterCount = 0

    fun writeToBuffer(buffer: BytePacketBuilder) {
        buffer.writeShort(parameters.size.toShort())
        for (parameter in parameters) {
            add(parameter)
        }
        buffer.writeFully(this.buffer.toByteArray())
//        for ((value, oid) in parameters.zip(statement.parameterTypeOids)) {
//            buffer.writeLengthPrefixedWithoutSelf {
//                if (value is PgJson) {
//                    if (oid == PgType.JSON || oid == PgType.JSON_ARRAY) {
//                        this.writeByte(' '.code.toByte())
//                    } else {
//                        this.writeByte(1)
//                    }
//                }
//                typeRegistry.encode(value, this)
//            }
//        }
    }

    private fun MutableList<Byte>.writeMany(vararg bytes: Byte) {
        for (i in bytes.indices) {
            add(bytes[i])
        }
    }

    internal inline fun writeLengthPrefixed(block: (PgArguments) -> Unit) {
        val startIndex = buffer.size
        writeInt(0)
        block(this)
        val currentSize = buffer.size - startIndex - 4
        buffer[startIndex] = (currentSize ushr 24 and 0xff).toByte()
        buffer[startIndex + 1] = (currentSize ushr 16 and 0xff).toByte()
        buffer[startIndex + 2] = (currentSize ushr 8 and 0xff).toByte()
        buffer[startIndex + 3] = (currentSize and 0xff).toByte()
    }

    fun writeByte(byte: Byte) {
        buffer.add(byte)
    }

    fun writeShort(short: Short) {
        buffer.writeMany(
            (short.toInt() ushr 8 and 0xff).toByte(),
            (short.toInt() and 0xff).toByte(),
        )
    }

    fun writeInt(int: Int) {
        buffer.writeMany(
            (int ushr 24 and 0xff).toByte(),
            (int ushr 16 and 0xff).toByte(),
            (int ushr 8 and 0xff).toByte(),
            (int and 0xff).toByte(),
        )
    }

    fun writeLong(long: Long) {
        buffer.writeMany(
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

    fun writeFloat(float: Float) {
        writeInt(float.toBits())
    }

    fun writeDouble(double: Double) {
        writeLong(double.toBits())
    }

    fun writeFully(byteArray: ByteArray, offset: Int = 0, length: Int = byteArray.size) {
        for (i in offset..<(offset + length)) {
            buffer.add(byteArray[i])
        }
    }

    fun writeText(text: String) {
        writeFully(text.toByteArray(charset = Charsets.UTF_8))
    }

    fun add(parameter: Any?) {
        parameterCount++
        if (parameter is PgJson) {
            statement.parameterTypeOids.getOrNull(parameterCount)?.let { oid ->
                if (oid == PgType.JSON || oid == PgType.JSON_ARRAY) {
                    buffer.add(' '.code.toByte())
                } else {
                    buffer.add(1)
                }
            }
        }
        writeLengthPrefixed {
            typeRegistry.encode(parameter, it)
        }
    }

    fun toByteArray(): ByteArray = buffer.toByteArray()

    fun outputStream(): OutputStream = object : OutputStream() {
        override fun write(b: Int) {
            buffer.add(b.toByte())
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            writeFully(byteArray = b, offset = off, length = len)
        }

        override fun close() {}
    }
}
