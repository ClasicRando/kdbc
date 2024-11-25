package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.validateByte
import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.core.validateShort
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlinx.io.Sink
import kotlinx.io.writeDoubleLe
import kotlinx.io.writeFloatLe
import kotlinx.io.writeIntLe
import kotlinx.io.writeLongLe
import kotlinx.io.writeShortLe
import kotlin.reflect.typeOf

internal object BooleanTypeDescription :
    MySqlTypeDescription<Boolean>(dbType = MySqlType.Tiny, kType = typeOf<Boolean>()) {
    override val flags: ColumnFlags = ColumnFlags.BINARY + ColumnFlags.UNSIGNED

    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Tiny ||
            dbType == MySqlType.Short ||
            dbType == MySqlType.Int24 ||
            dbType == MySqlType.Long ||
            dbType == MySqlType.LongLong ||
            dbType == MySqlType.Bit
    }

    override fun encode(value: Boolean, buffer: Sink) {
        buffer.writeByte(if (value) 1 else 0)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Boolean {
        return decodeInt(value.bytes) != 0L
    }

    override fun decodeText(value: MySqlValue.Text): Boolean {
        return value.text.toLong() != 0L
    }
}

internal object TinyIntTypeDescription :
    MySqlTypeDescription<Byte>(dbType = MySqlType.Long, kType = typeOf<Byte>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Byte, buffer: Sink) {
        buffer.writeByte(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Byte {
        val int = decodeInt(value.bytes)
        return validateByte(int)
    }

    override fun decodeText(value: MySqlValue.Text): Byte {
        return value.text.toByte()
    }
}

internal object ShortTypeDescription :
    MySqlTypeDescription<Short>(dbType = MySqlType.Long, kType = typeOf<Short>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Short, buffer: Sink) {
        buffer.writeShortLe(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Short {
        val int = decodeInt(value.bytes)
        return validateShort(int)
    }

    override fun decodeText(value: MySqlValue.Text): Short {
        return value.text.toShort()
    }
}

internal object IntegerTypeDescription :
    MySqlTypeDescription<Int>(dbType = MySqlType.Long, kType = typeOf<Int>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Int, buffer: Sink) {
        buffer.writeIntLe(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Int {
        val int = decodeInt(value.bytes)
        return validateInt(int)
    }

    override fun decodeText(value: MySqlValue.Text): Int {
        return value.text.toInt()
    }
}

internal object LongTypeDescription :
    MySqlTypeDescription<Long>(dbType = MySqlType.Long, kType = typeOf<Long>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Long, buffer: Sink) {
        buffer.writeLongLe(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Long {
        return decodeInt(value.bytes)
    }

    override fun decodeText(value: MySqlValue.Text): Long {
        return value.text.toLong()
    }
}

private fun intCompatible(dbType: MySqlType): Boolean {
    return dbType == MySqlType.Tiny ||
        dbType == MySqlType.Short ||
        dbType == MySqlType.Long ||
        dbType == MySqlType.LongLong ||
        dbType == MySqlType.Int24
}

private fun decodeInt(buffer: ByteReadBuffer): Long {
    val byteCount = buffer.remaining()
    if (byteCount > 8) {
        throw KdbcException(
            "Expected integer value to be at most 8 bytes but found $byteCount bytes"
        )
    }
    return buffer.readIntLe(buffer.remaining())
}

internal object FloatTypeDescription : MySqlTypeDescription<Float>(dbType = MySqlType.Float, kType = typeOf<Float>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = floatCompatible(dbType)

    override fun encode(value: Float, buffer: Sink) {
        buffer.writeFloatLe(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Float {
        // This allows possible truncation of values but MySQL might send 8 byte floats that only
        // contain 4 bytes of actual data. Truncation is also the default behaviour of MySQL so this
        // operation is consistent
        return decodeFloat(value.bytes).toFloat()
    }

    override fun decodeText(value: MySqlValue.Text): Float {
        return value.text.toFloat()
    }
}

internal object DoubleTypeDescription : MySqlTypeDescription<Double>(dbType = MySqlType.Double, kType = typeOf<Double>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = floatCompatible(dbType)

    override fun encode(value: Double, buffer: Sink) {
        buffer.writeDoubleLe(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Double {
        return decodeFloat(value.bytes)
    }

    override fun decodeText(value: MySqlValue.Text): Double {
        return value.text.toDouble()
    }
}

private fun floatCompatible(dbType: MySqlType): Boolean {
    return dbType == MySqlType.Float || dbType == MySqlType.Double
}

private fun decodeFloat(buffer: ByteReadBuffer): Double {
    return when (val length = buffer.remaining()) {
        4 -> buffer.readFloatLe().toDouble()
        8 -> buffer.readDoubleLe()
        else -> {
            throw KdbcException(
                "Decoding of floating point number requires 4 or 8 bytes but found $length. " +
                    "Note that decimal and floating point numbers are not interchangeable"
            )
        }
    }
}
