package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.connection.IntBitFlags
import io.github.clasicrando.kdbc.core.validateByte
import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.core.validateShort
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlinx.io.Sink
import kotlinx.io.writeDoubleLe
import kotlinx.io.writeFloatLe
import kotlinx.io.writeIntLe
import kotlinx.io.writeLongLe
import kotlinx.io.writeShortLe
import kotlin.reflect.typeOf

/**
 * Implementation of a [MySqlTypeDescription] for the [Boolean] type. Accepts TINY, SHORT, INT24,
 * LONG, LONGLONG and BIT.
 */
internal object BooleanTypeDescription :
    MySqlTypeDescription<Boolean>(dbType = MySqlType.Tiny, kType = typeOf<Boolean>()) {
    override val flags: IntBitFlags = ColumnFlags.BINARY + ColumnFlags.UNSIGNED

    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Tiny ||
            dbType == MySqlType.Short ||
            dbType == MySqlType.Int24 ||
            dbType == MySqlType.Long ||
            dbType == MySqlType.LongLong ||
            dbType == MySqlType.Bit
    }

    /** Writes 1 byte as 1 for true and 0 for false */
    override fun encode(value: Boolean, buffer: Sink) {
        buffer.writeByte(if (value) 1 else 0)
    }

    /** Reads an integer value and return true when the value is not 0, otherwise false */
    override fun decodeBytes(value: MySqlValue.Binary): Boolean {
        return decodeInt(value.bytes) != 0L
    }

    /** Convert the text into a [Long] and return true when the value is not 0, otherwise false */
    override fun decodeText(value: MySqlValue.Text): Boolean {
        return value.text.toLong() != 0L
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Byte] type. Accepts TINY, SHORT, INT24, LONG
 * and LONGLONG.
 */
internal object TinyIntTypeDescription :
    MySqlTypeDescription<Byte>(dbType = MySqlType.Tiny, kType = typeOf<Byte>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Byte, buffer: Sink) {
        buffer.writeByte(value)
    }

    /**
     * Decode using [decodeInt] then checking if the value is a valid [Byte]
     *
     * @throws io.github.clasicrando.kdbc.core.exceptions.KdbcException if the value is not a valid
     *   [Byte]
     */
    override fun decodeBytes(value: MySqlValue.Binary): Byte {
        val int = decodeInt(value.bytes)
        return validateByte(int)
    }

    /** Parses the text as a [Byte] */
    override fun decodeText(value: MySqlValue.Text): Byte {
        return value.text.toByte()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Short] type. Accepts TINY, SHORT, INT24, LONG
 * and LONGLONG.
 */
internal object ShortTypeDescription :
    MySqlTypeDescription<Short>(dbType = MySqlType.Short, kType = typeOf<Short>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Short, buffer: Sink) {
        buffer.writeShortLe(value)
    }

    /**
     * Decode using [decodeInt] then checking if the value is a valid [Short]
     *
     * @throws io.github.clasicrando.kdbc.core.exceptions.KdbcException if the value is not a valid
     *   [Short]
     */
    override fun decodeBytes(value: MySqlValue.Binary): Short {
        val int = decodeInt(value.bytes)
        return validateShort(int)
    }

    /** Parses the text as a [Short] */
    override fun decodeText(value: MySqlValue.Text): Short {
        return value.text.toShort()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Int] type. Accepts TINY, SHORT, INT24, LONG
 * and LONGLONG.
 */
internal object IntegerTypeDescription :
    MySqlTypeDescription<Int>(dbType = MySqlType.Long, kType = typeOf<Int>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Int, buffer: Sink) {
        buffer.writeIntLe(value)
    }

    /**
     * Decode using [decodeInt] then checking if the value is a valid [Int]
     *
     * @throws io.github.clasicrando.kdbc.core.exceptions.KdbcException if the value is not a valid
     *   [Int]
     */
    override fun decodeBytes(value: MySqlValue.Binary): Int {
        val int = decodeInt(value.bytes)
        return validateInt(int)
    }

    /** Parses the text as a [Int] */
    override fun decodeText(value: MySqlValue.Text): Int {
        return value.text.toInt()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Long] type. Accepts TINY, SHORT, INT24, LONG
 * and LONGLONG.
 */
internal object LongTypeDescription :
    MySqlTypeDescription<Long>(dbType = MySqlType.LongLong, kType = typeOf<Long>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = intCompatible(dbType)

    override fun encode(value: Long, buffer: Sink) {
        buffer.writeLongLe(value)
    }

    /** Decode using [decodeInt] */
    override fun decodeBytes(value: MySqlValue.Binary): Long {
        return decodeInt(value.bytes)
    }

    /** Parses the text as a [Long] */
    override fun decodeText(value: MySqlValue.Text): Long {
        return value.text.toLong()
    }
}

/** Returns true if [dbType] is TINY, SHORT, INT24, LONG or LONGLONG */
private fun intCompatible(dbType: MySqlType): Boolean {
    return dbType.inner == MySqlType.Tiny.inner ||
        dbType.inner == MySqlType.Short.inner ||
        dbType.inner == MySqlType.Long.inner ||
        dbType.inner == MySqlType.LongLong.inner ||
        dbType.inner == MySqlType.Int24.inner
}

/**
 * Read all remaining bytes in the buffer as a [Long]. The value might not actually be a long but
 * the convenience of decoding to [Long] and then validating ranges later makes it easier. For
 * example, if there are only 2 bytes in the buffer than the value is actually a [Short] so the
 * [Long] with only have 2 bytes possibly populated.
 */
private fun decodeInt(buffer: ByteReadBuffer): Long {
    val byteCount = buffer.remaining
    if (byteCount > 8) {
        throw MySqlException(
            "Expected integer value to be at most 8 bytes but found $byteCount bytes"
        )
    }
    return buffer.readIntLe(buffer.remaining)
}

/** Implementation of a [MySqlTypeDescription] for the [Float] type. Accepts FLOAT and DOUBLE */
internal object FloatTypeDescription :
    MySqlTypeDescription<Float>(dbType = MySqlType.Float, kType = typeOf<Float>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = floatCompatible(dbType)

    override fun encode(value: Float, buffer: Sink) {
        buffer.writeFloatLe(value)
    }

    /**
     * Decode bytes using [decodeFloat] and convert to a [Float]. This allows possible truncation of
     * values but MySQL might send 8 byte floats that only contain 4 bytes of actual data.
     * Truncation is also the default behaviour of MySQL so this operation is consistent
     */
    override fun decodeBytes(value: MySqlValue.Binary): Float {
        return decodeFloat(value.bytes).toFloat()
    }

    /** Parses the text as a [Float] */
    override fun decodeText(value: MySqlValue.Text): Float {
        return value.text.toFloat()
    }
}

/** Implementation of a [MySqlTypeDescription] for the [Double] type. Accepts FLOAT and DOUBLE */
internal object DoubleTypeDescription :
    MySqlTypeDescription<Double>(dbType = MySqlType.Double, kType = typeOf<Double>()) {
    override fun isCompatible(dbType: MySqlType): Boolean = floatCompatible(dbType)

    override fun encode(value: Double, buffer: Sink) {
        buffer.writeDoubleLe(value)
    }

    /** Decode bytes using [decodeFloat] */
    override fun decodeBytes(value: MySqlValue.Binary): Double {
        return decodeFloat(value.bytes)
    }

    /** Parses the text as a [Double] */
    override fun decodeText(value: MySqlValue.Text): Double {
        return value.text.toDouble()
    }
}

/** Returns true if the [dbType] is FLOAT or DOUBLE */
private fun floatCompatible(dbType: MySqlType): Boolean {
    return dbType == MySqlType.Float || dbType == MySqlType.Double
}

/**
 * Check the size of the remaining bytes. For 4 bytes, read as a [Float] and coerce to a [Double].
 * For 8 bytes, read a [Double].
 *
 * @throws MySqlException if the number of bytes remaining in the buffer is not 4 or 8
 */
private fun decodeFloat(buffer: ByteReadBuffer): Double {
    return when (val length = buffer.remaining) {
        4 -> buffer.readFloatLe().toDouble()
        8 -> buffer.readDoubleLe()
        else -> {
            throw MySqlException(
                "Decoding of floating point number requires 4 or 8 bytes but found $length. " +
                    "Note that decimal and floating point numbers are not interchangeable"
            )
        }
    }
}
