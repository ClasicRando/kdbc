package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [Short] type. This maps to the `int2`/`smallint`
 * type in a postgresql database.
 */
object SmallIntTypeDescription : PgTypeDescription<Short>(
    pgType = PgType.Int2,
    kType = typeOf<Short>(),
) {
    /** Simply writes the [Short] value to the buffer */
    override fun encode(value: Short, buffer: ByteWriteBuffer) {
        buffer.writeShort(value)
    }

    /** Read the first [Short] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Short {
        return value.bytes.readShort()
    }

    /**
     * Convert the [String] value into a [Short]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Short]
     */
    override fun decodeText(value: PgValue.Text): Short {
        return value.text
            .toShortOrNull()
            ?: columnDecodeError<Short>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Short",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Short]. This maps to the `int2[]`/`smallint[]`
 * type in a postgresql database.
 */
object SmallIntArrayTypeDescription : ArrayTypeDescription<Short>(
    pgType = PgType.Int2Array,
    innerType = SmallIntTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [Int] type. This maps to the `int4`/`integer`
 * type in a postgresql database.
 */
object IntTypeDescription : PgTypeDescription<Int>(
    pgType = PgType.Int4,
    kType = typeOf<Int>(),
) {
    /** Simply writes the [Int] value to the buffer */
    override fun encode(value: Int, buffer: ByteWriteBuffer) {
        buffer.writeInt(value)
    }

    /** Read the first [Int] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Int {
        return value.bytes.readInt()
    }

    /**
     * Convert the [String] value into a [Int]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Int]
     */
    override fun decodeText(value: PgValue.Text): Int {
        return value.text
            .toIntOrNull()
            ?: columnDecodeError<Int>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Int",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Int]. This maps to the `int4[]`/`integer[]` type
 * in a postgresql database.
 */
object IntArrayTypeDescription : ArrayTypeDescription<Int>(
    pgType = PgType.Int4Array,
    innerType = IntTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [Int] type. This maps to the `oid` type in a
 * postgresql database.
 */
object OidTypeDescription : PgTypeDescription<Int>(
    pgType = PgType.Oid,
    kType = typeOf<Int>(),
) {
    /** Simply writes the [Int] value to the buffer */
    override fun encode(value: Int, buffer: ByteWriteBuffer) {
        buffer.writeInt(value)
    }

    /** Read the first [Int] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Int {
        return value.bytes.readInt()
    }

    /**
     * Convert the [String] value into a [Int]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Int]
     */
    override fun decodeText(value: PgValue.Text): Int {
        return value.text
            .toIntOrNull()
            ?: columnDecodeError<Int>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Int",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Int]. This maps to the `oid[]` type in a
 * postgresql database.
 */
object OidArrayTypeDescription : ArrayTypeDescription<Int>(
    pgType = PgType.OidArray,
    innerType = IntTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [Long] type. This maps to the `int8`/`bigint`
 * type in a postgresql database.
 */
object BigIntTypeDescription : PgTypeDescription<Long>(
    pgType = PgType.Int8,
    kType = typeOf<Long>(),
) {
    /** Simply writes the [Long] value to the buffer */
    override fun encode(value: Long, buffer: ByteWriteBuffer) {
        buffer.writeLong(value)
    }

    /** Read the first [Long] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Long {
        return value.bytes.readLong()
    }

    /**
     * Convert the [String] value into a [Long]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Long]
     */
    override fun decodeText(value: PgValue.Text): Long {
        return value.text
            .toLongOrNull()
            ?: columnDecodeError<Long>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Long",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Long]. This maps to the `int8[]`/`bigint[]` type
 * in a postgresql database.
 */
object BigIntArrayTypeDescription : ArrayTypeDescription<Long>(
    pgType = PgType.Int8Array,
    innerType = BigIntTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [Float] type. This maps to the `float4`/`real`
 * type in a postgresql database.
 */
object RealTypeDescription : PgTypeDescription<Float>(
    pgType = PgType.Float4,
    kType = typeOf<Float>(),
) {
    /** Simply writes the [Float] value to the buffer */
    override fun encode(value: Float, buffer: ByteWriteBuffer) {
        buffer.writeFloat(value)
    }

    /** Read the first [Long] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Float {
        return value.bytes.readFloat()
    }

    /**
     * Convert the [String] value into a [Float]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Float]
     */
    override fun decodeText(value: PgValue.Text): Float {
        return value.text
            .toFloatOrNull()
            ?: columnDecodeError<Float>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Float",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Float]. This maps to the `float4[]`/`real[]`
 * type in a postgresql database.
 */
object RealArrayTypeDescription : ArrayTypeDescription<Float>(
    pgType = PgType.Float4Array,
    innerType = RealTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [Double] type. This maps to the
 * `float8`/`double precision` type in a postgresql database.
 */
object DoublePrecisionTypeDescription : PgTypeDescription<Double>(
    pgType = PgType.Float8,
    kType = typeOf<Double>(),
) {
    /** Simply writes the [Double] value to the buffer */
    override fun encode(value: Double, buffer: ByteWriteBuffer) {
        buffer.writeDouble(value)
    }

    /** Read the first [Long] value from the buffer. */
    override fun decodeBytes(value: PgValue.Binary): Double {
        return value.bytes.readDouble()
    }

    /**
     * Convert the [String] value into a [Double]
     *
     * @throws ColumnDecodeError if the [String] value cannot be converted to a [Double]
     */
    override fun decodeText(value: PgValue.Text): Double {
        return value.text
            .toDoubleOrNull()
            ?: columnDecodeError<Double>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Double",
            )
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [Double]. This maps to the
 * `float8[]`/`double precision[]` type in a postgresql database.
 */
object DoublePrecisionArrayTypeDescription : ArrayTypeDescription<Double>(
    pgType = PgType.Float8Array,
    innerType = DoublePrecisionTypeDescription,
)
