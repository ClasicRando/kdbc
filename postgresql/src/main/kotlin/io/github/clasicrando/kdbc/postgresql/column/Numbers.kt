package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError

/**
 * Implementation of [PgTypeEncoder] for [Short]. This maps to the `int2` or `smallint` type in a
 * postgresql database. Simply writes the [Short] value to the buffer.
 */
val shortTypeEncoder = PgTypeEncoder<Short>(PgType.Int2) { value, buffer ->
    buffer.writeShort(value)
}

/**
 * Implementation of [PgTypeDecoder] for [Short]. This maps to the `int2` or `smallint` type in a
 * postgresql database.
 *
 * ### Binary
 * Read the first [Short] value from the buffer.
 *
 * ### Text
 * Convert the [String] value into a [Short]
 *
 * @throws ColumnDecodeError if the [String] value cannot be converted to a [Short]
 */
val shortTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readShort()
        is PgValue.Text -> value.text
            .toShortOrNull()
            ?: columnDecodeError<Short>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Short",
            )
    }
}


/**
 * Implementation of [PgTypeEncoder] for [Int]. This maps to the `int4` or `int` type in a
 * postgresql database. Simply writes the [Int] value to the buffer.
 */
val intTypeEncoder = PgTypeEncoder<Int>(PgType.Int4) { value, buffer ->
    buffer.writeInt(value)
}

/**
 * Implementation of [PgTypeDecoder] for [Int]. This maps to the `int4` or `int` type in a
 * postgresql database.
 *
 * ### Binary
 * Read the first [Int] value from the buffer.
 *
 * ### Text
 * Convert the [String] value into a [Int]
 *
 * @throws ColumnDecodeError if the [String] value cannot be converted to a [Int]
 */
val intTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readInt()
        is PgValue.Text -> value.text
            .toIntOrNull()
            ?: columnDecodeError<Int>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Int",
            )
    }
}

/**
 * Implementation of [PgTypeEncoder] for [Long]. This maps to the `bigint` type in a postgresql
 * database. Simply writes the [Long] value to the buffer.
 */
val longTypeEncoder = PgTypeEncoder<Long>(PgType.Int8) { value, buffer ->
    buffer.writeLong(value)
}

/**
 * Implementation of [PgTypeDecoder] for [Long]. This maps to the `int8` or `bigint` type in a
 * postgresql database.
 *
 * ### Binary
 * Read the first [Long] value from the buffer.
 *
 * ### Text
 * Convert the [String] value into a [Long]
 *
 * @throws ColumnDecodeError if the [String] value cannot be converted to a [Long]
 */
val longTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readLong()
        is PgValue.Text -> value.text
            .toLongOrNull()
            ?: columnDecodeError<Long>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Long",
            )
    }
}

/**
 * Implementation of [PgTypeEncoder] for [Float]. This maps to the `float4` or `real` type in a
 * postgresql database. Simply writes the [Float] value to the buffer.
 */
val floatTypeEncoder = PgTypeEncoder<Float>(PgType.Float4) { value, buffer ->
    buffer.writeFloat(value)
}

/**
 * Implementation of [PgTypeDecoder] for [Float]. This maps to the `float4` or `real` type in a
 * postgresql database.
 *
 * ### Binary
 * Read the first [Float] value from the buffer.
 *
 * ### Text
 * Convert the [String] value into a [Float]
 *
 * @throws ColumnDecodeError if the [String] value cannot be converted to a [Float]
 */
val floatTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readFloat()
        is PgValue.Text -> value.text
            .toFloatOrNull()
            ?: columnDecodeError<Float>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Float",
            )
    }
}


/**
 * Implementation of [PgTypeEncoder] for [Double]. This maps to the `float8` or `double precision`
 * type in a postgresql database. Simply writes the [Short] value to the buffer.
 */
val doubleTypeEncoder = PgTypeEncoder<Double>(PgType.Float8) { value, buffer ->
    buffer.writeDouble(value)
}

/**
 * Implementation of [PgTypeDecoder] for [Double]. This maps to the `float8` or `double precision`
 * type in a postgresql database.
 *
 * ### Binary
 * Read the first [Double] value from the buffer.
 *
 * ### Text
 * Convert the [String] value into a [Double]
 *
 * @throws ColumnDecodeError if the [String] value cannot be converted to a [Double]
 */
val doubleTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readDouble()
        is PgValue.Text -> value.text
            .toDoubleOrNull()
            ?: columnDecodeError<Double>(
                type = value.typeData,
                reason = "Could not convert '${value.text}' into a Double",
            )
    }
}
