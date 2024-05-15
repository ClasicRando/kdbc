package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.type.ArrayLiteralParser
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.withNullability

/** Dummy [PgColumnDescription] to create a [PgValue.Text] instance for text decoding */
private val dummyFieldDescription = PgColumnDescription(
    fieldName = "",
    tableOid = 0,
    columnAttribute = 0,
    dataTypeSize = 0,
    pgType = PgType.Unknown,
    typeModifier = 0,
    formatCode = 1,
)

/**
 * Implementation of a [PgTypeDescription] for array types. Data supplied is the [PgType] of the
 * array type, the [KType] of the
 */
abstract class ArrayTypeDescription<T : Any>(
    pgType: PgType,
    private val innerType: PgTypeDescription<T>,
) : PgTypeDescription<List<T?>>(
    pgType = pgType,
    kType = List::class
        .createType(arguments = listOf(
            KTypeProjection.invariant(innerType.kType.withNullability(nullable = true))
        )),
) {
    /**
     * Encode a [List] of [T] into the argument [buffer]. This writes:
     *  1. The number of dimensions (always 1)
     *  2. Array header flags (not used so always 0)
     *  3. The OID of the [List] item type
     *  4. The number of items in the [List]
     *  5. The lower bound of the array (always 1)
     *  5. Each item encoded into the [buffer] (length prefixed if not null)
     *
     *  [pg source code](https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1272)
     */
    override fun encode(value: List<T?>, buffer: ByteWriteBuffer) {
        buffer.writeInt(1)
        buffer.writeInt(0)
        buffer.writeInt(innerType.pgType.oid)
        buffer.writeInt(value.size)
        buffer.writeInt(1)
        for (item in value) {
            if (item == null) {
                buffer.writeByte(-1)
                continue
            }
            buffer.writeLengthPrefixed {
                innerType.encode(item, this)
            }
        }
    }

    /**
     * Message contains:
     *  1. [Int] - Number of dimensions for the array. Must be 1 or an [IllegalArgumentException]
     *  is thrown
     *  2. [Int] - Array header flags (discarded)
     *  3. [Int] - Array item type OID
     *  4. [Int] - Length of the array
     *  5. [Int] - Lower bound of the array. Must be 1 or an [IllegalArgumentException] is thrown
     *  6. Dynamic - All items of the array as the number of bytes ([Int]) followed by that number
     *  of bytes
     *
     * [pg source code](https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1549)
     *
     * @throws columnDecodeError if the decode operation fails (reason supplied in [Exception])
     */
    override fun decodeBytes(value: PgValue.Binary): List<T?> {
        val dimensions = value.bytes.readInt()
        if (dimensions == 0) {
            return listOf()
        }
        checkOrColumnDecodeError(
            check = dimensions == 1,
            kType = kType,
            type = value.typeData,
        ) {
            "Attempted to decode an array of $dimensions dimensions. Only 1-dimensional " +
                    "arrays are supported"
        }
        // Discard flags value. No longer in use
        value.bytes.readInt()

        val elementTypeOid = value.bytes.readInt()
        val length = value.bytes.readInt()
        val lowerBound = value.bytes.readInt()
        checkOrColumnDecodeError(
            check = lowerBound == 1,
            kType = kType,
            type = value.typeData,
        ) { "Attempted to read an array with a lower bound other than 1. Got $lowerBound" }

        val fieldDescription = PgColumnDescription(
            fieldName = "",
            tableOid = 0,
            columnAttribute = 0,
            dataTypeSize = 0,
            pgType = PgType.fromOid(elementTypeOid),
            typeModifier = 0,
            formatCode = 1,
        )
        return List(length) {
            // Read length value but don't use it since a ReadBufferSlice cannot be constructed
            val elementLength = value.bytes.readInt()
            if (elementLength == -1) {
                return@List null
            }
            val slice = value.bytes.slice(elementLength)
            value.bytes.skip(elementLength)
            innerType.decode(PgValue.Binary(slice, fieldDescription))
        }
    }

    /**
     * Message is a text representation of the array (also called an array literal). Must be
     * wrapped in curly braces and each item is separated by a comma. For details see
     * [ArrayLiteralParser]. With all the items in text format, push each [String] item into a
     * [PgValue.Text] and decode into the resulting [List].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1017)
     *
     * @throws columnDecodeError if the decode operation fails (reason supplied in [Exception])
     */
    override fun decodeText(value: PgValue.Text): List<T?> {
        checkOrColumnDecodeError(
            check = value.text.startsWith('{') && value.text.endsWith('}'),
            kType = kType,
            type = value.typeData,
        ) { "An array literal value must start and end with a curly brace" }

        return ArrayLiteralParser.parse(value.text)
            .map {
                when {
                    it == null -> null
                    else -> {
                        val innerValue = PgValue.Text(it, dummyFieldDescription)
                        innerType.decode(innerValue)
                    }
                }
            }.toList()
    }
}
