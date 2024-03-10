package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.array.ArrayLiteralParser
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Function for creating new [PgArrayTypeEncoder] instances. Requires an inner [encoder], the
 * [pgType] of the array and all [compatibleTypes] of the array.
 */
inline fun <reified T : Any, E : PgTypeEncoder<T>> arrayTypeEncoder(
    encoder: E,
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
): PgTypeEncoder<List<T?>> {
    return PgArrayTypeEncoder(
        encoder = encoder,
        pgType = pgType,
        compatibleTypes = compatibleTypes,
        encodeTypes = listOf(typeOf<List<T?>>()),
    )
}

/**
 * Special function to create new [PgArrayTypeEncoder] instances without the use of an inline
 * function. This requires the caller to provide the correct [KType] as an argument rather than
 * generating the type using the type [T] as a reified argument.
 */
internal fun <T : Any, E : PgTypeEncoder<T>> arrayTypeEncoder(
    encoder: E,
    pgType: PgType,
    arrayType: KType,
    compatibleTypes: Array<PgType>? = null,
): PgTypeEncoder<List<T?>> {
    return PgArrayTypeEncoder(
        encoder = encoder,
        pgType = pgType,
        compatibleTypes = compatibleTypes,
        encodeTypes = listOf(arrayType),
    )
}

/**
 * Implementation of [PgTypeEncoder] for a [List] of [T]. This requires an [encoder] of the [List]
 * item type to allow for encoding of the items into the argument buffer. The 1 limitation of the
 * array encoder is that it does not allow for multidimensional arrays (i.e. [encoder] is a
 * [PgArrayTypeEncoder]).
 */
@PublishedApi
internal class PgArrayTypeEncoder<T : Any, E : PgTypeEncoder<T>>(
    private val encoder: E,
    override var pgType: PgType,
    override val compatibleTypes: Array<PgType>?,
    override val encodeTypes: List<KType>,
) : PgTypeEncoder<List<T?>> {
    init {
        require(encoder !is PgArrayTypeEncoder<*, *>) {
            "Multi dimensional arrays are not supported"
        }
    }

    /**
     * Encode a [List] of [T] into the argument [buffer]. This writes:
     *  1. The number of dimensions (always 1)
     *  2. Array header flags (not used so always 0)
     *  3. The Oid of the [List] item type
     *  4. The number of items in the [List]
     *  5. The lower bound of the array (always 1)
     *  5. Each item encoded into the [buffer] (length prefixed if not null)
     */
    // https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1272
    override fun encode(value: List<T?>, buffer: ByteWriteBuffer) {
        buffer.writeInt(1)
        buffer.writeInt(0)
        buffer.writeInt(encoder.pgType.oidOrUnknown())
        buffer.writeInt(value.size)
        buffer.writeInt(1)
        for (item in value) {
            if (item == null) {
                buffer.writeByte(-1)
                continue
            }
            buffer.writeLengthPrefixed {
                encoder.encode(item, this)
            }
        }
    }
}

/** Dummy [PgColumnDescription] to create a [PgValue.Text] instance for text decoding */
@PublishedApi
internal val dummyFieldDescription = PgColumnDescription(
    fieldName = "",
    tableOid = 0,
    columnAttribute = 0,
    dataTypeSize = 0,
    pgType = PgType.Unknown,
    typeModifier = 0,
    formatCode = 1,
)

/**
 * Dummy [PgColumnDescription] to create dummy instances of [PgValue.Binary] with the supplied
 * [typeOid]
 */
@PublishedApi
internal fun dummyTypedFieldDescription(typeOid: Int) = PgColumnDescription(
    fieldName = "",
    tableOid = 0,
    columnAttribute = 0,
    dataTypeSize = 0,
    pgType = PgType.ByOid(typeOid),
    typeModifier = 0,
    formatCode = 1,
)

/**
 * Function for creating new [PgTypeDecoder] instances that decode into a [List] of [T]. Requires
 * the [List] item's type [decoder].
 */
inline fun <reified T : Any, D : PgTypeDecoder<T>> arrayTypeDecoder(
    decoder: D,
): PgTypeDecoder<List<T?>> {
    return arrayTypeDecoder(decoder, typeOf<List<T?>>())
}

private const val ARRAY_LITERAL_CHECK_MESSAGE =
    "An array literal value must start and end with a curly brace"

/**
 * Returns an implementation of a [PgTypeDecoder] that decodes a [PgValue] into a [List] of [T].
 *
 * ### Binary
 * Message contains:
 *  1. [Int] - Number of dimensions for the array. Must be 1 or an [IllegalArgumentException] is
 *  thrown
 *  2. [Int] - Array header flags (discarded)
 *  3. [Int] - Array item type Oid
 *  4. [Int] - Length of the array
 *  5. [Int] - Lower bound of the array. Must be 1 or an [IllegalArgumentException] is thrown
 *  6. Dynamic - All items of the array as the number of bytes ([Int]) followed by that number of
 *  bytes
 *
 * ### Text
 * Message is a text representation of the array (also called an array literal). Must be wrapped in
 * curly braces and each item is separated by a comma. For details see [ArrayLiteralParser]. With
 * all the items in text format, push each [String] item into a [PgValue.Text] and decode into the
 * resulting [List].
 *
 * @throws columnDecodeError if the decode operation fails (reason supplied in [Exception])
 */
@PublishedApi
internal fun <T : Any, D : PgTypeDecoder<T>> arrayTypeDecoder(
    decoder : D,
    arrayType: KType,
): PgTypeDecoder<List<T?>> = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1549
        is PgValue.Binary -> {
            val dimensions = value.bytes.readInt()
            if (dimensions == 0) {
                return@PgTypeDecoder listOf()
            }
            checkOrColumnDecodeError(
                check = dimensions == 1,
                kType = arrayType,
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
                kType = arrayType,
                type = value.typeData,
            ) { "Attempted to read an array with a lower bound other than 1. Got $lowerBound" }

            val fieldDescription = dummyTypedFieldDescription(elementTypeOid)
            List(length) {
                // Read length value but don't use it since a ReadBufferSlice cannot be constructed
                val elementLength = value.bytes.readInt()
                val slice = value.bytes.slice(elementLength)
                value.bytes.skip(elementLength)
                decoder.decode(PgValue.Binary(slice, fieldDescription))
            }
        }
        is PgValue.Text -> {
            checkOrColumnDecodeError(
                check = value.text.startsWith('{') && value.text.endsWith('}'),
                kType = arrayType,
                type = value.typeData,
            ) { ARRAY_LITERAL_CHECK_MESSAGE }

            ArrayLiteralParser.parse(value.text)
                .map {
                    when {
                        it == null -> null
                        else -> {
                            val innerValue = PgValue.Text(it, dummyFieldDescription)
                            decoder.decode(innerValue)
                        }
                    }
                }.toList()
        }
    }
}
