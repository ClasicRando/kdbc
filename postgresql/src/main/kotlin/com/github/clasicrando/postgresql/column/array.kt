package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ArrayWriteBuffer
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.writeInt
import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.array.ArrayLiteralParser
import com.github.clasicrando.postgresql.row.PgColumnDescription
import kotlin.reflect.KType
import kotlin.reflect.typeOf

inline fun <reified T : Any, E : PgTypeEncoder<T>> arrayTypeEncoder(
    encoder: E,
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
): PgTypeEncoder<List<T?>> {
    return PgArrayTypeEncoder(encoder, pgType, compatibleTypes, typeOf<List<T?>>())
}

@PublishedApi
internal class PgArrayTypeEncoder<T : Any, E : PgTypeEncoder<T>>(
    private val encoder: E,
    override var pgType: PgType,
    override val compatibleTypes: Array<PgType>?,
    override val encodeType: KType,
) : PgTypeEncoder<List<T?>> {
    init {
        require(encoder !is PgArrayTypeEncoder<*, *>) {
            "Multi dimensional arrays are not supported"
        }
    }

    // https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1272
    override fun encode(value: List<T?>, buffer: ArrayWriteBuffer) {
        buffer.writeInt(1)
        buffer.writeInt(0)
        buffer.writeInt(encoder.pgType.oidOrUnknown())
        buffer.writeInt(value.size)
        buffer.writeInt(1)
        for (item in value) {
            buffer.writeLengthPrefixed {
                if (item == null) {
                    writeByte(-1)
                    return@writeLengthPrefixed
                }
                encoder.encode(item, this)
            }
        }
    }
}

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

inline fun <reified T : Any, D : PgTypeDecoder<T>> arrayTypeDecoder(
    decoder : D,
): PgTypeDecoder<List<T?>> = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/d57b7cc3338e9d9aa1d7c5da1b25a17c5a72dcce/src/backend/utils/adt/arrayfuncs.c#L1549
        is PgValue.Binary -> {
            val dimensions = value.bytes.readInt()
            if (dimensions == 0) {
                return@PgTypeDecoder listOf()
            }
            require(dimensions == 1) {
                "Attempted to decode an array of $dimensions dimensions. Only 1-dimensional " +
                        "arrays are supported"
            }
            // Discard flags value. No longer in use
            value.bytes.readInt()

            val elementTypeOid = value.bytes.readInt()
            val length = value.bytes.readInt()
            val lowerBound = value.bytes.readInt()
            require(lowerBound == 1) {
                "Attempted to read an array with a first dimension other than 1"
            }

            val fieldDescription = dummyTypedFieldDescription(elementTypeOid)
            List(length) {
                // Read length value but don't use it since a ReadBufferSlice cannot be constructed
                val elementLength = value.bytes.readInt()
                val slice = value.bytes.subSlice(elementLength)
                decoder.decode(PgValue.Binary(slice, fieldDescription))
            }
        }
        is PgValue.Text -> {
            if (!value.text.startsWith('{') || !value.text.endsWith('}')) {
                columnDecodeError<List<T>>(value.typeData)
            }

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
