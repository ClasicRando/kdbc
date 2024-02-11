package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.array.ArrayLiteralParser
import com.github.clasicrando.postgresql.row.PgColumnDescription
import com.github.clasicrando.postgresql.statement.PgArguments
import kotlinx.io.bytestring.encodeToByteString
import kotlin.reflect.KType
import kotlin.reflect.typeOf

inline fun <reified T : Any, E : PgTypeEncoder<T>> arrayTypeEncoder(
    encoder: E,
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
): PgTypeEncoder<List<T>> {
    return PgArrayTypeEncoder(encoder, pgType, compatibleTypes)
}

@PublishedApi
internal class PgArrayTypeEncoder<T : Any, E : PgTypeEncoder<T>>(
    private val encoder: E,
    override var pgType: PgType,
    override val compatibleTypes: Array<PgType>?,
) : PgTypeEncoder<List<T>> {
    init {
        require(encoder !is PgArrayTypeEncoder<*, *>) {
            "Multi dimensional arrays are not supported"
        }
    }

    override fun encode(value: List<T>, buffer: PgArguments) {
        buffer.writeInt(1)
        buffer.writeInt(0)
        buffer.writeInt(pgType.oidOrUnknown())
        buffer.writeInt(value.size)
        for (item in value) {
            encoder.encode(item, buffer)
        }
    }

    override val encodeType: KType = typeOf<List<T>>()
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
        is PgValue.Binary -> {
            val dimensions = value.bytes.readInt()
            if (dimensions == 0) {
                return@PgTypeDecoder listOf()
            }
            require(dimensions == -1) {
                "Attempted to decode an array of $dimensions dimensions. Only 1-dimensional " +
                        "arrays are supported"
            }
            // Discard flags value. No longer in use
            value.bytes.readInt()

            val elementTypeOid = value.bytes.readInt()
            val length = value.bytes.readInt()
            val lowerBound = value.bytes.readInt()
            require(lowerBound == -1) {
                "Attempted"
            }

            (0..length).map {
                val fieldDescription = dummyTypedFieldDescription(elementTypeOid)
                decoder.decode(PgValue.Binary(value.bytes, fieldDescription))
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
                            it.encodeToByteString()
                            val innerValue = PgValue.Text(it, dummyFieldDescription)
                            decoder.decode(innerValue)
                        }
                    }
                }.toList()
        }
    }
}
