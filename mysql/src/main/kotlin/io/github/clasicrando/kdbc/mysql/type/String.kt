package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.column.ColumnMetadata
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.type.Json
import io.github.clasicrando.kdbc.mysql.buffer.writeLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.writeStringLengthEncoded
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import java.math.BigDecimal
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.io.Sink

/**
 * Implementation of a [MySqlTypeDescription] for the [String] type. Accepts BLOB, VARCHAR,
 * TINYBLOB, MEDIUMBLOB, LONGBLOB, STRING, VARSTRING and ENUM types when decoding.
 */
internal object StringTypeDescription :
    MySqlTypeDescription<String>(dbType = MySqlType.VarString, kType = typeOf<String>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType.inner == MySqlType.Varchar.inner ||
            dbType.inner == MySqlType.Blob.inner ||
            dbType.inner == MySqlType.TinyBlob.inner ||
            dbType.inner == MySqlType.MediumBlob.inner ||
            dbType.inner == MySqlType.LongBlob.inner ||
            dbType.inner == MySqlType.String.inner ||
            dbType.inner == MySqlType.VarString.inner ||
            dbType.inner == MySqlType.Enum.inner
    }

    override fun encode(value: String, buffer: Sink) {
        buffer.writeStringLengthEncoded(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): String {
        return value.bytes.readText()
    }

    override fun decodeText(value: MySqlValue.Text): String {
        return value.text
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [BigDecimal] type. Accepts NEWDECIMAL and
 * DECIMAL types when decoding. For reading a writing, the binary protocol uses decimal text
 * representations of the values.
 */
internal object BigDecimalTypeDescription :
    MySqlTypeDescription<BigDecimal>(dbType = MySqlType.NewDecimal, kType = typeOf<BigDecimal>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType.inner == MySqlType.NewDecimal.inner || dbType.inner == MySqlType.Decimal.inner
    }

    override fun encode(value: BigDecimal, buffer: Sink) {
        buffer.writeStringLengthEncoded(value.toPlainString())
    }

    override fun decodeBytes(value: MySqlValue.Binary): BigDecimal {
        return BigDecimal(value.bytes.readText())
    }

    override fun decodeText(value: MySqlValue.Text): BigDecimal {
        return BigDecimal(value.text)
    }
}

/** Implementation of [MySqlTypeDescription] for custom enum columns in a mysql database */
@PublishedApi
internal class EnumTypeDescription<E : Enum<E>>(kType: KType, values: Array<E>) :
    MySqlTypeDescription<E>(dbType = StringTypeDescription.dbType, kType = kType) {
    private val nameMap: Map<E, String>
    internal val entryLookup: Map<String, E>

    init {
        val renameMap: Map<String, String> =
            values
                .firstOrNull()
                ?.declaringJavaClass
                ?.fields
                ?.mapNotNull { field ->
                    if (!field.isEnumConstant) {
                        return@mapNotNull null
                    }
                    val renameAnnotation =
                        field.annotations.asSequence().mapNotNull { it as? Rename }.firstOrNull()
                            ?: return@mapNotNull null
                    field.name to renameAnnotation.value
                }
                ?.toMap() ?: mapOf()
        nameMap = values.associateWith { renameMap[it.name] ?: it.name }
        entryLookup = nameMap.asSequence().associate { it.value to it.key }
    }

    override fun isCompatible(dbType: MySqlType): Boolean {
        return StringTypeDescription.isCompatible(dbType)
    }

    /** Writes the [Enum.name] property as text to the argument buffer. */
    override fun encode(value: E, buffer: Sink) {
        StringTypeDescription.encode(value.name, buffer)
    }

    private fun getLabel(text: String, type: ColumnMetadata): E {
        return entryLookup[text]
            ?: columnDecodeError(
                kType = kType,
                type = type,
                reason = "Could not find enum value for '$text'",
            )
    }

    /**
     * Reads all the bytes as a UTF-8 encoded [String]. Then find the enum value that matches that
     * [String] by [Enum.name]. If no match is found, throw a
     * [io.github.clasicrando.kdbc.core.column.ColumnDecodeError].
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if a variant of [E] cannot
     *   be found by [Enum.name] from the decoded [String] value
     */
    override fun decodeBytes(value: MySqlValue.Binary): E {
        return getLabel(StringTypeDescription.decode(value), value.column)
    }

    /**
     * Use the [String] value to find the enum value that matches that [String] by [Enum.name]. If
     * no match is found, throw a [io.github.clasicrando.kdbc.core.column.ColumnDecodeError].
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if a variant of [E] cannot
     *   be found by [Enum.name] from the decoded [String] value
     */
    override fun decodeText(value: MySqlValue.Text): E {
        return getLabel(StringTypeDescription.decode(value), value.column)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Json] type. Accepts BLOB, VARCHAR, TINYBLOB,
 * MEDIUMBLOB, LONGBLOB, STRING, VARSTRING and ENUM types when decoding.
 */
internal object JsonTypeDescription :
    MySqlTypeDescription<Json>(dbType = MySqlType.String, kType = typeOf<Json>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType.inner == MySqlType.Json.inner ||
            StringTypeDescription.isCompatible(dbType) ||
            ByteArrayTypeDescription.isCompatible(dbType)
    }

    /** Write value length encoded either as bytes or text */
    override fun encode(value: Json, buffer: Sink) {
        buffer.writeLengthEncoded { value.writeToBuffer(this) }
    }

    /** Dump all bytes into [Json.Bytes] */
    override fun decodeBytes(value: MySqlValue.Binary): Json {
        return Json.Bytes(value.bytes.readBytes())
    }

    /** Dump text into [Json.Text] */
    override fun decodeText(value: MySqlValue.Text): Json {
        return Json.Text(value.text)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Json.Text] type. Wraps the
 * [JsonTypeDescription] for all actions but converts the returned [Json] if internally it's
 * [Json.Bytes].
 */
internal object JsonTextTypeDescription :
    MySqlTypeDescription<Json.Text>(
        dbType = JsonTypeDescription.dbType,
        kType = typeOf<Json.Text>(),
    ) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return JsonTypeDescription.isCompatible(dbType)
    }

    override fun encode(value: Json.Text, buffer: Sink) {
        JsonTypeDescription.encode(value, buffer)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Json.Text {
        return JsonTypeDescription.decodeBytes(value).asJson()
    }

    override fun decodeText(value: MySqlValue.Text): Json.Text {
        return JsonTypeDescription.decodeText(value).asJson()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Json.Bytes] type. Wraps the
 * [JsonTypeDescription] for all actions but converts the returned [Json] if internally it's
 * [Json.Text].
 */
internal object JsonBytesTypeDescription :
    MySqlTypeDescription<Json.Bytes>(
        dbType = JsonTypeDescription.dbType,
        kType = typeOf<Json.Bytes>(),
    ) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return JsonTypeDescription.isCompatible(dbType)
    }

    override fun encode(value: Json.Bytes, buffer: Sink) {
        JsonTypeDescription.encode(value, buffer)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Json.Bytes {
        return JsonTypeDescription.decodeBytes(value).asJson()
    }

    override fun decodeText(value: MySqlValue.Text): Json.Bytes {
        return JsonTypeDescription.decodeText(value).asJson()
    }
}
