package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlin.reflect.typeOf

/** Implementation of a [PgTypeDescription] for the [PgJson] type */
abstract class AbstractJsonTypeDescription(pgType: PgType) : PgTypeDescription<PgJson>(
    pgType = pgType,
    kType = typeOf<PgJson>(),
) {
    /**
     * Calls [PgJson.writeToBuffer] which encodes the json data into the buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L150)
     */
    override fun encode(value: PgJson, buffer: ByteWriteBuffer) {
        buffer.writeByte(1)
        value.writeToBuffer(buffer)
    }

    /**
     * Create a new [PgJson] by reading the binary data as a [String] and parsing then parsing to a
     * [JsonElement]. If the value is a `jsonb` then read the first byte to the get the jsonb
     * version. Currently, the only accepted value is 1 and all other values will throw a
     * [ColumnDecodeError]. If the value is `json` then no header values are expected. After
     * processing the possible header [Byte], the remaining bytes in the buffer are passed to
     * [PgJson.fromBytes] for decoding into a new [PgJson] instance.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L136)
     *
     * @throws SerializationException if the bytes are not valid JSON
     * @throws ColumnDecodeError if the JSONB format is not version = 1 or the binary data cannot
     * be decoded as a [JsonElement]
     */
    override fun decodeBytes(value: PgValue.Binary): PgJson {
        if (value.typeData.pgType == PgType.Jsonb) {
            val version = value.bytes.readByte()
            checkOrColumnDecodeError<PgJson>(
                check = version == 1.toByte(),
                type = value.typeData,
            ) { "Unsupported JSONB format version $version. Only version 1 is supported" }
        }

        val jsonString = value.bytes.readText()
        return try {
            PgJson(Json.parseToJsonElement(jsonString))
        } catch (ex: SerializationException) {
            columnDecodeError<PgJson>(
                type = value.typeData,
                reason = "Could not parse the '$jsonString' into a json value. ${ex.message}",
            )
        }
    }

    /**
     * Attempt to parse the [String] value into a new [PgJson] using [PgJson.fromString].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L124)
     *
     * @throws ColumnDecodeError if the header value
     */
    override fun decodeText(value: PgValue.Text): PgJson {
        return try {
            PgJson.fromString(value.text)
        } catch (ex: SerializationException) {
            columnDecodeError<PgJson>(
                type = value.typeData,
                reason = "Could not parse the '${value.text}' into a json value. ${ex.message}",
            )
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgJson] type. This maps to the `json` type in a
 * postgresql database.
 */
object JsonTypeDescription : AbstractJsonTypeDescription(pgType = PgType.Json)

/**
 * Implementation of a [ArrayTypeDescription] for [PgJson]. This maps to the `json[]` type in a
 * postgresql database.
 */
object JsonArrayTypeDescription : ArrayTypeDescription<PgJson>(
    pgType = PgType.JsonArray,
    innerType = JsonTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [PgJson] type. This maps to the `jsonb` type in
 * a postgresql database.
 */
object JsonbTypeDescription : AbstractJsonTypeDescription(pgType = PgType.Jsonb)

/**
 * Implementation of a [ArrayTypeDescription] for [PgJson]. This maps to the `jsonb[]` type in a
 * postgresql database.
 */
object JsonbArrayTypeDescription : ArrayTypeDescription<PgJson>(
    pgType = PgType.JsonbArray,
    innerType = JsonTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `jsonpath` type
 * in a postgresql database.
 */
object JsonPathTypeDescription : PgTypeDescription<String>(
    pgType = PgType.Jsonpath,
    kType = typeOf<String>(),
) {
    /**
     * Writes the jsonpath version number (always 1) followed by the path as UTF8 text
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L113)
     */
    override fun encode(value: String, buffer: ByteWriteBuffer) {
        buffer.writeByte(1)
        buffer.writeText(value)
    }

    /**
     * Read the jsonpath version (verifying that it is 1) then read the remaining text as the
     * jsonpath value.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L145)
     *
     * @throws ColumnDecodeError if the jsonpath format is not version = 1
     */
    override fun decodeBytes(value: PgValue.Binary): String {
        val version = value.bytes.readLong()
        checkOrColumnDecodeError<PgJson>(
            check = version == 1L,
            type = value.typeData,
        ) { "Unsupported JSONB format version $version. Only version 1 is supported" }

        return value.bytes.readText()
    }

    /**
     * Simply return the text value
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L132)
     */
    override fun decodeText(value: PgValue.Text): String {
        return value.text
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [PgJson]. This maps to the `jsonpath[]` type in a
 * postgresql database.
 */
object JsonPathArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.JsonpathArray,
    innerType = JsonPathTypeDescription,
)
