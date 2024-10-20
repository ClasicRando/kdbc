package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.serialization.SerializationException
import kotlin.reflect.typeOf

/** Implementation of a [PgTypeDescription] for the [PgJson] type */
internal object JsonTypeDescription : PgTypeDescription<PgJson>(
    dbType = PgType.Jsonb,
    kType = typeOf<PgJson>(),
) {
    override fun isCompatible(dbType: PgType): Boolean {
        return dbType == this.dbType || dbType == PgType.Json
    }

    /**
     * Writes a single [Byte] of 1, then calls [PgJson.writeToBuffer] which encodes the json data
     * into the buffer. This assumes that it is always writing a jsonb type because postgres
     * databases appear to always call `jsonb_recv` when the format type is binary.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonb.c#L93)
     */
    override fun encode(
        value: PgJson,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeByte(1)
        value.writeToBuffer(buffer)
    }

    /**
     * Create a new [PgJson] by reading the binary data as a [String] then parsing to a
     * [kotlinx.serialization.json.JsonElement]. If the value is a `jsonb` then read the first byte
     * to the get the jsonb version. Currently, the only accepted value is 1 and all other values
     * will throw a [io.github.clasicrando.kdbc.core.column.ColumnDecodeError]. If the value is
     * `json` then no header values are expected. After processing the possible header [Byte], the
     * remaining bytes in the buffer are passed to a new [PgJson] instance.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L136)
     *
     * @throws SerializationException if the bytes are not valid JSON
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the JSONB format is not
     * version = 1 or the binary data cannot be decoded as a
     * [kotlinx.serialization.json.JsonElement]
     */
    override fun decodeBytes(value: PgValue.Binary): PgJson {
        if (value.typeData.pgType.oid == PgType.JSONB) {
            val version = value.bytes.readByte()
            checkOrColumnDecodeError<PgJson>(
                check = version == 1.toByte(),
                type = value.typeData,
            ) { "Unsupported JSONB format version $version. Only version 1 is supported" }
        }

        return PgJson.Bytes(value.bytes.readBytes())
    }

    /**
     * Attempt to parse the [String] value into a new [PgJson].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L124)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the header value
     */
    override fun decodeText(value: PgValue.Text): PgJson {
        return try {
            PgJson.Text(value.text)
        } catch (ex: SerializationException) {
            columnDecodeError<PgJson>(
                type = value.typeData,
                reason = "Could not parse the '${value.text}' into a json value",
                cause = ex,
            )
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the `jsonpath` type in a postgresql database. This
 * maps to the [String] type for convenience.
 */
internal object JsonPathTypeDescription : PgTypeDescription<PgJsonPath>(
    dbType = PgType.Jsonpath,
    kType = typeOf<PgJsonPath>(),
) {
    /**
     * Writes the jsonpath version number (always 1) followed by the path as UTF8 text
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L113)
     */
    override fun encode(
        value: PgJsonPath,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeByte(1)
        buffer.writeText(value.value)
    }

    /**
     * Read the jsonpath version (verifying that it is 1) then read the remaining text as the
     * jsonpath value.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L145)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the jsonpath format is
     * not version = 1
     */
    override fun decodeBytes(value: PgValue.Binary): PgJsonPath {
        val version = value.bytes.readLong()
        checkOrColumnDecodeError<PgJson>(
            check = version == 1L,
            type = value.typeData,
        ) { "Unsupported JSONPATH format version $version. Only version 1 is supported" }

        return PgJsonPath(value.bytes.readText())
    }

    /**
     * Simply return the text value
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L132)
     */
    override fun decodeText(value: PgValue.Text): PgJsonPath {
        return PgJsonPath(value.text)
    }
}
