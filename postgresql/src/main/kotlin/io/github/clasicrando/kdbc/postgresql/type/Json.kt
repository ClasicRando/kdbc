package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeString
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.type.Json
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.typeOf

/** Implementation of a [PgTypeDescription] for the [Json] type */
internal object JsonTypeDescription :
    PgTypeDescription<Json>(dbType = PgType.Jsonb, kType = typeOf<Json>()) {
    override fun isCompatible(dbType: PgType): Boolean {
        return dbType == this.dbType || dbType == PgType.Json
    }

    /**
     * Writes a single [Byte] of 1, then calls [Json.writeToBuffer] which encodes the json data into
     * the buffer. This assumes that it is always writing a jsonb type because postgres databases
     * appear to always call `jsonb_recv` when the format type is binary.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonb.c#L93)
     */
    override fun encode(value: Json, buffer: ByteWriteBuffer) {
        buffer.writeByte(1)
        value.writeToBuffer(buffer)
    }

    /**
     * Create a new [Json] by reading the binary data and create a new [Json.Bytes] instance.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L136)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the JSONB format is not
     *   version = 1
     */
    override fun decodeBytes(value: PgValue.Binary): Json {
        if (value.typeData.pgType.oid == PgType.JSONB) {
            val version = value.bytes.readByte()
            checkOrColumnDecodeError<Json>(check = version == 1.toByte(), type = value.typeData) {
                "Unsupported JSONB format version $version. Only version 1 is supported"
            }
        }

        return Json.Bytes(value.bytes.readBytes())
    }

    /**
     * Create a new [Json] by passing the text value into a new [Json.Text] instance.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L124)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the header value
     */
    override fun decodeText(value: PgValue.Text): Json {
        return Json.Text(value.text)
    }
}

/** Implementation of a [PgTypeDescription] for the [Json.Bytes] type */
internal object JsonBytesTypeDescription :
    PgTypeDescription<Json.Bytes>(dbType = PgType.Jsonb, kType = typeOf<Json.Bytes>()) {
    override fun isCompatible(dbType: PgType): Boolean {
        return JsonTypeDescription.isCompatible(dbType)
    }

    /** Defers to [JsonTypeDescription.encode] */
    override fun encode(value: Json.Bytes, buffer: ByteWriteBuffer) {
        JsonTypeDescription.encode(value, buffer)
    }

    /** Defers to [JsonTypeDescription.decodeBytes] */
    override fun decodeBytes(value: PgValue.Binary): Json.Bytes {
        return JsonTypeDescription.decodeBytes(value).asJson()
    }

    /** Defers to [JsonTypeDescription.decodeText] */
    override fun decodeText(value: PgValue.Text): Json.Bytes {
        return JsonTypeDescription.decodeText(value).asJson()
    }
}

/** Implementation of a [PgTypeDescription] for the [Json.Bytes] type */
internal object JsonTextTypeDescription :
    PgTypeDescription<Json.Text>(dbType = JsonTypeDescription.dbType, kType = typeOf<Json.Text>()) {
    override fun isCompatible(dbType: PgType): Boolean {
        return JsonTypeDescription.isCompatible(dbType)
    }

    /** Defers to [JsonTypeDescription.encode] */
    override fun encode(value: Json.Text, buffer: ByteWriteBuffer) {
        JsonTypeDescription.encode(value, buffer)
    }

    /** Defers to [JsonTypeDescription.decodeBytes] */
    override fun decodeBytes(value: PgValue.Binary): Json.Text {
        return JsonTypeDescription.decodeBytes(value).asJson()
    }

    /** Defers to [JsonTypeDescription.decodeText] */
    override fun decodeText(value: PgValue.Text): Json.Text {
        return JsonTypeDescription.decodeText(value).asJson()
    }
}

/**
 * Implementation of a [PgTypeDescription] for the `jsonpath` type in a postgresql database. This
 * maps to the [String] type for convenience.
 */
internal object JsonPathTypeDescription :
    PgTypeDescription<PgJsonPath>(dbType = PgType.Jsonpath, kType = typeOf<PgJsonPath>()) {
    /**
     * Writes the jsonpath version number (always 1) followed by the path as UTF8 text
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L113)
     */
    override fun encode(value: PgJsonPath, buffer: ByteWriteBuffer) {
        buffer.writeByte(1)
        buffer.writeString(value.value)
    }

    /**
     * Read the jsonpath version (verifying that it is 1) then read the remaining text as the
     * jsonpath value.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L145)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the jsonpath format is
     *   not version = 1
     */
    override fun decodeBytes(value: PgValue.Binary): PgJsonPath {
        val version = value.bytes.readLong()
        checkOrColumnDecodeError<PgJsonPath>(check = version == 1L, type = value.typeData) {
            "Unsupported JSONPATH format version $version. Only version 1 is supported"
        }

        return PgJsonPath(value.bytes.readText())
    }

    /**
     * Simply return the text value
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/jsonpath.c#L132)
     */
    override fun decodeText(value: PgValue.Text): PgJsonPath {
        return PgJsonPath(value.text)
    }
}
