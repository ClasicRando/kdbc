package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.type.PgInet
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import kotlinx.serialization.SerializationException

/**
 * Implementation of a [PgTypeEncoder] for the [PgJson] type. This maps to the `json` and `jsonb`
 * types in a postgresql database. The encoder calls [PgJson.writeToBuffer] which encodes the json
 * data into the buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L150)
 */
val jsonTypeEncoder = PgTypeEncoder<PgJson>(
    pgType = PgType.Jsonb,
    compatibleTypes = arrayOf(PgType.Json),
) { value, buffer ->
    value.writeToBuffer(buffer)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgInet] type. This maps to the `inet` type in a
 * postgresql database.
 *
 * ### Binary
 * If the value is a `jsonb` then read the first byte to the get the jsonb version. Currently, the
 * only accepted value is 1 and all other values will throw a [ColumnDecodeError]. If the value is
 * `json` then no header values are expected. After processing the possible header [Byte], the
 * remaining bytes in the buffer are passed to [PgJson.fromBytes] for decoding into a new [PgJson]
 * instance.
 *
 * ### Text
 * Attempt to parse the [String] value into a new [PgJson] using [PgJson.fromString].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L136)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L124)
 *
 * @throws ColumnDecodeError if the header value
 */
val jsonTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            if (value.typeData.pgType == PgType.Jsonb) {
                val version = value.bytes.readByte()
                checkOrColumnDecodeError<PgJson>(
                    check = version == 1.toByte(),
                    type = value.typeData,
                ) { "Unsupported JSONB format version $version. Only version 1 is supported" }
            }
            try {
                PgJson.fromBytes(value.bytes)
            } catch (ex: Throwable) {
                columnDecodeError<PgJson>(
                    type = value.typeData,
                    reason = "Could not parse the binary json into a json value. ${ex.message}",
                )
            }
        }
        is PgValue.Text -> try {
            PgJson.fromString(value.text)
        } catch (ex: SerializationException) {
            columnDecodeError<PgJson>(
                type = value.typeData,
                reason = "Could not parse the '${value.text}' into a json value. ${ex.message}",
            )
        }
    }
}
