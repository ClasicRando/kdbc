package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.inputStream
import com.github.clasicrando.common.buffer.outputStream
import com.github.clasicrando.postgresql.type.PgJson
import kotlinx.io.asInputStream
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToStream

@OptIn(ExperimentalSerializationApi::class)
val jsonTypeEncoder = PgTypeEncoder<PgJson>(
    pgType = PgType.Jsonb,
    compatibleTypes = arrayOf(PgType.Json),
) { value, buffer ->
    Json.encodeToStream(value.json, buffer.outputStream())
}

@OptIn(ExperimentalSerializationApi::class)
val jsonTypeDecoder = PgTypeDecoder<PgJson> { value ->
    when (value) {
        is PgValue.Binary -> {
            if (value.typeData.pgType == PgType.Jsonb) {
                val version = value.bytes.readByte()
                check(version == 1.toByte()) {
                    "Unsupported JSONB format version $version. Only version 1 is supported"
                }
            }
            Json.decodeFromStream(value.bytes.inputStream())
        }
        is PgValue.Text -> Json.decodeFromString(value.text)
    }
}
