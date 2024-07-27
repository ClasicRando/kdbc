package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToStream
import java.io.ByteArrayOutputStream

/**
 * Postgresql `json` or `jsonb` type. Allows for formatting the value into various JSON libraries.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-json.html)
 */
class PgJson internal constructor(val bytes: ByteArray) {
    @OptIn(ExperimentalSerializationApi::class)
    constructor(element: JsonElement): this(
        ByteArrayOutputStream().use {
            Json.encodeToStream(element, it)
            it.toByteArray()
        }
    )
    constructor(string: String): this(string.encodeToByteArray())

    /** Write the underlining JSON value to the [buffer] */
    internal fun writeToBuffer(buffer: ByteWriteBuffer) {
        buffer.writeBytes(bytes)
    }

    @OptIn(ExperimentalSerializationApi::class)
    inline fun <reified T : Any> decodeUsingSerialization(): T {
        return bytes.inputStream().use(Json.Default::decodeFromStream)
    }

    fun decodeAsJsonElement(): JsonElement {
        return decodeUsingSerialization()
    }
}
