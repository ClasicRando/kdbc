package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/**
 * Postgresql `json` or `jsonb` type. Allows for formatting the value into various JSON libraries.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-json.html)
 */
sealed class PgJson {
    class Bytes(val bytes: ByteArray) : PgJson()

    class Text(val text: String) : PgJson()

    /** Write the underlining JSON value to the [buffer] */
    internal fun writeToBuffer(buffer: ByteWriteBuffer) {
        when (this) {
            is Bytes -> buffer.writeBytes(bytes)
            is Text -> buffer.writeText(text)
        }
    }

    inline fun <reified T : Any> decodeUsingSerialization(): T {
        return when (this) {
            is Bytes -> Json.decodeFromString(bytes.toString(charset = Charsets.UTF_8))
            is Text -> Json.decodeFromString(text)
        }
    }

    fun decodeAsJsonElement(): JsonElement {
        return decodeUsingSerialization()
    }

    override fun toString(): String {
        return when (this) {
            is Bytes -> bytes.toString(charset = Charsets.UTF_8)
            is Text -> text
        }
    }

    companion object {
        fun fromJsonElement(jsonElement: JsonElement): PgJson {
            return Text(Json.encodeToString(jsonElement))
        }
    }
}
