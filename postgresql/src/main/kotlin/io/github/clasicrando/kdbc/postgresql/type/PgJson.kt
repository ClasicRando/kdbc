package io.github.clasicrando.kdbc.postgresql.type

import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/**
 * Postgresql `json` or `jsonb` type. Allows for formatting the value into various JSON libraries.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-json.html)
 */
public sealed class PgJson {
    public class Bytes(public val bytes: ByteArray) : PgJson() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Bytes) return false

            return bytes.contentEquals(other.bytes)
        }

        override fun hashCode(): Int {
            return bytes.contentHashCode()
        }

        override fun toString(): String {
            return "Bytes(bytes=${bytes.contentToString()})"
        }
    }

    public class Text(public val text: String) : PgJson() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Text) return false

            return text == other.text
        }

        override fun hashCode(): Int {
            return text.hashCode()
        }

        override fun toString(): String {
            return "Text(text='$text')"
        }
    }

    /** Write the underlining JSON value to the [buffer] */
    internal fun writeToBuffer(buffer: Sink) {
        when (this) {
            is Bytes -> buffer.write(bytes)
            is Text -> buffer.writeString(text)
        }
    }

    public inline fun <reified T : Any> decodeUsingSerialization(): T =
        when (this) {
            is Bytes -> Json.decodeFromString(bytes.toString(charset = Charsets.UTF_8))
            is Text -> Json.decodeFromString(text)
        }

    public fun decodeAsJsonElement(): JsonElement = decodeUsingSerialization()

    override fun toString(): String =
        when (this) {
            is Bytes -> bytes.toString(charset = Charsets.UTF_8)
            is Text -> text
        }

    public companion object {
        public fun fromJsonElement(jsonElement: JsonElement): PgJson =
            Text(Json.encodeToString(jsonElement))
    }
}
