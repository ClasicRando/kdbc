package io.github.clasicrando.kdbc.core.type

import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json as KotlinxJson
import kotlinx.serialization.json.JsonElement

/**
 * Wrapper for json data encoded as [Bytes] or [Text]
 */
public sealed class Json {
    public class Bytes(public val bytes: ByteArray) : Json() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Bytes) return false

            return bytes.contentEquals(other.bytes)
        }

        override fun hashCode(): Int {
            return bytes.contentHashCode()
        }

        override fun toString(): String {
            return "Json.Bytes(bytes=${bytes.contentToString()})"
        }
    }

    public class Text(public val text: String) : Json() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Text) return false

            return text == other.text
        }

        override fun hashCode(): Int {
            return text.hashCode()
        }

        override fun toString(): String {
            return "Json.Text(text='$text')"
        }
    }

    /** Write the underlining JSON value to the [buffer] */
    public fun writeToBuffer(buffer: Sink) {
        when (this) {
            is Bytes -> buffer.write(bytes)
            is Text -> buffer.writeString(text)
        }
    }

    public inline fun <reified T : Any> decodeUsingSerialization(): T {
        return when (this) {
            is Bytes -> KotlinxJson.decodeFromString(bytes.toString(charset = Charsets.UTF_8))
            is Text -> KotlinxJson.decodeFromString(text)
        }
    }

    public fun decodeAsJsonElement(): JsonElement {
        return decodeUsingSerialization()
    }

    override fun toString(): String {
        return when (this) {
            is Bytes -> bytes.toString(charset = Charsets.UTF_8)
            is Text -> text
        }
    }

    public companion object {
        public fun fromJsonElement(jsonElement: JsonElement): Json {
            return Text(KotlinxJson.encodeToString(jsonElement))
        }
    }
}

