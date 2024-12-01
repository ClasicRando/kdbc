package io.github.clasicrando.kdbc.core.type

import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.Json as KotlinxJson

/** Wrapper for json data encoded as [Bytes] or [Text] */
public sealed class Json {
    public class Bytes(public val bytes: ByteArray) : Json() {
        override fun toString(): String {
            return "Json.Bytes(bytes=${bytes.contentToString()})"
        }
    }

    public class Text(public val text: String) : Json() {
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

    /**
     * Converts this opaque [Json] type into the desired [Json] subtype. If the underlining value is
     * already of type [T] then the original value is returned
     */
    public inline fun <reified T : Json> asJson(): T {
        if (this is T) {
            return this
        }
        return when (this) {
            is Bytes -> Text(this.bytes.toString(charset = Charsets.UTF_8)) as T
            is Text -> Bytes(this.text.toByteArray(charset = Charsets.UTF_8)) as T
        }
    }

    override fun toString(): String {
        return when (this) {
            is Bytes -> bytes.toString(charset = Charsets.UTF_8)
            is Text -> text
        }
    }

    final override fun hashCode(): Int {
        return when (this) {
            is Bytes -> this.bytes.contentHashCode()
            is Text -> this.text.hashCode()
        }
    }

    final override fun equals(other: Any?): Boolean {
        if (other !is Json) {
            return false
        }
        return when (this) {
            is Bytes ->
                when (other) {
                    is Text -> this.bytes.toString(charset = Charsets.UTF_8) == other.text
                    is Bytes -> this.bytes.contentEquals(other.bytes)
                }
            is Text ->
                when (other) {
                    is Text -> this.text == other.text
                    is Bytes -> this.text == other.bytes.toString(charset = Charsets.UTF_8)
                }
        }
    }

    public companion object {
        public fun fromJsonElement(jsonElement: JsonElement): Json {
            return Text(KotlinxJson.encodeToString(jsonElement))
        }
    }
}
