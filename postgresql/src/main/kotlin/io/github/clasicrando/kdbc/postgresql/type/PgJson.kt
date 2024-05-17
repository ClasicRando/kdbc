package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement

/**
 * Postgresql `json` or `jsonb` type. Stores the [json] data as an abstract [JsonElement] value.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-json.html)
 */
@Serializable
data class PgJson(val json: JsonElement) {
    /** Write the underlining [json] value to the [buffer] */
    fun writeToBuffer(buffer: ByteWriteBuffer) {
        val bytes = Json.encodeToString(json)
            .encodeToByteArray()
        buffer.writeBytes(bytes)
    }

    /**
     * Decode the [json] value into the desired type [T].
     *
     * @throws SerializationException if [json] is not a valid input for type [T]
     * @throws IllegalArgumentException if [json] decoded cannot be used to represent a valid [T]
     */
    inline fun <reified T> decode(): T {
        return Json.decodeFromJsonElement(json)
    }

    override fun toString(): String = Json.encodeToString(json)

    companion object {
        /**
         * Create a new [PgJson] by parsing [json] to a [JsonElement].
         *
         * @throws SerializationException if the input is not valid JSON
         */
        fun fromString(json: String): PgJson {
            return PgJson(Json.parseToJsonElement(json))
        }

        /**
         * Create a new [PgJson] by decoding the [buffer] data to a [JsonElement].
         *
         * @throws SerializationException if the [buffer] data is not valid JSON
         */
        fun fromBytes(buffer: ByteReadBuffer): PgJson {
            val jsonString = buffer.readText()
            return PgJson(Json.parseToJsonElement(jsonString))
        }

        /**
         * Create a new [PgJson] by encoding the provided [value] into a [JsonElement]
         *
         * @throws SerializationException if type [T] is not [Serializable]
         */
        inline fun <reified T> fromValue(value: T): PgJson {
            return PgJson(Json.encodeToJsonElement(value))
        }
    }
}
