package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.encodeToStream

@Serializable
data class PgJson(val json: JsonElement) {
    @OptIn(ExperimentalSerializationApi::class)
    fun writeToBuffer(buffer: ByteWriteBuffer) {
        Json.encodeToStream(json, buffer.outputStream())
    }

    inline fun <reified T> decode(): T {
        return Json.decodeFromJsonElement(json)
    }

    override fun toString(): String = Json.encodeToString(json)

    companion object {
        fun fromString(json: String) {
            PgJson(Json.parseToJsonElement(json))
        }

        fun fromBytes(buffer: ByteReadBuffer) {
            val jsonString = String(bytes = buffer.readBytes())
            PgJson(Json.decodeFromString<JsonElement>(jsonString))
        }

        inline fun <reified T> fromValue(value: T): PgJson {
            return PgJson(Json.encodeToJsonElement(value))
        }
    }
}
