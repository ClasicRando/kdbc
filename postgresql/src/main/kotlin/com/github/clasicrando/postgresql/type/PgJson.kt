package com.github.clasicrando.postgresql.type

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement

@Serializable
data class PgJson(val json: JsonElement) {
    constructor(json: String): this(Json.parseToJsonElement(json))

    inline fun <reified T> decode(): T {
        return Json.decodeFromJsonElement(json)
    }

    override fun toString(): String = Json.encodeToString(json)
}
