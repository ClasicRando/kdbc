package io.github.clasicrando.kdbc.mysql.statement

import kotlin.math.floor

internal data class MySqlArguments(
    val inner: List<MySqlArgument>,
) {
    val isEmpty = inner.isEmpty()

    fun nullBitMap(): ByteArray {
        val bytes = ByteArray(floor(inner.size / 8.0).toInt() + 1)
        for (i in inner.indices) {
            val byteIndex = i / 8
            val bitOffset = i % 8

            val byte = if (inner[i].value.value == null) 1 else 0
            bytes[byteIndex] = (bytes[byteIndex].toInt() or (byte shl bitOffset)).toByte()
        }
        return bytes
    }
}
