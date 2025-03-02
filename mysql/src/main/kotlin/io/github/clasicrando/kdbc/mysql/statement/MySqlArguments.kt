package io.github.clasicrando.kdbc.mysql.statement

import kotlin.math.floor

/**
 * Wrapper collection for a [List] of [MySqlArgument]. This exists since MySQL expects a null bitmap
 * with the arguments which is created during the constructor call using the [inner] arguments.
 */
internal data class MySqlArguments(val inner: List<MySqlArgument>) {
    /**
     * Bitmap for all arguments where each argument has a bit indicating if the value is null
     * (rather than a magic value). The size of the bitmap is `floor(argCount / 8) + 1`.
     */
    val nullBitMap: ByteArray =
        ByteArray(floor(inner.size / 8.0).toInt() + 1).apply {
            for (i in inner.indices) {
                val byteIndex = i / 8
                val bitOffset = i % 8

                val byte = if (inner[i].value == null) 1 else 0
                this[byteIndex] = (this[byteIndex].toInt() or (byte shl bitOffset)).toByte()
            }
        }
}
