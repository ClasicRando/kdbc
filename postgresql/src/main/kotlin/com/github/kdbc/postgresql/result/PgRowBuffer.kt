package com.github.kdbc.postgresql.result

import com.github.kdbc.core.AutoRelease
import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.postgresql.message.PgMessage

/**
 * Container for [PgMessage.DataRow] values as a reference to a main [innerBuffer] and each value
 * in the row as slice of that main buffer for the specified size (or null if the length of the
 * value is -1).
 */
internal class PgRowBuffer(private val innerBuffer: ByteReadBuffer) : AutoRelease {
    val values: Array<ByteReadBuffer?> = run {
        val count = innerBuffer.readShort()
        Array(count.toInt()) {
            val length = innerBuffer.readInt()
            if (length < 0) {
                return@Array null
            }
            val slice = innerBuffer.slice(length)
            innerBuffer.skip(length)
            slice
        }
    }

    override fun release() {
        innerBuffer.release()
    }
}
