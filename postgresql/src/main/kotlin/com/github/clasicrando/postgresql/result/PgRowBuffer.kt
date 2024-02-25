package com.github.clasicrando.postgresql.result

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.buffer.ByteReadBuffer

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
