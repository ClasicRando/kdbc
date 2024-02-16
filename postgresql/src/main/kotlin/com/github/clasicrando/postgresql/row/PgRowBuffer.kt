package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.buffer.ArrayReadBuffer
import com.github.clasicrando.common.buffer.ReadBufferSlice
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readShort

internal class PgRowBuffer(private val innerBuffer: ArrayReadBuffer) : AutoRelease {
    val values: Array<ReadBufferSlice?> = run {
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
