package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.buffer.ArrayListReadBuffer
import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.ReadBufferSlice
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readShort

internal class PgRowBuffer(private val innerBuffer: ArrayListReadBuffer) {
    val values: Array<ReadBuffer?> = run {
        val count = innerBuffer.readShort()
        var offset = 2
        Array(count.toInt()) {
            val length = innerBuffer.readInt()
            offset += 4
            if (length < 0) {
                return@Array null
            }
            val buf = ReadBufferSlice(innerBuffer, offset, length)
            innerBuffer.skip(length)
            offset += length
            buf
        }
    }
}
