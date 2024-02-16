package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.buffer.ArrayReadBuffer
import com.github.clasicrando.common.buffer.ReadBufferSlice
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readShort

internal class PgRowBuffer(private val innerBuffer: ArrayReadBuffer) {
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

    fun release() {
        innerBuffer.release()
    }

    inline fun <R> use(block: (PgRowBuffer) -> R): R {
        var cause: Throwable? = null
        return try {
            block(this)
        } catch (ex: Throwable) {
            cause = ex
            throw cause
        } finally {
            when (cause) {
                null -> release()
                else -> try {
                    release()
                } catch (ex2: Throwable) {
                    cause.addSuppressed(ex2)
                }
            }
        }
    }
}
