package com.github.clasicrando.common.stream

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer

interface AsyncStream : AutoRelease {
    val isConnected: Boolean
    suspend fun connect(): Result<Unit>
    suspend fun writeBuffer(buffer: ByteWriteBuffer): Result<Unit>
    suspend fun readByte(): Result<Byte>
    suspend fun readInt(): Result<Int>
    suspend fun readBuffer(size: Int): Result<ByteReadBuffer>
}
