package com.github.clasicrando.common.stream

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import kotlin.coroutines.suspendCoroutine

private val logger = KotlinLogging.logger {}

class Nio2AsyncStream(private val address: InetSocketAddress) : AsyncStream {
    private val socket = AsynchronousSocketChannel.open()
    private val readBuffer = ByteBuffer.allocateDirect(4096)
        .apply { limit(0) }

    override val isConnected: Boolean get() = socket.isOpen

    override suspend fun connect(): Result<Unit> {
        val result = suspendCoroutine { continuation ->
            socket.connect(
                address,
                null,
                SuspendVoidCompletionHandler(continuation),
            )
        }.onFailure {
            logger.atTrace {
                message = "Error attempting connection to {address}"
                payload = mapOf("address" to address)
                cause = it
            }
        }
        if (result.isSuccess) {
            logger.atTrace {
                message = "Successfully connected to {address}"
                payload = mapOf("address" to address)
            }
        }
        return result
    }

    override suspend fun writeBuffer(buffer: ByteWriteBuffer): Result<Unit> {
        while (buffer.innerBuffer.hasRemaining()) {
            val result = suspendCoroutine { continuation ->
                socket.write(
                    buffer.innerBuffer,
                    null,
                    SuspendCompletionHandler(continuation),
                )
            }
            when {
                result.isFailure -> {
                    val error = result.exceptionOrNull()!!
                    logger.atTrace {
                        message = "Error writing bytes to {address}"
                        payload = mapOf("address" to address)
                        cause = error
                    }
                    return Result.failure(error)
                }
                else -> {
                    val bytes = result.getOrNull()!!
                    logger.atTrace {
                        message = "Wrote {count} bytes to {address}"
                        payload = mapOf("count" to bytes, "address" to address)
                    }
                }
            }
        }
        return Result.success(Unit)
    }

    private suspend fun readIntoBuffer(min: Int): Result<Unit> {
        readBuffer.clear()
        while (true) {
            val result = suspendCoroutine { continuation ->
                socket.read(readBuffer, null, SuspendCompletionHandler(continuation))
            }
            val bytesRead = when {
                result.isFailure -> return Result.failure(result.exceptionOrNull()!!)
                else -> result.getOrNull()!!
            }
            if (bytesRead == -1) {
                logger.trace { "Unexpectedly reached end of stream" }
                return Result.failure(EndOfStream())
            }
            logger.atTrace {
                message = "Received {count} bytes from {address}"
                payload = mapOf("count" to bytesRead, "address" to address)
            }
            if (readBuffer.position() >= min) {
                readBuffer.flip()
                return Result.success(Unit)
            }
            if (!readBuffer.hasRemaining()) {
                return Result.failure(FullBuffer())
            }
        }
    }

    override suspend fun readByte(): Result<Byte> {
        if (readBuffer.remaining() >= 1) {
            return Result.success(readBuffer.get())
        }
        val readResult = readIntoBuffer(1)
        if (readResult.isFailure) {
            return Result.failure(readResult.exceptionOrNull()!!)
        }
        return Result.success(readBuffer.get())
    }

    override suspend fun readInt(): Result<Int> {
        if (readBuffer.remaining() >= 4) {
            return Result.success(readBuffer.getInt())
        }
        val available = readBuffer.remaining()
        val intRead = ByteBuffer.allocate(4)
        while (readBuffer.hasRemaining()) {
            intRead.put(readBuffer.get())
        }

        val readResult = readIntoBuffer(4 - available)
        if (readResult.isFailure) {
            return Result.failure(readResult.exceptionOrNull()!!)
        }

        while (intRead.hasRemaining()) {
            intRead.put(readBuffer.get())
        }
        intRead.flip()
        return Result.success(intRead.getInt())
    }

    override suspend fun readBuffer(size: Int): Result<ByteReadBuffer> {
        val bytes = ByteArray(size)
        if (readBuffer.remaining() >= size) {
            readBuffer.get(bytes)
            return Result.success(ByteReadBuffer(bytes))
        }
        val available = readBuffer.remaining()
        readBuffer.get(bytes, 0, available)

        val readResult = readIntoBuffer(size - available)
        if (readResult.isFailure) {
            return Result.failure(readResult.exceptionOrNull()!!)
        }

        readBuffer.get(bytes, available, bytes.size - available)
        return Result.success(ByteReadBuffer(bytes))
    }

    override fun release() {
        socket.close()
    }
}
