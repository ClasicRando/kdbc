package io.github.clasicrando.kdbc.postgresql.buffer

import kotlinx.io.Buffer
import kotlinx.io.DelicateIoApi
import kotlinx.io.Sink
import kotlinx.io.writeToInternalBuffer

/**
 * Write to this buffer, keeping track of the number of bytes written within the [block] to prefix
 * the bytes written with the number of bytes written as an [Int]. By default, the number of bytes
 * used to write the length value to the buffer is not included in the length value but this can be
 * overridden by supplying true for [includeLength].
 *
 * This operation works by capturing the pre-write buffer position, writing a placeholder [Int] of 0
 * to the buffer, executing the [block] to perform the desired write operations, calculating the
 * number of bytes written (excluding the length bytes if [includeLength] is false) and finally
 * updating the previously written [Int] length with the calculated value. As a consequence of not
 * knowing how many bytes may be written there is no way to verify the buffer has the remaining
 * capacity to successfully write all required bytes, meaning the operation is not transactional and
 * will perform write operations until the buffer overflows or the [block] completes. This should be
 * kept in mind if you attempt to dump the buffer contents when encountering an error.
 *
 * @throws IllegalStateException if the number of bytes written exceeds [Int.MAX_VALUE]
 */
@OptIn(DelicateIoApi::class)
internal inline fun Sink.writeLengthPrefixed(
    includeLength: Boolean = false,
    block: Sink.() -> Unit,
) {
    val tempBuffer = Buffer()
    block(tempBuffer)
    val length = tempBuffer.size + if (includeLength) 4 else 0
    check(length in 0..Int.MAX_VALUE)
    writeInt(length.toInt())
    this.writeToInternalBuffer { it.write(tempBuffer, tempBuffer.size) }
    transferFrom(tempBuffer)
}
