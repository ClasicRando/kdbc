package com.github.clasicrando.common.message

import com.github.clasicrando.common.buffer.ArrayListWriteBuffer
import com.github.clasicrando.common.buffer.buildBytes

class MessageSendBuffer @PublishedApi internal constructor() : ArrayListWriteBuffer() {
    companion object {
        inline fun buildByteArray(block: MessageSendBuffer.() -> Unit): ByteArray {
            return MessageSendBuffer().buildBytes(block)
        }
    }
}
