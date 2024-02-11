package com.github.clasicrando.common.message

import com.github.clasicrando.common.buffer.ArrayListBuffer
import com.github.clasicrando.common.buffer.buildBytes

class MessageSendBuffer @PublishedApi internal constructor() : ArrayListBuffer() {
    companion object {
        inline fun buildByteArray(block: MessageSendBuffer.() -> Unit): ByteArray {
            return MessageSendBuffer().buildBytes(block)
        }
    }
}
