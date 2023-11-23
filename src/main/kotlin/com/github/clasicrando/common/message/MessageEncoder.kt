package com.github.clasicrando.common.message

import java.nio.ByteBuffer

interface MessageEncoder<in T> {
    fun encode(value: T, buffer: ByteBuffer)
}
