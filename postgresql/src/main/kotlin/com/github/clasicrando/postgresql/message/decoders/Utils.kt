package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import java.nio.charset.Charset

const val zero: Byte = 0

fun ReadBuffer.readCString(charset: Charset = Charsets.UTF_8): String {
    val temp = generateSequence {
        val byte = this@readCString.readByte()
        byte.takeIf { it != zero }
    }.toList().toByteArray()
    return temp.toString(charset = charset)
}
