package com.github.clasicrando.postgresql.message.decoders

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

const val zero: Byte = 0

fun ByteReadPacket.readCString(charset: Charset = Charsets.UTF_8): String {
    val temp = generateSequence {
        val byte = this@readCString.readByte()
        byte.takeIf { it != zero }
    }.toList().toByteArray()
    return temp.toString(charset = charset)
}
