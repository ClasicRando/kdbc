package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage.Companion.ZERO_CODE
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

fun ByteReadPacket.readCString(charset: Charset): String {
    val temp = generateSequence {
        val byte = this@readCString.readByte()
        byte.takeIf { it != ZERO_CODE }
    }.toList().toByteArray()
    return temp.toString(charset = charset)
}
