package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeInt

internal fun BytePacketBuilder.writeCode(message: PgMessage) {
    writeByte(message.code)
}

fun BytePacketBuilder.writeCString(content: String, charset: Charset) {
    writeFully(content.toByteArray(charset = charset))
    writeByte(0)
}

inline fun BytePacketBuilder.writeLengthPrefixed(block: BytePacketBuilder.() -> Unit) {
    val size: Int
    val packet = buildPacket {
        block()
        size = this.size
    }
    writeInt(size + 4)
    writePacket(packet)
}

inline fun BytePacketBuilder.writeLengthPrefixedWithoutSelf(block: BytePacketBuilder.() -> Unit) {
    val size: Int
    val packet = buildPacket {
        block()
        size = this.size
    }
    writeInt(size)
    writePacket(packet)
}
