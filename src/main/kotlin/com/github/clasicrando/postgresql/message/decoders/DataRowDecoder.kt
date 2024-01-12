package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readBytes
import io.ktor.utils.io.core.readInt
import io.ktor.utils.io.core.readShort

internal object DataRowDecoder : MessageDecoder<PgMessage.DataRow> {
    override fun decode(packet: ByteReadPacket): PgMessage.DataRow {
        val columnCount = packet.readShort()
        val row = Array(columnCount.toInt()) {
            val length = packet.readInt()
            length.takeIf { it > 0 }
                ?.let { packet.readBytes(n = it) }
        }
        return PgMessage.DataRow(row)
    }
}