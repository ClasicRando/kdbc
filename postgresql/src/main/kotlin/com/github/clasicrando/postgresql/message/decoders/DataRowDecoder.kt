package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.asReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.row.PgRowBuffer
import io.ktor.utils.io.core.ByteReadPacket

internal object DataRowDecoder : MessageDecoder<PgMessage.DataRow> {
    override fun decode(packet: ByteReadPacket): PgMessage.DataRow {
        val rowBuffer = PgRowBuffer(packet.asReadBuffer())
        return PgMessage.DataRow(rowBuffer)
    }
}