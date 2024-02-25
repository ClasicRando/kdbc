package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.result.PgRowBuffer

internal object DataRowDecoder : MessageDecoder<PgMessage.DataRow> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.DataRow {
        val rowBuffer = PgRowBuffer(buffer)
        return PgMessage.DataRow(rowBuffer)
    }
}