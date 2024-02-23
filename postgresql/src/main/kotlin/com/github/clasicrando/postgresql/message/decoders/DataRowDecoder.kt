package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ArrayReadBuffer
import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.result.PgRowBuffer

internal object DataRowDecoder : MessageDecoder<PgMessage.DataRow> {
    override fun decode(buffer: ReadBuffer): PgMessage.DataRow {
        val rowBuffer = PgRowBuffer(buffer as ArrayReadBuffer)
        return PgMessage.DataRow(rowBuffer)
    }
}