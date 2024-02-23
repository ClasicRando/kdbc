package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readShort
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.column.PgColumnDescription

internal object RowDescriptionDecoder : MessageDecoder<PgMessage.RowDescription> {
    override fun decode(buffer: ReadBuffer): PgMessage.RowDescription {
        val descriptions = buffer.use { buf ->
            List(buf.readShort().toInt()) {
                PgColumnDescription(
                    fieldName = buf.readCString(),
                    tableOid = buf.readInt(),
                    columnAttribute = buf.readShort(),
                    pgType = PgType.fromOid(buf.readInt()),
                    dataTypeSize = buf.readShort(),
                    typeModifier = buf.readInt(),
                    formatCode = buf.readShort(),
                )
            }
        }
        return PgMessage.RowDescription(descriptions)
    }
}
