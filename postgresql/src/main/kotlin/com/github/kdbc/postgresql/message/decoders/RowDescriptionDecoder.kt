package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.column.PgColumnDescription
import com.github.kdbc.postgresql.column.PgType
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.RowDescription]. This message is sent when the client issues a
 * describe request against a statement or portal. The message describes how the rows of a query
 * result should look like and is decoded into a [List] of [PgColumnDescription]. The contents are:
 *
 * - the number of fields as a [Short]
 * - for each field
 *     - the field name as a CString
 *     - the field's table OID if the field is identified as a columns of a specific table
 *     (otherwise zero)
 *     - the field's attribute number if the field is identified as a column of a specific table
 *     (otherwise zero)
 *     - the OID of the field's data type
 *     - the data type size (negative value denotes a variable-width type like varchar)
 *     - the type modifier
 *     - the format code of the field. Currently zero (text) and one (binary) are the only values
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION)
 */
internal object RowDescriptionDecoder : MessageDecoder<PgMessage.RowDescription> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.RowDescription {
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
