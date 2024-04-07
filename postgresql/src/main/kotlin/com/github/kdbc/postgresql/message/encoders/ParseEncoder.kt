package com.github.kdbc.postgresql.message.encoders

import com.github.kdbc.core.buffer.ByteWriteBuffer
import com.github.kdbc.core.message.MessageEncoder
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.Parse]. This message is sent to request the backend parse a
 * statement. The contents are:
 * - a header [Byte] of 'P'
 * - the length of the following data (including the size of the [Int] length)
 * - the name of the resulting prepared statement (an empty string creates an unnamed statement)
 * - the query to be parsed as a CString
 * - the number of parameter data types specified. For our purposes this always matches the number
 * of parameters supplied to the query executor
 * - each data type OID of all parameters provided
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARSE)
 */
internal object ParseEncoder : MessageEncoder<PgMessage.Parse> {
    override fun encode(value: PgMessage.Parse, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.preparedStatementName)
            writeCString(value.query)
            writeShort(value.parameterTypes.size.toShort())
            for (type in value.parameterTypes) {
                writeInt(type)
            }
        }
    }
}
