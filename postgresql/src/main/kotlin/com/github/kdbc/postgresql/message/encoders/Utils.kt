package com.github.kdbc.postgresql.message.encoders

import com.github.kdbc.core.buffer.ByteWriteBuffer
import com.github.kdbc.postgresql.message.PgMessage

/** Special method to write the [PgMessage.code] to the [ByteWriteBuffer] */
internal fun ByteWriteBuffer.writeCode(message: PgMessage) {
    writeByte(message.code)
}
