package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.postgresql.message.PgMessage

/** Special method to write the [PgMessage.code] to the [ByteWriteBuffer] */
internal fun ByteWriteBuffer.writeCode(message: PgMessage) {
    writeByte(message.code)
}
