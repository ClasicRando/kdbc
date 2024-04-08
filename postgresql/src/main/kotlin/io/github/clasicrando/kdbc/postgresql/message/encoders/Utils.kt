package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/** Special method to write the [PgMessage.code] to the [ByteWriteBuffer] */
internal fun ByteWriteBuffer.writeCode(message: PgMessage) {
    writeByte(message.code)
}
