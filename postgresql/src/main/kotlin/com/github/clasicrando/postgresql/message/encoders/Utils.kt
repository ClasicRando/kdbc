package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal fun ByteWriteBuffer.writeCode(message: PgMessage) {
    writeByte(message.code)
}
