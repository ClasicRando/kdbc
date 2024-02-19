package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal fun MessageSendBuffer.writeCode(message: PgMessage) {
    writeByte(message.code)
}
