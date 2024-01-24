package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.postgresql.message.PgMessage

internal interface PgWriteMessage {
    @JvmInline
    value class Multiple(val messages: Iterable<PgMessage>): PgWriteMessage
    @JvmInline
    value class Single(val message: PgMessage): PgWriteMessage
}
