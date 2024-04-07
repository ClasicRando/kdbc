package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.BackendKeyData]. This message is sent after a successful login.
 * Contents are:
 * - the process ID of the backend receiving messages from this connection
 * - the secret key of the backend
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BACKENDKEYDATA)
 */
internal object BackendKeyDataDecoder : MessageDecoder<PgMessage.BackendKeyData> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.BackendKeyData {
        return buffer.use {
            PgMessage.BackendKeyData(it.readInt(), it.readInt())
        }
    }
}
