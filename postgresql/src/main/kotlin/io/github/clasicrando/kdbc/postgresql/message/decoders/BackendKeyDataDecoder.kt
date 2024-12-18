package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Buffer

/**
 * [MessageDecoder] for [PgMessage.BackendKeyData]. This message is sent after a successful login.
 * Contents are:
 * - the process ID of the backend receiving messages from this connection
 * - the secret key of the backend
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BACKENDKEYDATA)
 */
internal object BackendKeyDataDecoder : PgMessageDecoder<PgMessage.BackendKeyData>() {
    override fun decode(buffer: Buffer): PgMessage.BackendKeyData {
        return PgMessage.BackendKeyData(buffer.readInt(), buffer.readInt())
    }
}
