package io.github.clasicrando.kdbc.mysql.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteArrayWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.stream.Stream
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.mysql.message.Capabilities

private const val RESOURCE_TYPE = "MySqlStream"

internal class MySqlStream(
    private val stream: Stream,
    internal val connectionOptions: MySqlConnectionOptions,
) : DefaultUniqueResourceId(),
    AutoCloseable {
    /** Returns true if the underlining [stream] is still connected */
    val isConnected: Boolean get() = stream.isConnected

    override val resourceType: String = RESOURCE_TYPE

    /** Reusable buffer for writing messages to the database server */
    private val messageSendBuffer: ByteWriteBuffer = ByteArrayWriteBuffer(SEND_BUFFER_SIZE)

    private var sequenceId = 0

    internal val capabilities =
        Capabilities.CLIENT_PROTOCOL_41 +
            Capabilities.CLIENT_IGNORE_SPACE +
            Capabilities.CLIENT_DEPRECATE_EOF +
            Capabilities.CLIENT_FOUND_ROWS +
            Capabilities.CLIENT_TRANSACTIONS +
            Capabilities.CLIENT_SECURE_CONNECTION +
            Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA +
            Capabilities.CLIENT_MULTI_STATEMENTS +
            Capabilities.CLIENT_MULTI_RESULTS +
            Capabilities.CLIENT_PLUGIN_AUTH +
            Capabilities.CLIENT_PS_MULTI_RESULTS +
            Capabilities.CLIENT_SSL +
            if (connectionOptions.database == null) {
                Capabilities(0)
            } else {
                Capabilities.CLIENT_CONNECT_WITH_DB
            }

    suspend fun sendPacket() {
    }

    suspend fun writePacket() {
    }

    override fun close() {
        if (stream.isConnected) {
            stream.close()
        }
    }

    companion object {
        private const val SEND_BUFFER_SIZE = 4096

        suspend fun connect(
            stream: Stream,
            connectionOptions: MySqlConnectionOptions,
        ): MySqlStream {
            stream.connect(timeout = connectionOptions.connectionTimeout)
            val mySqlStream =
                MySqlStream(
                    stream = stream,
                    connectionOptions = connectionOptions,
                )
        }
    }
}
