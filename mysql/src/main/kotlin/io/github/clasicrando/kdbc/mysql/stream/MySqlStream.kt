package io.github.clasicrando.kdbc.mysql.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.core.stream.Stream
import io.github.clasicrando.kdbc.mysql.authentication.authFlow
import io.github.clasicrando.kdbc.mysql.buffer.read3ByteIntLe
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.writePackets
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import io.github.clasicrando.kdbc.mysql.message.decoders.EofDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.ErrDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.OkDecoder
import io.github.clasicrando.kdbc.mysql.message.encoders.MySqlMessageEncoders
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.io.Buffer
import kotlinx.io.InternalIoApi
import kotlinx.io.Source

private val logger = KotlinLogging.logger {}
private const val RESOURCE_TYPE = "MySqlStream"

internal class MySqlStream(val innerStream: Stream, val connectionOptions: MySqlConnectionOptions) :
    DefaultUniqueResourceId(), AutoCloseable {
    /** Returns true if the underlining [innerStream] is still connected */
    val isConnected: Boolean
        get() = innerStream.isConnected

    override val resourceType: String = RESOURCE_TYPE

    private var sequenceId = 0
    var serverVersion: Triple<Int, Int, Int> = Triple(0, 0, 0)
    var isTls = false
        private set

    private val waitingQueue = ArrayDeque<Waiting>()

    var capabilities =
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
            Capabilities.CLIENT_LOCAL_FILES +
            Capabilities.CLIENT_CONNECT_ATTRS +
            if (connectionOptions.database == null) {
                Capabilities(0uL)
            } else {
                Capabilities.CLIENT_CONNECT_WITH_DB
            }

    fun updateFirstWaiting(newWaiting: Waiting) {
        waitingQueue.removeFirst()
        waitingQueue.addFirst(newWaiting)
    }

    fun addLastWaiting(waiting: Waiting) {
        waitingQueue.addLast(waiting)
    }

    fun removeFirstWaitingIfAny() {
        if (waitingQueue.isNotEmpty()) {
            waitingQueue.removeFirst()
        }
    }

    /**
     * Created a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logWithResource(logger, level, block)
    }

    suspend fun upgradeIfNeeded() {
        val serverSupportsTls = this.capabilities[Capabilities.CLIENT_SSL]
        if (connectionOptions.sslMode == SslMode.Disable) {
            this.capabilities -= Capabilities.CLIENT_SSL
        }

        when (connectionOptions.sslMode) {
            SslMode.Disable -> return
            SslMode.Allow,
            SslMode.Prefer -> {
                if (!serverSupportsTls) {
                    log(Level.WARN) { message = TLS_REJECT_WARNING }
                    return
                }
            }
            SslMode.Require,
            SslMode.VerifyCa,
            SslMode.VerifyFull -> {
                if (!serverSupportsTls) {
                    throw KdbcException("Server does not support TLS")
                }
            }
        }

        writePacket(
            MysqlMessage.SslRequest(maxPacketSize = MAX_PACKET_SIZE, characterSet = DEFAULT_CHARSET)
        )

        innerStream.upgradeTls(connectionOptions.connectionTimeout)
        isTls = true
    }

    suspend fun waitUntilReady() {
        while (waitingQueue.isNotEmpty()) {
            while (waitingQueue.firstOrNull() == Waiting.Row) {
                val packet = receiveNextPacket()
                if (packet.peek().readByteAsInt() == 0xfe && packet.size < 9) {
                    val eof = EofDecoder.decode(packet, Unit)
                    removeFirstWaitingIfAny()
                    if (eof.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        waitingQueue.addFirst(Waiting.Result)
                    }
                }
            }

            while (waitingQueue.firstOrNull() == Waiting.Result) {
                val packet = receiveNextPacket()
                if (packet.peek().readByteAsInt() == 0x00) {
                    val of = OkDecoder.decode(packet, Unit)
                    if (of.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        removeFirstWaitingIfAny()
                    }
                } else {
                    updateFirstWaiting(Waiting.Row)
                    skipResultMetadata(packet)
                }
            }
        }
    }

    private suspend fun readRawPacket(): Buffer {
        val header = innerStream.readBuffer(4)
        val packetSize = header.read3ByteIntLe()
        val sequenceId = header.readByteAsInt()
        this.sequenceId = if (sequenceId >= 255) 1 else sequenceId + 1
        return innerStream.readBuffer(packetSize)
    }

    @OptIn(InternalIoApi::class)
    suspend fun receiveNextPacket(): Buffer {
        val payload = readRawPacket()
        if (payload.buffer.size >= 0xff_ff_ff) {
            var lastRead = 0xff_ff_ffL
            while (lastRead == 0xff_ff_ffL) {
                val nextPayload = readRawPacket()
                lastRead = nextPayload.size
                payload.write(nextPayload, nextPayload.size)
            }
        }

        if (payload.exhausted()) {
            throw KdbcException("Received empty packet")
        }

        if (payload.peek().readByteAsInt() == 0xff) {
            removeFirstWaitingIfAny()
            val err = ErrDecoder.decode(payload, capabilities)
            throw MySqlException(
                "errorCode=${err.errorCode}, sqlState=${err.sqlState}, errorMessage='${err.errorMessage}'"
            )
        }
        return payload
    }

    suspend fun receiveEofIfPossible(): MysqlMessage.Eof? {
        if (capabilities[Capabilities.CLIENT_DEPRECATE_EOF]) {
            return null
        }
        return EofDecoder.decode(receiveNextPacket(), Unit)
    }

    private suspend fun skipResultMetadata(packet: Source) {
        val columnsCount = packet.readLongLengthEncoded()
        (0..columnsCount).forEach { receiveNextPacket() }
        receiveEofIfPossible()
    }

    suspend fun receiveOk(): MysqlMessage.Ok {
        return OkDecoder.decode(receiveNextPacket())
    }

    suspend fun <M : MysqlMessage> receiveNext(decoder: MessageDecoder<M, Capabilities>): M {
        return decoder.decode(receiveNextPacket(), capabilities)
    }

    suspend fun sendPacket(message: MysqlMessage) {
        sequenceId = 0
        writePacket(message)
    }

    suspend fun writePacket(message: MysqlMessage) {
        innerStream.writeTo {
            it.writePackets(currentSequenceId = sequenceId) {
                MySqlMessageEncoders.encode(message, this, capabilities)
            }
        }
    }

    override fun close() {
        if (innerStream.isConnected) {
            innerStream.close()
        }
    }

    companion object {
        const val DEFAULT_CHARSET = 224.toByte()
        const val MAX_PACKET_SIZE = 1024
        const val TLS_REJECT_WARNING =
            "Preferred SSL mode was rejected by server. Continuing with non TLS connection"

        suspend fun MySqlStream.setSessionVariables(options: List<String>) {
            try {
                sendPacket(MysqlMessage.Query("SET ${options.joinToString(separator = ",")};"))
                receiveOk()
            } catch (ex: Exception) {
                throw KdbcException("Could not set session variables: $options", ex)
            }
        }

        suspend fun connect(
            stream: Stream,
            connectionOptions: MySqlConnectionOptions,
        ): MySqlStream {
            stream.connect(timeout = connectionOptions.connectionTimeout)
            val mySqlStream =
                MySqlStream(innerStream = stream, connectionOptions = connectionOptions)
            mySqlStream.authFlow()

            val sessionVariables =
                buildList<String> {
                    if (connectionOptions.queryTimeout.isFinite()) {
                        add(
                            "MAX_EXECUTION_TIME=${connectionOptions.queryTimeout.inWholeMilliseconds}"
                        )
                    }
                    val sqlMode =
                        connectionOptions
                            .sqlModeOptions()
                            .filterNotNull()
                            .joinToString(
                                separator = ",",
                                prefix = "sql_mode=(SELECT CONCAT(@@sql_mode, ',",
                                postfix = "'))",
                            )
                    add(sqlMode)
                    add("TIME_ZONE=UTC")
                }
            mySqlStream.setSessionVariables(sessionVariables)
            return mySqlStream
        }
    }
}
