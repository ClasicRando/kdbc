package io.github.clasicrando.kdbc.mysql.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.core.stream.Stream
import io.github.clasicrando.kdbc.mysql.authentication.authFlow
import io.github.clasicrando.kdbc.mysql.buffer.read3ByteIntLe
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.write3ByteIntLe
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
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

private val logger = KotlinLogging.logger {}
private const val RESOURCE_TYPE = "MySqlStream"

/**
 * [Stream] wrapper class for facilitating postgresql specific message protocol behaviour. A
 * [MySqlConnection] will own a [MySqlStream] and utilize it's public methods to process incoming
 * server messages as well as send commands to the server.
 */
internal class MySqlStream(val innerStream: Stream, val connectionOptions: MySqlConnectionOptions) :
    DefaultUniqueResourceId(), AutoCloseable {
    /** Returns true if the underlining [innerStream] is still connected */
    val isConnected: Boolean
        get() = innerStream.isConnected

    override val resourceType: String = RESOURCE_TYPE

    /** Current packet sequence ID, resets to 0 at the start of each command group */
    private var sequenceId = 0
    var serverVersion: Triple<Int, Int, Int> = Triple(0, 0, 0)
    var isTls = false
        private set

    private val waitingQueue = ArrayDeque<Waiting>()
    val prePacketBuffer = ByteWriteBuffer(MAX_PACKET_SIZE * 4)
    val packetReadingBuffer = ByteWriteBuffer(MAX_PACKET_SIZE * 4)

    /**
     * Current client capabilities. Initialized with certain values but gets updated to remove
     * capabilities that the server does not possess.
     */
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
            Capabilities.CLIENT_CONNECT_ATTRS +
            if (connectionOptions.database == null) {
                LongBitFlags(0L)
            } else {
                Capabilities.CLIENT_CONNECT_WITH_DB
            }

    fun resetSequenceId() {
        sequenceId = 0
    }

    fun getAndIncrementSequenceId(): Byte {
        val result = sequenceId
        if (this.sequenceId >= 255) {
            log(Level.WARN) {
                this.message = "SequenceId overflowed. This will break your connection"
            }
            this.sequenceId = 0
            throw KdbcException("Message sequenceId overflowed")
        } else {
            this.sequenceId = this.sequenceId + 1
        }
        return result.toByte()
    }

    /** Replace the first entry in the queue with this new value */
    fun updateFirstWaiting(newWaiting: Waiting) {
        waitingQueue.removeFirst()
        waitingQueue.addFirst(newWaiting)
    }

    /** Add this [waiting] to the end of the queue. */
    fun addLastWaiting(waiting: Waiting) {
        waitingQueue.addLast(waiting)
    }

    /** Remove the first waiting element if any present */
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

    /**
     * Attempt to upgrade the stream to be TLS stream. Checks the updated [Capabilities] after
     * receiving the initial handshake sent by the server to confirm TLS connections are supported
     * by the server and client. If TLS is available then the [MysqlMessage.SslRequest] packet is
     * sent and the [Stream.upgradeTls] method is called.
     */
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
                    throw MySqlException("Server does not support TLS")
                }
            }
        }

        writeMessage(
            MysqlMessage.SslRequest(maxPacketSize = MAX_PACKET_SIZE, characterSet = DEFAULT_CHARSET)
        )

        innerStream.upgradeTls(connectionOptions.connectionTimeout)
        isTls = true
    }

    /** Wait until all queued messages from the server are processed before exiting. */
    suspend fun waitUntilReady() {
        while (waitingQueue.isNotEmpty()) {
            while (waitingQueue.firstOrNull() == Waiting.Row) {
                val packet = receiveNextPacket()
                if (packet.peekNextAsInt() == 0xfe && packet.remaining < 9) {
                    val eof = EofDecoder.decode(packet, Unit)
                    removeFirstWaitingIfAny()
                    if (eof.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        waitingQueue.addFirst(Waiting.Result)
                    }
                }
            }

            while (waitingQueue.firstOrNull() == Waiting.Result) {
                val packet = receiveNextPacket()
                if (packet.peekNextAsInt() == 0x00) {
                    val ok = OkDecoder.decode(packet, Unit)
                    if (!ok.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        removeFirstWaitingIfAny()
                    }
                } else {
                    updateFirstWaiting(Waiting.Row)
                    skipResultMetadata(packet)
                }
            }
        }
    }

    /**
     * Read the next packet for it's size and sequence ID, returning this packet's size that was
     * read into the internal buffer
     */
    private suspend fun readRawPacketIntoBuffer(): Int {
        val header = innerStream.readBuffer(4)
        val packetSize = header.read3ByteIntLe()
        val sequenceId = header.readByteAsInt()
        this.sequenceId = if (sequenceId >= 255) 1 else sequenceId + 1
        innerStream.readIntoBuffer(packetReadingBuffer, packetSize)
        return packetSize
    }

    /**
     * Read the next packet and if the first packet's size reaches the maximum, keep reading packets
     * until the most recent packet's size is not the max packet size, combining each packet body
     * into 1 final packet body that is returned.
     *
     * @throws MySqlException if the packet is an [MysqlMessage.Err] packet or the packet body is
     *   empty
     */
    suspend fun receiveNextPacket(): ByteReadBuffer {
        var lastRead = readRawPacketIntoBuffer()
        while (lastRead >= MAX_PACKET_SIZE) {
            lastRead = readRawPacketIntoBuffer()
        }
        val payload = packetReadingBuffer.toReadBuffer()

        if (payload.isExhausted) {
            throw MySqlException("Received empty packet")
        }

        if (payload.peekNextAsInt() == 0xff) {
            removeFirstWaitingIfAny()
            val err = ErrDecoder.decode(payload, capabilities)
            throw MySqlException(
                "errorCode=${err.errorCode}, sqlState=${err.sqlState}, errorMessage='${err.errorMessage}'"
            )
        }
        return payload
    }

    /**
     * Read the number of columns from this [packet] as a length encoded integer and skip that
     * number of packets
     */
    private suspend fun skipResultMetadata(packet: ByteReadBuffer) {
        val columnsCount = packet.readLongLengthEncoded()
        (1..columnsCount).forEach { receiveNextPacket() }
    }

    /** Shorthand for `OkDecoder.decode(receiveNextPacket())` */
    suspend fun receiveOk(): MysqlMessage.Ok {
        return OkDecoder.decode(receiveNextPacket())
    }

    /**
     * Receive the next packet and decode using a [MessageDecoder] that has a context of
     * [Capabilities]
     */
    suspend fun <M : MysqlMessage> receiveNext(decoder: MessageDecoder<M, LongBitFlags>): M {
        return decoder.decode(receiveNextPacket(), capabilities)
    }

    /**
     * Send this [message] to the server as the initial packet in a group of subsequent packets
     * (i.e. [sequenceId] is reset to 0)
     */
    suspend fun writeInitialMessage(message: MysqlMessage) {
        resetSequenceId()
        writeMessage(message)
    }

    /**
     * Write this [message] to the server by finding its encoder, and writing the contents into one
     * or more packets. Each packet will have the next sequence ID and [sequenceId] will be updated
     * after all packets are written.
     */
    suspend fun writeMessage(message: MysqlMessage) {
        MySqlMessageEncoders.encode(message, prePacketBuffer, capabilities)
        innerStream.writeTo { sink ->
            prePacketBuffer.useAsReadBuffer { readBuffer ->
                do {
                    val length = minOf(readBuffer.remaining, MAX_PACKET_SIZE - 4)
                    sink.write3ByteIntLe(length.toInt())
                    sink.writeByte(getAndIncrementSequenceId())
                    readBuffer.transferToSink(sink, length)
                } while (readBuffer.remaining > 0)
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
        const val MAX_PACKET_SIZE = 4096
        const val TLS_REJECT_WARNING =
            "Preferred SSL mode was rejected by server. Continuing with non TLS connection"

        suspend fun MySqlStream.setSessionVariables(options: List<String>) {
            try {
                writeInitialMessage(
                    MysqlMessage.Query("SET ${options.joinToString(separator = ",")};")
                )
                receiveOk()
            } catch (ex: Exception) {
                throw MySqlException("Could not set session variables: $options", ex)
            }
        }

        /**
         * Connect to a MySQL database using the underlining [stream], negotiating the initial
         * authentication flow and setting some session variables using a `SET` command before
         * returning.
         */
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
