package io.github.clasicrando.kdbc.mysql.authentication

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.decoders.AuthSwitchRequestDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.HandshakeDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.OkDecoder
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream.Companion.DEFAULT_CHARSET
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream.Companion.MAX_PACKET_SIZE

/**
 * Move through the authentication flow for a MySQL connection. The steps are:
 * 1. Decode and process the initial handshake packet sent from the server
 * 2. Perform a TLS handshake (if needed)
 * 3. Prepare and send a handshake response packet
 * 4. Receive packets until an Ok packet is sent, handling switch requests sent back to client and
 *    auth continuation packets for supported auth plugins
 */
internal suspend fun MySqlStream.authFlow() {
    val handshake = HandshakeDecoder.decode(buffer = this.receiveNextPacket(), context = Unit)
    var plugin = handshake.authPlugin

    val serverVersion = handshake.serverVersion.splitToSequence('.').iterator()
    val major = if (serverVersion.hasNext()) serverVersion.next().toIntOrNull() ?: 0 else 0
    val minor = if (serverVersion.hasNext()) serverVersion.next().toIntOrNull() ?: 0 else 0
    val patch = if (serverVersion.hasNext()) serverVersion.next().toIntOrNull() ?: 0 else 0
    this.serverVersion = Triple(major, minor, patch)
    this.capabilities = this.capabilities and handshake.serverCapabilities
    this.capabilities = this.capabilities + Capabilities.CLIENT_PROTOCOL_41

    upgradeIfNeeded()

    val authResponse =
        if (plugin != null && connectionOptions.password != null) {
            this.createAuthResponse(
                authPlugin = plugin,
                password = connectionOptions.password,
                authPluginData = handshake.authPluginData,
            )
        } else {
            null
        }

    val handshakeResponse =
        MysqlMessage.HandshakeResponse(
            database = connectionOptions.database,
            maxPacketSize = MAX_PACKET_SIZE,
            characterSet = DEFAULT_CHARSET,
            username = connectionOptions.username,
            authPlugin = plugin,
            authResponse = authResponse,
            sessionProperties = connectionOptions.properties,
        )
    this.writePacket(handshakeResponse)

    while (true) {
        val packet = this.receiveNextPacket()
        when (val id = packet.peekNextAsInt()) {
            0x00 -> {
                OkDecoder.decode(packet, Unit)
                break
            }
            0xfe -> {
                val switchRequest =
                    AuthSwitchRequestDecoder.decode(packet, connectionOptions.allowClearTextPlugin)
                plugin = switchRequest.plugin
                val response =
                    this.createAuthResponse(
                        authPlugin = plugin,
                        password = connectionOptions.password ?: "",
                        authPluginData = switchRequest.data,
                    )
                this.writePacket(MysqlMessage.AuthSwitchResponse(response))
            }
            else -> {
                if (plugin != null && connectionOptions.password != null) {
                    if (
                        this.handleAuthResponse(
                            plugin,
                            packet,
                            connectionOptions.password,
                            handshake.authPluginData,
                        )
                    ) {
                        break
                    }
                } else {
                    throw MySqlException("Unexpected packet 0x${id.toHexString()}")
                }
            }
        }
    }
}

/**
 * Create an auth response data chunk depending on the [authPlugin] specified.
 * - MySQL Native -> encrypt using SHA1 and plugin data
 * - Caching SHA256 -> encrypt using SHA256 algorithm and plugin data
 * - SHA256 -> if TLS active, send null terminated string of password, otherwise call [encryptRsa]
 *   to use the server's public key to encrypt
 * - Clear Text -> send null terminated string of password
 */
private suspend fun MySqlStream.createAuthResponse(
    authPlugin: AuthPlugin,
    password: String,
    authPluginData: ByteArray,
): ByteArray {
    return when (authPlugin) {
        AuthPlugin.MySqlNativePassword ->
            PasswordHelper.encryptPasswordSha1(password, authPluginData)
        AuthPlugin.CachingSha2Password ->
            PasswordHelper.encryptPasswordSha256(password, authPluginData)
        AuthPlugin.Sha256Password if isTls -> password.toByteArray().plus(0)
        AuthPlugin.Sha256Password -> encryptRsa(0x01, password, authPluginData)
        AuthPlugin.MySqlClearPassword -> password.toByteArray().plus(0)
    }
}

/**
 * Handle auth continuation response by checking the first byte of the [packet] and sending another
 * payload if the server is requesting more packets. The next packet will always be the result of
 * [encryptRsa]. Only valid for [AuthPlugin.CachingSha2Password].
 */
private suspend fun MySqlStream.handleAuthResponse(
    authPlugin: AuthPlugin,
    packet: ByteReadBuffer,
    password: String,
    authPluginData: ByteArray,
): Boolean {
    val firstByte = packet.readByteAsInt()
    if (authPlugin == AuthPlugin.CachingSha2Password && firstByte == 0x01) {
        when (val nextByte = packet.readByteAsInt()) {
            0x03 -> return true
            0x04 -> {
                val payload = encryptRsa(0x02, password, authPluginData)
                writePacket(MysqlMessage.MultiByte(payload))
                return false
            }
            else ->
                throw MySqlException(
                    "Unexpected result from fast authentication 0x${nextByte.toHexString()} when expected 0x03 (AUTH_OK) or 0x04 (AUTH_CONTINUE)"
                )
        }
    }
    throw MySqlException(
        "Unexpected packet 0x${firstByte.toHexString()} for auth plugin $authPlugin during authentication"
    )
}

/**
 * Send a single [byte] to request the server's public key, forwarding that key to
 * [PasswordHelper.encryptWithPublicKey] to create the required auth data
 */
private suspend fun MySqlStream.encryptRsa(
    byte: Byte,
    password: String,
    authPluginData: ByteArray,
): ByteArray {
    if (isTls) {
        return password.toByteArray().plus(0)
    }
    writePacket(MysqlMessage.SingleByte(byte))
    val bytes = receiveNextPacket()
    bytes.skip(1)
    return PasswordHelper.encryptWithPublicKey(
        publicKeyBytes = bytes.readBytes(),
        passwordCStringBytes = password.toByteArray().plus(0),
        authPluginData = authPluginData,
    )
}
