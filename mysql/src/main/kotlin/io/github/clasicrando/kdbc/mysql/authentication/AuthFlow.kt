package io.github.clasicrando.kdbc.mysql.authentication

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.decoders.AuthSwitchRequestDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.HandshakeDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.OkDecoder
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream.Companion.MAX_PACKET_SIZE
import kotlinx.io.Source
import kotlinx.io.readByteArray

internal suspend fun MySqlStream.authFlow() {
    val handshake =
        HandshakeDecoder.decode(buffer = this.receiveNextPacket(), context = Unit)
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
            collation = this.collation.code.toByte(),
            username = connectionOptions.username,
            authPlugin = plugin,
            authResponse = authResponse,
        )
    this.writePacket(handshakeResponse)

    while (true) {
        val packet = this.receiveNextPacket()
        when (val id = packet.peek().readByteAsInt()) {
            0x00 -> {
                OkDecoder.decode(packet, Unit)
                break
            }
            0xfe -> {
                val switchRequest = AuthSwitchRequestDecoder.decode(packet, connectionOptions.allowClearTextPlugin)
                plugin = switchRequest.plugin
                val response = this.createAuthResponse(
                    authPlugin = plugin,
                    password = connectionOptions.password ?: "",
                    authPluginData = switchRequest.data,
                )
                this.writePacket(MysqlMessage.AuthSwitchResponse(response))
            }
            else -> {
                if (plugin != null && connectionOptions.password != null) {
                    if (this.handleAuthResponse(plugin, packet, connectionOptions.password, handshake.authPluginData)) {
                        break
                    }
                } else {
                    throw KdbcException("Unexpected packet 0x${id.toHexString()}")
                }
            }
        }
    }
}

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
        AuthPlugin.Sha256Password -> {
            if (isTls) {
                return password.toByteArray().plus(0)
            }
            encryptRsa(0x01, password, authPluginData)
        }
        AuthPlugin.MySqlClearPassword -> password.toByteArray().plus(0)
    }
}

private suspend fun MySqlStream.handleAuthResponse(
    authPlugin: AuthPlugin,
    packet: Source,
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
                throw KdbcException(
                    "Unexpected result from fast authentication 0x${nextByte.toHexString()} when expected 0x03 (AUTH_OK) or 0x04 (AUTH_CONTINUE)"
                )
        }
    }
    throw KdbcException(
        "Unexpected packet 0x${firstByte.toHexString()} for auth plugin $authPlugin during authentication"
    )
}

private suspend fun MySqlStream.encryptRsa(
    byte: Byte,
    password: String,
    authPluginData: ByteArray,
): ByteArray {
    writePacket(MysqlMessage.SingleByte(byte))
    val bytes = receiveNextPacket()
    bytes.skip(1)
    return PasswordHelper.encryptWithPublicKey(
        publicKeyBytes = bytes.readByteArray(),
        passwordCStringBytes = password.toByteArray().plus(0),
        authPluginData = authPluginData,
    )
}
