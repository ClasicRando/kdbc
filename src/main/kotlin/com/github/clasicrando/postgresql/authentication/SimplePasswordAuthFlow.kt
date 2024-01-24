package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PasswordHelper
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.Level

/** Create a new simple password message, hashing the password if a [salt] provided */
private fun PgStream.createSimplePasswordMessage(
    username: String,
    password: String,
    salt: ByteArray?,
): PgMessage.PasswordMessage {
    val passwordBytes = password.toByteArray(charset = this.connectOptions.charset)
    val bytes = if (salt == null) {
        passwordBytes
    } else {
        PasswordHelper.encode(
            username = username.toByteArray(charset = this.connectOptions.charset),
            password = passwordBytes,
            salt = salt,
        )
    }
    return PgMessage.PasswordMessage(bytes)
}

/**
 * Handles the process of authenticating the connection when simple passwords are used. These
 * methods include Cleartext and MD5 hashed passwords.
 *
 * Creates and sends a simple password messages (cleartext if not [salt] provided, otherwise MD5).
 * Connection then waits for a server response, only returning true if the response message is
 * Authentication OK.
 */
internal suspend fun PgStream.simplePasswordAuthFlow(
    username: String,
    password: String,
    salt: ByteArray? = null,
): Boolean {
    val passwordMessage = createSimplePasswordMessage(
        username,
        password,
        salt,
    )
    this.writeToStream(passwordMessage)

    val response = this.receiveNextServerMessage() ?: return false
    if (response !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Response after a simple password message should be an Authentication.Ok"
            payload = mapOf("response" to response)
        }
        return false
    }
    val auth = response.authentication
    return auth !is Authentication.Ok
}
