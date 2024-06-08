package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.postgresql.PasswordHelper
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.stream.PgBlockingStream
import io.github.clasicrando.kdbc.postgresql.stream.PgSuspendingStream
import io.github.oshai.kotlinlogging.Level

/** Create a new simple password message, hashing the password if a [salt] provided */
private fun createSimplePasswordMessage(
    username: String,
    password: String,
    salt: ByteArray?,
): PgMessage.PasswordMessage {
    val passwordBytes = password.toByteArray()
    val bytes = if (salt == null) {
        passwordBytes
    } else {
        PasswordHelper.encode(
            username = username.toByteArray(),
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
 * Creates and sends a simple password messages (cleartext if no [salt] provided, otherwise MD5).
 * Connection then waits for a server response, only returning true if the response message is
 * Authentication OK.
 *
 * @throws PgAuthenticationError if the authentication flow failed for any reason. All other
 * [Throwable]s are also caught and added to a [PgAuthenticationError] as a suppressed error
 */
internal suspend fun PgSuspendingStream.simplePasswordAuthFlow(
    username: String,
    password: String,
    salt: ByteArray? = null,
) {
    try {
        val passwordMessage = createSimplePasswordMessage(
            username,
            password,
            salt,
        )
        this.writeToStream(passwordMessage)

        val response = this.receiveNextServerMessage()
        if (response !is PgMessage.Authentication) {
            this.log(Level.ERROR) {
                message = "Expected an Authentication message but got {code}"
                payload = mapOf("code" to response.code)
            }
            val errorMessage = "Expected an Authentication message but got $response"
            throw PgAuthenticationError(errorMessage)
        }
        val auth = response.authentication
        if (auth !is Authentication.Ok) {
            this.log(Level.ERROR) {
                message = "Expected an OK auth message but got {authMessage}"
                payload = mapOf("authMessage" to auth)
            }
            throw PgAuthenticationError("Expected an OK auth message but got $auth")
        }
    } catch (ex: PgAuthenticationError) {
        throw ex
    } catch (ex: Throwable) {
        val error = PgAuthenticationError("Generic SimplePassword auth error")
        error.addSuppressed(ex)
        throw error
    }
}

/**
 * Handles the process of authenticating the connection when simple passwords are used. These
 * methods include Cleartext and MD5 hashed passwords.
 *
 * Creates and sends a simple password messages (cleartext if no [salt] provided, otherwise MD5).
 * Connection then waits for a server response, only returning true if the response message is
 * Authentication OK.
 *
 * @throws PgAuthenticationError if the authentication flow failed for any reason. All other
 * [Throwable]s are also caught and added to a [PgAuthenticationError] as a suppressed error
 */
internal fun PgBlockingStream.simplePasswordAuthFlow(
    username: String,
    password: String,
    salt: ByteArray? = null,
) {
    try {
        val passwordMessage = createSimplePasswordMessage(
            username,
            password,
            salt,
        )
        this.writeToStream(passwordMessage)

        val response = this.receiveNextServerMessage()
        if (response !is PgMessage.Authentication) {
            this.log(Level.ERROR) {
                message = "Expected an Authentication message but got {code}"
                payload = mapOf("code" to response.code)
            }
            val errorMessage = "Expected an Authentication message but got $response"
            throw PgAuthenticationError(errorMessage)
        }
        val auth = response.authentication
        if (auth !is Authentication.Ok) {
            this.log(Level.ERROR) {
                message = "Expected an OK auth message but got {authMessage}"
                payload = mapOf("authMessage" to auth)
            }
            throw PgAuthenticationError("Expected an OK auth message but got $auth")
        }
    } catch (ex: PgAuthenticationError) {
        throw ex
    } catch (ex: Throwable) {
        val error = PgAuthenticationError("Generic SimplePassword auth error")
        error.addSuppressed(ex)
        throw error
    }
}
