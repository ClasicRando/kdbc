package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PasswordHelper
import com.github.clasicrando.postgresql.PgConnectionImpl
import com.github.clasicrando.postgresql.message.PgMessage

private fun createSimplePasswordMessage(
    connection: PgConnectionImpl,
    username: String,
    password: String,
    salt: ByteArray? = null,
): PgMessage.PasswordMessage {
    val passwordBytes = password.toByteArray(charset = connection.charset)
    val bytes = if (salt == null) {
        passwordBytes
    } else {
        PasswordHelper.encode(
            username = username.toByteArray(charset = connection.charset),
            password = passwordBytes,
            salt = salt,
        )
    }
    return PgMessage.PasswordMessage(bytes)
}

internal suspend fun simplePasswordAuthFlow(
    connection: PgConnectionImpl,
    username: String,
    password: String,
    salt: ByteArray? = null,
): Boolean {
    val passwordMessage = createSimplePasswordMessage(
        connection,
        username,
        password,
        salt,
    )
    connection.writeToStream(passwordMessage)

    val response = connection.receiveServerMessage()
    check(response is PgMessage.Authentication) {
        "Response after a simple password message should be an Authentication.Ok. Got $response"
    }
    val auth = response.authentication
    return auth !is Authentication.Ok
}
