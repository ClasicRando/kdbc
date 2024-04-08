package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.stream.PgBlockingStream
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import com.ongres.scram.client.ScramClient
import com.ongres.scram.client.ScramSession
import com.ongres.scram.common.stringprep.StringPreparations
import io.github.oshai.kotlinlogging.Level

/**
 * Using the provided [authMechanisms], initialize a [ScramClient] and create a [ScramSession] for
 * creating the first client message as well as the final client message sent in a later step.
 */
private suspend fun PgStream.sendScramInit(
    authMechanisms: Array<String>,
): ScramSession {
    log(Level.TRACE) { message = "Starting Scram Client" }
    val scramClient = ScramClient
        .channelBinding(ScramClient.ChannelBinding.NO)
        .stringPreparation(StringPreparations.NO_PREPARATION)
        .selectMechanismBasedOnServerAdvertised(*authMechanisms)
        .setup()
    val session = scramClient.scramSession("*")
    val initialResponse = PgMessage.SaslInitialResponse(
        scramClient.scramMechanism.name,
        session.clientFirstMessage(),
    )
    log(Level.TRACE) { message = "Sending initial SASL Response" }
    writeToStream(initialResponse)
    return session
}

/**
 * Wait for the next server message, returning the [Authentication.SaslContinue] message if the
 * server sent back the expected message.
 */
private suspend fun PgStream.receiveContinueMessage(): Authentication.SaslContinue {
    val continueMessage = this.messages.receive()
    if (continueMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to continueMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $continueMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val continueAuthMessage = continueMessage.authentication
    if (continueAuthMessage !is Authentication.SaslContinue) {
        this.log(Level.TRACE) {
            message = "Expected a SASL Continue message but got {authMessage}"
            payload = mapOf("authMessage" to continueAuthMessage)
        }
        throw PgAuthenticationError("Expected a SaslContinue message but got $continueAuthMessage")
    }
    this.log(Level.TRACE) {
        message = "Received SASL Continue message"
    }
    return continueAuthMessage
}

/**
 * Using the supplied [continueAuthMessage] and scram [session], process the server's first message
 * and send the required client final message.
 */
private suspend fun PgStream.sendClientFinalMessage(
    continueAuthMessage: Authentication.SaslContinue,
    session: ScramSession,
): ScramSession.ClientFinalProcessor {
    val password = this.connectOptions.password ?: throw PgAuthenticationError("Missing Password")
    val serverFirstProcessor = session.receiveServerFirstMessage(continueAuthMessage.saslData)
    val clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password)
    val responseMessage = PgMessage.SaslResponse(clientFinalProcessor.clientFinalMessage())
    this.log(Level.TRACE) { message = "Sending SASL Response" }
    writeToStream(responseMessage)
    return clientFinalProcessor
}

/**
 * Wait for the next server message, returning the [Authentication.SaslFinal] message if the server
 * sent back the expected message.
 */
private suspend fun PgStream.receiveFinalAuthMessage(): Authentication.SaslFinal {
    val finalMessage = this.messages.receive()
    if (finalMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to finalMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $finalMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val finalAuthMessage = finalMessage.authentication
    if (finalAuthMessage !is Authentication.SaslFinal) {
        this.log(Level.ERROR) {
            message = "Expected a SaslFinal auth message but got {authMessage}"
            payload = mapOf("authMessage" to finalAuthMessage)
        }
        throw PgAuthenticationError("Expected a SaslFinal auth message but got $finalAuthMessage")
    }
    return finalAuthMessage
}

/**
 * Wait for the next server message, expecting an [Authentication.Ok]. If that is not the case,
 * throw a [PgAuthenticationError]
 */
private suspend fun PgStream.receiveOkAuthMessage() {
    val okMessage = this.messages.receive()
    if (okMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to okMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $okMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val okAuthMessage = okMessage.authentication
    if (okAuthMessage !is Authentication.Ok) {
        this.log(Level.ERROR) {
            message = "Expected an OK auth message but got {authMessage}"
            payload = mapOf("authMessage" to okMessage)
        }
        throw PgAuthenticationError("Expected an OK auth message but got $okAuthMessage")
    }
}

/**
 * Handles the various messages to and from the server for authentication using SASL (see
 * [docs](https://www.postgresql.org/docs/current/sasl-authentication.html)). This should be called
 * when the initial SASL request message is received from a postgresql server. Steps are as follows:
 *
 * 1. Initialize the scram session, sending the first client message
 * 2. Wait for and process the server response. Only a SASL Continue message will move to the next
 * step
 * 3. Using the existing scram session and contents of the previously received continue message,
 * prepare and send the final client message
 * 4. Wait for and process the server response. Only a SASL Final message will move to the next step
 * 5. Validate that the final server response was correct
 * 6. Wait for and process the server response expecting an Authentication OK message.
 *
 * If all steps have been followed then the [PgStream] has been authenticated.
 *
 * @throws PgAuthenticationError if the authentication flow failed for any reason. All other
 * [Throwable]s are also caught and added to a [PgAuthenticationError] as a suppressed error
 */
internal suspend fun PgStream.saslAuthFlow(auth: Authentication.Sasl) {
    try {
        val session = this.sendScramInit(auth.authMechanisms.toTypedArray())

        val continueAuthMessage = this.receiveContinueMessage()

        val clientFinalProcessor = this.sendClientFinalMessage(continueAuthMessage, session)

        val finalAuthMessage = this.receiveFinalAuthMessage()

        clientFinalProcessor.receiveServerFinalMessage(finalAuthMessage.saslData)

        this.receiveOkAuthMessage()
    } catch (ex: PgAuthenticationError) {
        throw ex
    } catch (ex: Throwable) {
        this.log(Level.TRACE) {
            message = "SASL auth flow error"
            cause = ex
        }
        val error = PgAuthenticationError("Generic SASL auth error")
        error.addSuppressed(ex)
        throw error
    }
}


/**
 * Using the provided [authMechanisms], initialize a [ScramClient] and create a [ScramSession] for
 * creating the first client message as well as the final client message sent in a later step.
 */
private fun PgBlockingStream.sendScramInit(
    authMechanisms: Array<String>,
): ScramSession {
    log(Level.TRACE) { message = "Starting Scram Client" }
    val scramClient = ScramClient
        .channelBinding(ScramClient.ChannelBinding.NO)
        .stringPreparation(StringPreparations.NO_PREPARATION)
        .selectMechanismBasedOnServerAdvertised(*authMechanisms)
        .setup()
    val session = scramClient.scramSession("*")
    val initialResponse = PgMessage.SaslInitialResponse(
        scramClient.scramMechanism.name,
        session.clientFirstMessage(),
    )
    log(Level.TRACE) { message = "Sending initial SASL Response" }
    writeToStream(initialResponse)
    return session
}

/**
 * Wait for the next server message, returning the [Authentication.SaslContinue] message if the
 * server sent back the expected message.
 */
private fun PgBlockingStream.receiveContinueMessage(): Authentication.SaslContinue {
    val continueMessage = this.receiveNextServerMessage()
    if (continueMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to continueMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $continueMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val continueAuthMessage = continueMessage.authentication
    if (continueAuthMessage !is Authentication.SaslContinue) {
        this.log(Level.TRACE) {
            message = "Expected a SASL Continue message but got {authMessage}"
            payload = mapOf("authMessage" to continueAuthMessage)
        }
        throw PgAuthenticationError("Expected a SaslContinue message but got $continueAuthMessage")
    }
    this.log(Level.TRACE) {
        message = "Received SASL Continue message"
    }
    return continueAuthMessage
}

/**
 * Using the supplied [continueAuthMessage] and scram [session], process the server's first message
 * and send the required client final message.
 */
private fun PgBlockingStream.sendClientFinalMessage(
    continueAuthMessage: Authentication.SaslContinue,
    session: ScramSession,
): ScramSession.ClientFinalProcessor {
    val password = this.connectOptions.password ?: throw PgAuthenticationError("Missing Password")
    val serverFirstProcessor = session.receiveServerFirstMessage(continueAuthMessage.saslData)
    val clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password)
    val responseMessage = PgMessage.SaslResponse(clientFinalProcessor.clientFinalMessage())
    this.log(Level.TRACE) { message = "Sending SASL Response" }
    writeToStream(responseMessage)
    return clientFinalProcessor
}

/**
 * Wait for the next server message, returning the [Authentication.SaslFinal] message if the server
 * sent back the expected message.
 */
private fun PgBlockingStream.receiveFinalAuthMessage(): Authentication.SaslFinal {
    val finalMessage = this.receiveNextServerMessage()
    if (finalMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to finalMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $finalMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val finalAuthMessage = finalMessage.authentication
    if (finalAuthMessage !is Authentication.SaslFinal) {
        this.log(Level.ERROR) {
            message = "Expected a SaslFinal auth message but got {authMessage}"
            payload = mapOf("authMessage" to finalAuthMessage)
        }
        throw PgAuthenticationError("Expected a SaslFinal auth message but got $finalAuthMessage")
    }
    return finalAuthMessage
}

/**
 * Wait for the next server message, expecting an [Authentication.Ok]. If that is not the case,
 * throw a [PgAuthenticationError]
 */
private fun PgBlockingStream.receiveOkAuthMessage() {
    val okMessage = this.receiveNextServerMessage()
    if (okMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to okMessage.code)
        }
        val errorMessage = "Expected an Authentication message but got $okMessage"
        throw PgAuthenticationError(errorMessage)
    }
    val okAuthMessage = okMessage.authentication
    if (okAuthMessage !is Authentication.Ok) {
        this.log(Level.ERROR) {
            message = "Expected an OK auth message but got {authMessage}"
            payload = mapOf("authMessage" to okMessage)
        }
        throw PgAuthenticationError("Expected an OK auth message but got $okAuthMessage")
    }
}

/**
 * Handles the various messages to and from the server for authentication using SASL (see
 * [docs](https://www.postgresql.org/docs/current/sasl-authentication.html)). This should be called
 * when the initial SASL request message is received from a postgresql server. Steps are as follows:
 *
 * 1. Initialize the scram session, sending the first client message
 * 2. Wait for and process the server response. Only a SASL Continue message will move to the next
 * step
 * 3. Using the existing scram session and contents of the previously received continue message,
 * prepare and send the final client message
 * 4. Wait for and process the server response. Only a SASL Final message will move to the next step
 * 5. Validate that the final server response was correct
 * 6. Wait for and process the server response expecting an Authentication OK message.
 *
 * If all steps have been followed then the [PgStream] has been authenticated.
 *
 * @throws PgAuthenticationError if the authentication flow failed for any reason. All other
 * [Throwable]s are also caught and added to a [PgAuthenticationError] as a suppressed error
 */
internal fun PgBlockingStream.saslAuthFlow(auth: Authentication.Sasl) {
    try {
        val session = this.sendScramInit(auth.authMechanisms.toTypedArray())

        val continueAuthMessage = this.receiveContinueMessage()

        val clientFinalProcessor = this.sendClientFinalMessage(continueAuthMessage, session)

        val finalAuthMessage = this.receiveFinalAuthMessage()

        clientFinalProcessor.receiveServerFinalMessage(finalAuthMessage.saslData)

        this.receiveOkAuthMessage()
    } catch (ex: PgAuthenticationError) {
        throw ex
    } catch (ex: Throwable) {
        this.log(Level.TRACE) {
            message = "SASL auth flow error"
            cause = ex
        }
        val error = PgAuthenticationError("Generic SASL auth error")
        error.addSuppressed(ex)
        throw error
    }
}
