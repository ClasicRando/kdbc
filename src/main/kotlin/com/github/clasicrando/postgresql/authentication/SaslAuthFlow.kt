package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.message.PgMessage
import com.ongres.scram.client.ScramClient
import com.ongres.scram.client.ScramSession
import com.ongres.scram.common.stringprep.StringPreparations
import io.github.oshai.kotlinlogging.Level

/**
 * Using the provided [authMechanisms], initialize a [ScramClient] and create a [ScramSession] for
 * creating the first client message as well as the final client message sent in a later step.
 */
private suspend fun PgConnection.sendScramInit(
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
 * server sent back the expected message. Otherwise, null is returned.
 */
private suspend fun PgConnection.receiveContinueMessage(): Authentication.SaslContinue? {
    val continueMessage = this.receiveServerMessage()
    if (continueMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to continueMessage.code)
        }
        return null
    }
    val continueAuthMessage = continueMessage.authentication
    if (continueAuthMessage !is Authentication.SaslContinue) {
        this.log(Level.TRACE) {
            message = "Expected a SASL Continue message but got {authMessage}"
            payload = mapOf("authMessage" to continueAuthMessage)
        }
        return null
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
private suspend fun PgConnection.sendClientFinalMessage(
    continueAuthMessage: Authentication.SaslContinue,
    session: ScramSession,
): ScramSession.ClientFinalProcessor {
    val password = this.connectOptions.password ?: error("Missing Password")
    val serverFirstProcessor = session.receiveServerFirstMessage(continueAuthMessage.saslData)
    val clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password)
    val responseMessage = PgMessage.SaslResponse(clientFinalProcessor.clientFinalMessage())
    this.log(Level.TRACE) { message = "Sending SASL Response" }
    this.writeToStream(responseMessage)
    return clientFinalProcessor
}

/**
 * Wait for the next server message, returning the [Authentication.SaslFinal] message if the server
 * sent back the expected message. Otherwise, null is returned.
 */
private suspend fun PgConnection.receiveFinalAuthMessage(): Authentication.SaslFinal? {
    val finalMessage = this.receiveServerMessage()
    if (finalMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to finalMessage.code)
        }
        return null
    }
    val finalAuthMessage = finalMessage.authentication
    if (finalAuthMessage !is Authentication.SaslFinal) {
        this.log(Level.ERROR) {
            message = "Expected an OK auth message but got {authMessage}"
            payload = mapOf("authMessage" to finalAuthMessage)
        }
        return null
    }
    return finalAuthMessage
}

/** Wait for the next server message, returning true if the message is [Authentication.Ok] */
private suspend fun PgConnection.receiveOkAuthMessage(): Boolean {
    val okMessage = this.receiveServerMessage()
    if (okMessage !is PgMessage.Authentication) {
        this.log(Level.ERROR) {
            message = "Expected an Authentication message but got {code}"
            payload = mapOf("code" to okMessage.code)
        }
        return false
    }
    val okAuthMessage = okMessage.authentication
    if (okAuthMessage !is Authentication.Ok) {
        this.log(Level.ERROR) {
            message = "Expected an OK auth message but got {authMessage}"
            payload = mapOf("authMessage" to okMessage)
        }
        return false
    }
    return true
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
 * 6. Wait for and process the server response. If the message was an Authentication OK message,
 * return true, otherwise return false.
 *
 * @return True when authentication was successful, otherwise false
 */
internal suspend fun PgConnection.saslAuthFlow(auth: Authentication.Sasl): Boolean {
    val session = this.sendScramInit(auth.authMechanisms.toTypedArray())

    val continueAuthMessage = this.receiveContinueMessage() ?: return false

    val clientFinalProcessor = this.sendClientFinalMessage(continueAuthMessage, session)

    val finalAuthMessage = this.receiveFinalAuthMessage() ?: return false

    clientFinalProcessor.receiveServerFinalMessage(finalAuthMessage.saslData)

    return this.receiveOkAuthMessage()
}
