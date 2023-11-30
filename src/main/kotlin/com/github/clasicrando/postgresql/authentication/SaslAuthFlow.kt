package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PgConnectionImpl
import com.github.clasicrando.postgresql.message.PgMessage
import com.ongres.scram.client.ScramClient
import com.ongres.scram.client.ScramSession
import com.ongres.scram.common.stringprep.StringPreparations
import io.github.oshai.kotlinlogging.Level

private suspend fun PgConnectionImpl.sendScramInit(
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

private suspend fun PgConnectionImpl.receiveContinueMessage(): Authentication.SaslContinue? {
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

private suspend fun PgConnectionImpl.sendClientFinalMessage(
    continueAuthMessage: Authentication.SaslContinue,
    session: ScramSession,
): ScramSession.ClientFinalProcessor {
    val password = this.configuration.password ?: error("Missing Password")
    val serverFirstProcessor = session.receiveServerFirstMessage(continueAuthMessage.saslData)
    val clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password)
    val responseMessage = PgMessage.SaslResponse(clientFinalProcessor.clientFinalMessage())
    this.log(Level.TRACE) { message = "Sending SASL Response" }
    this.writeToStream(responseMessage)
    return clientFinalProcessor
}

private suspend fun PgConnectionImpl.receiveFinalAuthMessage(): Authentication.SaslFinal? {
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

private suspend fun PgConnectionImpl.receiveOkAuthMessage(): Boolean {
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

suspend fun PgConnectionImpl.saslAuthFlow(auth: Authentication.Sasl): Boolean {
    val session = this.sendScramInit(auth.authMechanisms.toTypedArray())

    val continueAuthMessage = this.receiveContinueMessage() ?: return false

    val clientFinalProcessor = this.sendClientFinalMessage(continueAuthMessage, session)

    val finalAuthMessage = this.receiveFinalAuthMessage() ?: return false

    clientFinalProcessor.receiveServerFinalMessage(finalAuthMessage.saslData)

    return this.receiveOkAuthMessage()
}
