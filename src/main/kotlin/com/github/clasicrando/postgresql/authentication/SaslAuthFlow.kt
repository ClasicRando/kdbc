package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.postgresql.PgConnectionImpl
import com.github.clasicrando.postgresql.message.PgMessage
import com.ongres.scram.client.ScramClient
import com.ongres.scram.client.ScramSession
import com.ongres.scram.common.stringprep.StringPreparations

private suspend fun sendScramInit(
    connection: PgConnectionImpl,
    authMechanisms: Array<String>,
): ScramSession {
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
    connection.logger.info("Sending initial SASL Response")
    connection.writeToStream(initialResponse)
    return session
}

private suspend fun receiveContinueMessage(connection: PgConnectionImpl): Authentication.SaslContinue? {
    val continueMessage = connection.receiveServerMessage()
    if (continueMessage !is PgMessage.Authentication) {
        connection.logger.error(
            "Expected an Authentication message but got {code}",
            continueMessage.code,
        )
        return null
    }
    val continueAuthMessage = continueMessage.authentication
    if (continueAuthMessage !is Authentication.SaslContinue) {
        connection.logger.error(
            "Expected a SASL Continue message but got {authMessage}",
            continueAuthMessage,
        )
        return null
    }
    connection.logger.info("Received SASL Continue message")
    return continueAuthMessage
}

private suspend fun sendClientFinalMessage(
    connection: PgConnectionImpl,
    continueAuthMessage: Authentication.SaslContinue,
    session: ScramSession,
): ScramSession.ClientFinalProcessor {
    val password = connection.configuration.password ?: error("Missing Password")
    val serverFirstProcessor = session.receiveServerFirstMessage(continueAuthMessage.saslData)
    val clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password)
    val responseMessage = PgMessage.SaslResponse(clientFinalProcessor.clientFinalMessage())
    connection.logger.info("Sending SASL Response")
    connection.writeToStream(responseMessage)
    return clientFinalProcessor
}

private suspend fun receiveFinalAuthMessage(connection: PgConnectionImpl): Authentication.SaslFinal? {
    val finalMessage = connection.receiveServerMessage()
    if (finalMessage !is PgMessage.Authentication) {
        connection.logger.error(
            "Expected an Authentication message but got {code}",
            finalMessage.code,
        )
        return null
    }
    val finalAuthMessage = finalMessage.authentication
    if (finalAuthMessage !is Authentication.SaslFinal) {
        connection.logger.error(
            "Expected an OK auth message but got {authMessage}",
            finalAuthMessage,
        )
        return null
    }
    return finalAuthMessage
}

private suspend fun receiveOkAuthMessage(connection: PgConnectionImpl): Boolean {
    val okMessage = connection.receiveServerMessage()
    if (okMessage !is PgMessage.Authentication) {
        connection.logger.error(
            "Expected an Authentication message but got {code}",
            okMessage.code,
        )
        return false
    }
    val okAuthMessage = okMessage.authentication
    if (okAuthMessage !is Authentication.Ok) {
        connection.logger.error(
            "Expected an OK auth message but got {authMessage}",
            okAuthMessage,
        )
        return false
    }
    return true
}

suspend fun saslAuthFlow(connection: PgConnectionImpl, auth: Authentication.Sasl): Boolean {
    connection.logger.info("Starting Scram Client")
    val session = sendScramInit(connection, auth.authMechanisms.toTypedArray())

    val continueAuthMessage = receiveContinueMessage(connection) ?: return false
    connection.logger.info("Received SASL Continue message")

    val clientFinalProcessor = sendClientFinalMessage(connection, continueAuthMessage, session)

    val finalAuthMessage = receiveFinalAuthMessage(connection) ?: return false

    clientFinalProcessor.receiveServerFinalMessage(finalAuthMessage.saslData)

    return receiveOkAuthMessage(connection)
}
