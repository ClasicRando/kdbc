package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.stream.RawMessage
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/** Common entry point for decoding backend [PgMessage]s */
internal object PgMessageDecoders {
    /**
     * Decode the [RawMessage] into a [PgMessage] by looking up the appropriate
     * [io.github.clasicrando.kdbc.core.message.MessageDecoder] and calling
     * [io.github.clasicrando.kdbc.core.message.MessageDecoder.decode]. In the case that the
     * [RawMessage.format] does not match a known format code, [PgMessage.UnknownMessage] will be
     * returned and subsequent methods processing messages should ignore the message.
     */
    fun decode(rawMessage: RawMessage): PgMessage {
        val contents = rawMessage.contents
        return when (rawMessage.format) {
            PgMessage.AUTHENTICATION_CODE -> AuthenticationMessageDecoder.decode(contents)
            PgMessage.ERROR_RESPONSE_CODE -> ErrorResponseDecoder.decode(contents)
            PgMessage.BACKEND_KEY_DATA_CODE -> BackendKeyDataDecoder.decode(contents)
            PgMessage.PARAMETER_STATUS_CODE -> ParameterStatusDecoder.decode(contents)
            PgMessage.READY_FOR_QUERY_CODE -> ReadyForQueryDecoder.decode(contents)
            PgMessage.NOTICE_RESPONSE_CODE -> NoticeResponseDecoder.decode(contents)
            PgMessage.DATA_ROW_CODE -> DataRowDecoder.decode(contents)
            PgMessage.COMMAND_COMPLETE_CODE -> CommandCompleteDecoder.decode(contents)
            PgMessage.ROW_DESCRIPTION_CODE -> RowDescriptionDecoder.decode(contents)
            PgMessage.PARSE_COMPLETE_CODE -> PgMessage.ParseComplete
            PgMessage.BIND_COMPLETE_CODE -> PgMessage.BindComplete
            PgMessage.CLOSE_COMPLETE_CODE -> PgMessage.CloseComplete
            PgMessage.COPY_IN_RESPONSE_CODE -> CopyInResponseDecoder.decode(contents)
            PgMessage.COPY_OUT_RESPONSE_CODE -> CopyOutResponseDecoder.decode(contents)
            PgMessage.COPY_DATA_CODE -> CopyDataDecoder.decode(contents)
            PgMessage.COPY_DONE_CODE -> PgMessage.CopyDone
            PgMessage.NOTIFICATION_RESPONSE_CODE -> NotificationResponseDecoder.decode(contents)
            PgMessage.PARAMETER_DESCRIPTION_CODE -> ParameterDescriptionDecoder.decode(contents)
            PgMessage.NEGOTIATE_PROTOCOL_VERSION_CODE -> NegotiateProtocolVersionDecoder.decode(contents)
            else -> {
                logger.atTrace {
                    message = "Received unexpected message of format = '${rawMessage.format}'"
                }
                rawMessage.contents.close()
                PgMessage.UnknownMessage
            }
        }
    }
}