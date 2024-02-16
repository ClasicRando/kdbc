package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.stream.RawMessage
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

internal class PgMessageDecoders {
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
            PgMessage.COPY_DONE -> PgMessage.CopyDone
            PgMessage.NOTIFICATION_RESPONSE_CODE -> NotificationResponseDecoder.decode(contents)
            PgMessage.PARAMETER_DESCRIPTION_CODE -> ParameterDescriptionDecoder.decode(contents)
            else -> {
                logger.atTrace {
                    message = "Received message {format}"
                    payload = mapOf("format" to rawMessage.format)
                }
                rawMessage.contents.release()
                PgMessage.NoData
            }
        }
    }
}