package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.stream.RawMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.utils.io.charsets.Charset

private val logger = KotlinLogging.logger {}

internal class PgMessageDecoders(charset: Charset) {
    private val authenticationMessageDecoder = AuthenticationMessageDecoder(charset)
    private val errorResponseDecoder = ErrorResponseDecoder(charset)
    private val noticeResponseDecoder = NoticeResponseDecoder(charset)
    private val parameterStatusDecoder = ParameterStatusDecoder(charset)
    private val commandCompleteDecoder = CommandCompleteDecoder(charset)
    private val rowDescriptionDecoder = RowDescriptionDecoder(charset)
    private val notificationResponseDecoder = NotificationResponseDecoder(charset)

    fun decode(rawMessage: RawMessage): PgMessage {
        val contents = rawMessage.contents
        return when (rawMessage.format) {
            PgMessage.AUTHENTICATION_CODE -> authenticationMessageDecoder.decode(contents)
            PgMessage.ERROR_RESPONSE_CODE -> errorResponseDecoder.decode(contents)
            PgMessage.BACKEND_KEY_DATA_CODE -> BackendKeyDataDecoder.decode(contents)
            PgMessage.PARAMETER_STATUS_CODE -> parameterStatusDecoder.decode(contents)
            PgMessage.READY_FOR_QUERY_CODE -> ReadyForQueryDecoder.decode(contents)
            PgMessage.NOTICE_RESPONSE_CODE -> noticeResponseDecoder.decode(contents)
            PgMessage.DATA_ROW_CODE -> DataRowDecoder.decode(contents)
            PgMessage.COMMAND_COMPLETE_CODE -> commandCompleteDecoder.decode(contents)
            PgMessage.ROW_DESCRIPTION_CODE -> rowDescriptionDecoder.decode(contents)
            PgMessage.PARSE_COMPLETE_CODE -> PgMessage.ParseComplete
            PgMessage.BIND_COMPLETE_CODE -> PgMessage.BindComplete
            PgMessage.CLOSE_COMPLETE_CODE -> PgMessage.CloseComplete
            PgMessage.COPY_IN_RESPONSE_CODE -> CopyInResponseDecoder.decode(contents)
            PgMessage.COPY_OUT_RESPONSE_CODE -> CopyOutResponseDecoder.decode(contents)
            PgMessage.COPY_DATA_CODE -> CopyDataDecoder.decode(contents)
            PgMessage.COPY_DONE -> PgMessage.CopyDone
            PgMessage.NOTIFICATION_RESPONSE_CODE -> notificationResponseDecoder.decode(contents)
            else -> {
                logger.atTrace {
                    message = "Received message {format}"
                    payload = mapOf("format" to rawMessage.format)
                }
                PgMessage.NoData
            }
        }
    }
}