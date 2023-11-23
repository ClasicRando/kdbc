package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.stream.RawMessage
import io.klogging.NoCoLogging
import io.ktor.utils.io.charsets.Charset

class MessageDecoders(charset: Charset) : NoCoLogging {
    private val authenticationMessageDecoder = AuthenticationMessageDecoder(charset)
    private val errorResponseDecoder = ErrorResponseDecoder(charset)
    private val noticeResponseDecoder = NoticeResponseDecoder(charset)
    private val parameterStatusDecoder = ParameterStatusDecoder(charset)
    private val commandCompleteDecoder = CommandCompleteDecoder(charset)
    private val rowDescriptionDecoder = RowDescriptionDecoder(charset)

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
            else -> {
                logger.info("Received message ${rawMessage.format}")
                PgMessage.NoData
//                error("Cannot parse message ${rawMessage.format}")
            }
        }
    }
}