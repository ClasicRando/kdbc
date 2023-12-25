package com.github.clasicrando.postgresql.message

import com.github.clasicrando.postgresql.copy.CopyFormat
import com.github.clasicrando.postgresql.row.PgRowFieldDescription

sealed class PgMessage(val code: Byte) {

    data class Authentication(
        val authentication: com.github.clasicrando.postgresql.authentication.Authentication,
    ) : PgMessage(AUTHENTICATION_CODE) // B
    data class BackendKeyData(
        val processId: Int,
        val secretKey: Int,
    ) : PgMessage(BACKEND_KEY_DATA_CODE) // B
    data class Bind(
        val portal: String,
        val statementName: String,
        val parameters: List<Any?>,
    ) : PgMessage(BIND_CODE) // F
    data object BindComplete : PgMessage(BIND_COMPLETE_CODE) // B
    data class CancelRequest(val processId: Int, val secretKey: Int) : PgMessage(ZERO_CODE) // F
    data class Close(val target: CloseTarget, val targetName: String) : PgMessage(CLOSE_CODE) // F
    data object CloseComplete : PgMessage(CLOSE_COMPLETE_CODE) // B
    data class CommandComplete(
        val rowCount: Long,
        val message: String,
    ) : PgMessage(COMMAND_COMPLETE_CODE) // B
    class CopyData(val data: ByteArray) : PgMessage(COPY_DATA_CODE) // F & B
    data object CopyDone : PgMessage(COPY_DONE) // F & B
    data class CopyFail(val message: String) : PgMessage(COPY_FAIL) // F
    class CopyInResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: Array<CopyFormat>,
    ) : PgMessage(COPY_IN_RESPONSE_CODE) // B
    class CopyOutResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: Array<CopyFormat>,
    ) : PgMessage(COPY_OUT_RESPONSE_CODE) // B
    class CopyBothResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: Array<CopyFormat>,
    ) : PgMessage(COPY_BOTH_RESPONSE_CODE) // B
    class DataRow(val values: Array<ByteArray?>) : PgMessage(DATA_ROW_CODE) // B
    data class Describe(val target: DescribeTarget, val name: String) : PgMessage(DESCRIBE_CODE) // F
    data object EmptyQueryResponse : PgMessage(EMPTY_QUERY_RESPONSE) // B
    data class ErrorResponse(val fields: Map<Char, String>) : PgMessage(ERROR_RESPONSE_CODE) // B
    data class Execute(val portalName: String, val maxRowCount: Int) : PgMessage(EXECUTE_CODE) // F
    data object Flush : PgMessage(FLUSH_CODE) // F
    data class FunctionCall(
        val functionOid: Int,
        val arguments: List<Any?>,
    ) : PgMessage(FUNCTION_CALL_CODE) // F
    class FunctionCallResponse(val result: ByteArray?) : PgMessage(FUNCTION_CALL_RESPONSE_CODE) // B
//    data object GssEncRequest : PgMessage(ZERO_CODE) // F
//    class GssResponse(val data: ByteArray) : PgMessage(GSS_RESPONSE_CODE) // F
    class NegotiateProtocolVersion(
        val minProtocolVersion: Int,
        val protocolVersionsNotRecognized: Array<String>,
    ) : PgMessage(NEGOTIATE_PROTOCOL_VERSION_CODE) // B
    data object NoData : PgMessage(NO_DATA_CODE) // B
    data class NoticeResponse(val fields: Map<Char, String>) : PgMessage(NOTICE_RESPONSE_CODE) // B
    data class NotificationResponse(
        val processId: Int,
        val channelName: String,
        val payload: String,
    ) : PgMessage(NOTIFICATION_RESPONSE_CODE) // B
    data class ParameterDescription(
        val parameterCount: Short,
        val parameterDataTypes: List<Int>,
    ) : PgMessage(PARAMETER_DESCRIPTION_CODE) // B
    data class ParameterStatus(
        val name: String,
        val value: String,
    ) : PgMessage(PARAMETER_STATUS_CODE) // B
    data class Parse(
        val preparedStatementName: String,
        val query: String,
        val parameters: List<Any?>,
    ) : PgMessage(PARSE_CODE) // F
    data object ParseComplete : PgMessage(PARSE_COMPLETE_CODE) // B
    class PasswordMessage(val password: ByteArray) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    data object PortalSuspended : PgMessage(PORTAL_SUSPENDED_CODE) // B
    data class Query(val query: String) : PgMessage(QUERY_CODE) // F
    data class ReadyForQuery(
        val transactionStatus: TransactionStatus,
    ) : PgMessage(READY_FOR_QUERY_CODE) // B
    data class RowDescription(
        val fields: List<PgRowFieldDescription>,
    ) : PgMessage(ROW_DESCRIPTION_CODE) // B
    data class SaslInitialResponse(
        val mechanism: String,
        val saslData: String,
    ) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    data class SaslResponse(val saslData: String) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    data object SslRequest : PgMessage(ZERO_CODE) // F
    data class StartupMessage(val params: List<Pair<String, String>>) : PgMessage(ZERO_CODE) // F
    data object Sync : PgMessage(SYNC_CODE) // F
    data object Terminate : PgMessage(TERMINATE_CODE) // F

    companion object {
        const val ZERO_CODE: Byte = 0
        const val AUTHENTICATION_CODE = 'R'.code.toByte()
        const val BACKEND_KEY_DATA_CODE = 'K'.code.toByte()
        const val BIND_CODE = 'B'.code.toByte()
        const val BIND_COMPLETE_CODE = '2'.code.toByte()
        const val CLOSE_CODE = 'C'.code.toByte()
        const val CLOSE_COMPLETE_CODE = '3'.code.toByte()
        const val COMMAND_COMPLETE_CODE = 'C'.code.toByte()
        const val COPY_DATA_CODE = 'd'.code.toByte()
        const val COPY_DONE = 'c'.code.toByte()
        const val COPY_FAIL = 'f'.code.toByte()
        const val COPY_IN_RESPONSE_CODE = 'G'.code.toByte()
        const val COPY_OUT_RESPONSE_CODE = 'H'.code.toByte()
        const val COPY_BOTH_RESPONSE_CODE = 'W'.code.toByte()
        const val DATA_ROW_CODE = 'D'.code.toByte()
        const val DESCRIBE_CODE = 'D'.code.toByte()
        const val EMPTY_QUERY_RESPONSE = 'I'.code.toByte()
        const val ERROR_RESPONSE_CODE = 'E'.code.toByte()
        const val EXECUTE_CODE = 'E'.code.toByte()
        const val FLUSH_CODE = 'H'.code.toByte()
        const val FUNCTION_CALL_CODE = 'F'.code.toByte()
        const val FUNCTION_CALL_RESPONSE_CODE = 'V'.code.toByte()
//        const val GSS_RESPONSE_CODE = 'p'.code.toByte()
        const val NEGOTIATE_PROTOCOL_VERSION_CODE = 'v'.code.toByte()
        const val NO_DATA_CODE = 'n'.code.toByte()
        const val NOTICE_RESPONSE_CODE = 'N'.code.toByte()
        const val NOTIFICATION_RESPONSE_CODE = 'A'.code.toByte()
        const val PARAMETER_DESCRIPTION_CODE = 't'.code.toByte()
        const val PARAMETER_STATUS_CODE = 'S'.code.toByte()
        const val PARSE_CODE = 'P'.code.toByte()
        const val PARSE_COMPLETE_CODE = '1'.code.toByte()
        const val PASSWORD_MESSAGE_CODE = 'p'.code.toByte()
        const val PORTAL_SUSPENDED_CODE = 's'.code.toByte()
        const val QUERY_CODE = 'Q'.code.toByte()
        const val READY_FOR_QUERY_CODE = 'Z'.code.toByte()
        const val ROW_DESCRIPTION_CODE = 'T'.code.toByte()
        const val SYNC_CODE = 'S'.code.toByte()
        const val TERMINATE_CODE = 'X'.code.toByte()
    }
}
