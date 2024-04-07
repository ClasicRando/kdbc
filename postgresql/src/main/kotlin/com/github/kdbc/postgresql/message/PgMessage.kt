package com.github.kdbc.postgresql.message

import com.github.kdbc.core.message.SizedMessage
import com.github.kdbc.postgresql.column.PgColumnDescription
import com.github.kdbc.postgresql.copy.CopyFormat
import com.github.kdbc.postgresql.message.information.InformationResponse
import com.github.kdbc.postgresql.result.PgRowBuffer
import com.github.kdbc.postgresql.statement.PgArguments

/**
 * Specified frontend and backend messages that can be sent to and received from the database
 * server. Each message variant only stores the message related contents and the header [Byte] that
 * is related to that message. Header bytes can be reused if the server knows how to distinguish
 * between message contents or the 2 messages that share a header byte are a frontend and backend
 * message so there would be no conflict. For a more thorough definition of each message, see the
 * [message docs](https://www.postgresql.org/docs/current/protocol-message-formats.html) and
 * [message flow docs](https://www.postgresql.org/docs/current/protocol-flow.html)
 */
@Suppress("unused")
internal sealed class PgMessage(val code: Byte) {
    /**
     * Backend message sent with the [AUTHENTICATION_CODE] header [Byte]. Contains the
     * [authentication] data that needs to be handled.
     */
    data class Authentication(
        val authentication: com.github.kdbc.postgresql.authentication.Authentication,
    ) : PgMessage(AUTHENTICATION_CODE) // B
    /**
     * Backend message sent with the [BACKEND_KEY_DATA_CODE] header [Byte]. Contains the
     * [processId] and [secretKey] that a client must use to cancel a request in progress.
     */
    data class BackendKeyData(
        val processId: Int,
        val secretKey: Int,
    ) : PgMessage(BACKEND_KEY_DATA_CODE) // B
    /**
     * Frontend message sent with the [BIND_CODE] header [Byte]. Supplies the optional [portal]
     * name (if null or empty, the unnamed portal is used), the [statementName] and a buffer
     * containing the [PgArguments] to be bound to the portal.
     */
    data class Bind(
        val portal: String?,
        val statementName: String,
        val parameters: PgArguments,
    ) : PgMessage(BIND_CODE) // F
    /**
     * Backend message sent with the [BIND_COMPLETE_CODE] header [Byte]. Contains no data, only
     * signifying that the previous [Bind] request by the frontend has been completed.
     */
    data object BindComplete : PgMessage(BIND_COMPLETE_CODE) // B
    /**
     * Frontend message sent with the no header [Byte]. Contains the [processId] and [secretKey]
     * sent from the backend during connection established.
     *
     * @see BackendKeyData
     */
    data class CancelRequest(val processId: Int, val secretKey: Int) : PgMessage(ZERO_CODE) // F
    /**
     * Frontend message sent with the [CLOSE_CODE] header [Byte]. Contains the [target] type and
     * the [targetName] (if null or empty, the unnamed statement or portal is closed).
     */
    data class Close(val target: MessageTarget, val targetName: String?) : PgMessage(CLOSE_CODE) // F
    /**
     * Backend message sent with the [CLOSE_COMPLETE_CODE] header [Byte]. Contains no data, only
     * signifying that the previous [Close] request by the frontend has been completed.
     */
    data object CloseComplete : PgMessage(CLOSE_COMPLETE_CODE) // B
    /**
     * Backend message sent with the [COMMAND_COMPLETE_CODE] header [Byte]. Contains the number of
     * rows impacted as [rowCount] and a [message] which depends on the type of command sent.
     *
     * [see](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE)
     */
    data class CommandComplete(
        val rowCount: Long,
        val message: String,
    ) : PgMessage(COMMAND_COMPLETE_CODE) // B
    /**
     * Backend and frontend message sent with the [COPY_DATA_CODE] header [Byte]. Contains the copy
     * [data] as a [ByteArray]. When this message is sent from the backend, the [data] represents
     * a single row. WHen this message is sent from the frontend, the [data] could represent any
     * chunk of the total data copied since the backend parsing the data as it comes in, byte by
     * byte.
     *
     * To optimize the sending of `COPY FROM` data, this messages is marked as [SizedMessage] to
     * allow for packing as many messages into a buffer before flushing the messages to the
     * backend. This is to avoid complex chunking logic of the data users might provide to the copy
     * method and copying data only once (into the buffer).
     */
    class CopyData(val data: ByteArray) : PgMessage(COPY_DATA_CODE), SizedMessage { // F & B
        override val size: Int = 5 + data.size
    }
    /**
     * Backend and frontend message sent with the [COPY_DONE_CODE] header [Byte]. Contains no data,
     * only signifying that the copy operation is done. The frontend sends this message when the
     * `COPY FROM` operation is complete. The backend sends this message when the `COPY TO`
     * operation is done and the client should stop processing [CopyData] messages from the backend.
     */
    data object CopyDone : PgMessage(COPY_DONE_CODE) // F & B
    /**
     * Frontend message sent with the [COPY_FAIL_CODE] header [Byte]. Contains the [message]
     * explaining why the `COPY FROM` operation was aborted.
     */
    data class CopyFail(val message: String) : PgMessage(COPY_FAIL_CODE) // F
    /**
     * Backend message sent with the [COPY_IN_RESPONSE_CODE] header [Byte]. Contains the details
     * of the `COPY FROM` operation. Currently, none of the fields are used since they only echo
     * sent data from the client through the `COPY` query.
     */
    class CopyInResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: List<CopyFormat>,
    ) : PgMessage(COPY_IN_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [COPY_OUT_RESPONSE_CODE] header [Byte]. Contains the details
     * of the `COPY TO` operation. Currently, none of the fields are used since they only echo sent
     * data from the client through the `COPY` query.
     */
    data class CopyOutResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: List<CopyFormat>,
    ) : PgMessage(COPY_OUT_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [COPY_BOTH_RESPONSE_CODE] header [Byte]. Contains the details
     * of the copy both operation. Currently, this message is not referenced since it's only used
     * for logical replication which is not supported.
     */
    data class CopyBothResponse(
        val copyFormat: CopyFormat,
        val columnCount: Int,
        val columnFormats: List<CopyFormat>,
    ) : PgMessage(COPY_BOTH_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [DATA_ROW_CODE] header [Byte]. Contains the row data of a
     * single query result in [rowBuffer].
     */
    data class DataRow(val rowBuffer: PgRowBuffer) : PgMessage(DATA_ROW_CODE) // B
    /**
     * Frontend message sent with the [DESCRIBE_CODE] header [Byte]. Contains the [target] of the
     * describe command and the name of the target to be described.
     */
    data class Describe(val target: MessageTarget, val name: String) : PgMessage(DESCRIBE_CODE) // F
    /**
     * Backend message sent with the [EMPTY_QUERY_RESPONSE_CODE] header [Byte]. Contains no data,
     * only signifying that a query sent is empty. This will never be encountered since empty
     * querying are caught in the client before sending to the server.
     */
    data object EmptyQueryResponse : PgMessage(EMPTY_QUERY_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [ERROR_RESPONSE_CODE] header [Byte]. Contains the
     * [informationResponse] data sent.
     *
     * @see [InformationResponse]
     */
    data class ErrorResponse(val informationResponse: InformationResponse)
        : PgMessage(ERROR_RESPONSE_CODE) // B
    /**
     * Frontend message sent with the [EXECUTE_CODE] header [Byte]. Contains the [portalName] to
     * execute (if null or empty, the unnamed portal is executed) and the [maxRowCount] of the
     * query result.
     */
    data class Execute(val portalName: String?, val maxRowCount: Int) : PgMessage(EXECUTE_CODE) // F
    /**
     * Frontend message sent with the [FLUSH_CODE] header [Byte]. Contains no data, only requesting
     * that the backend to send all pending messages in it's output buffer to the client. This
     * message is currently not used but exists in the message protocol.
     */
    data object Flush : PgMessage(FLUSH_CODE) // F
    /**
     * Frontend message sent with the [FUNCTION_CALL_CODE] header [Byte]. Contains the
     * [functionOid] (the OID of the function object) to be called and the [arguments] that will
     * be passed to the function. This message is currently not used but exists in the message
     * protocol.
     *
     * [docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-FUNCTION-CALL)
     */
    data class FunctionCall(
        val functionOid: Int,
        val arguments: List<Any?>,
    ) : PgMessage(FUNCTION_CALL_CODE) // F
    /**
     * Backend message sent with the [FUNCTION_CALL_RESPONSE_CODE] header [Byte]. Contains the
     * [result] value from the function call as a [ByteArray]. This message is currently not used
     * but exists in the message protocol.
     *
     * [docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-FUNCTION-CALL)
     */
    class FunctionCallResponse(val result: ByteArray?) : PgMessage(FUNCTION_CALL_RESPONSE_CODE) // B
//    data object GssEncRequest : PgMessage(ZERO_CODE) // F
//    class GssResponse(val data: ByteArray) : PgMessage(GSS_RESPONSE_CODE) // F
    /**
     * Backend message sent with the [NEGOTIATE_PROTOCOL_VERSION_CODE] header [Byte]. Contains the
     * [minProtocolVersion] and collection of [protocolOptionsNotRecognized].
     */
    data class NegotiateProtocolVersion(
        val minProtocolVersion: Int,
        val protocolOptionsNotRecognized: List<String>,
    ) : PgMessage(NEGOTIATE_PROTOCOL_VERSION_CODE) // B
    /**
     * Backend message sent with the [NO_DATA_CODE] header [Byte]. Contains no data, only
     * signifying the targeted portal or statement returns no data.
     */
    data object NoData : PgMessage(NO_DATA_CODE) // B
    /**
     * Backend message sent with the [NOTICE_RESPONSE_CODE] header [Byte]. Contains the
     * [informationResponse] data sent.
     *
     * @see [InformationResponse]
     */
    data class NoticeResponse(val informationResponse: InformationResponse) : PgMessage(NOTICE_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [NOTIFICATION_RESPONSE_CODE] header [Byte]. Contains the
     * [processId] of the backend that sent the notification, the [channelName] associated with the
     * notification and the option [payload] of the notification.
     */
    data class NotificationResponse(
        val processId: Int,
        val channelName: String,
        val payload: String,
    ) : PgMessage(NOTIFICATION_RESPONSE_CODE) // B
    /**
     * Backend message sent with the [PARAMETER_DESCRIPTION_CODE] header [Byte]. Contains the
     * [parameterDataTypes] of the described statement.
     */
    data class ParameterDescription(
        val parameterDataTypes: List<Int>,
    ) : PgMessage(PARAMETER_DESCRIPTION_CODE) // B
    /**
     * Backend message sent with the [PARAMETER_STATUS_CODE] header [Byte]. Contains the [name] and
     * [value] of a backend parameter.
     */
    data class ParameterStatus(
        val name: String,
        val value: String,
    ) : PgMessage(PARAMETER_STATUS_CODE) // B
    /**
     * Frontend message sent with the [PARSE_CODE] header [Byte]. Contains the
     * [preparedStatementName] (used to reference a prepared statement again), the [query] to be
     * parsed by the backend and the [parameterTypes] as a [List] of data type OIDs.
     */
    data class Parse(
        val preparedStatementName: String,
        val query: String,
        val parameterTypes: List<Int>,
    ) : PgMessage(PARSE_CODE) // F
    /**
     * Backend message sent with the [PARSE_COMPLETE_CODE] header [Byte]. Contains no data, only
     * signifying that the previous [Parse] request has completed.
     */
    data object ParseComplete : PgMessage(PARSE_COMPLETE_CODE) // B
    /**
     * Frontend message sent with the [PASSWORD_MESSAGE_CODE] header [Byte]. Contains the
     * [password] as a [ByteArray] (encrypted if required by the server).
     */
    class PasswordMessage(val password: ByteArray) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    /**
     * Backend message sent with the [PORTAL_SUSPENDED_CODE] header [Byte]. Contains no data, only
     * signifying that the portal [Execute] request has completed with the max number of rows
     * met before returning all rows that could be fetched. This message is currently not used
     * since this client never caps portal [Execute] requests.
     */
    data object PortalSuspended : PgMessage(PORTAL_SUSPENDED_CODE) // B
    /**
     * Frontend message sent with the [QUERY_CODE] header [Byte]. Contains the [query] as a
     * [String] to be executed by the server.
     */
    data class Query(val query: String) : PgMessage(QUERY_CODE) // F
    /**
     * Backend message sent with the [READY_FOR_QUERY_CODE] header [Byte]. Contains the
     * [transactionStatus] code telling the client the status of the current transaction block.
     */
    data class ReadyForQuery(
        val transactionStatus: TransactionStatus,
    ) : PgMessage(READY_FOR_QUERY_CODE) // B
    /**
     * Backend message sent with the [ROW_DESCRIPTION_CODE] header [Byte]. Contains the [fields]
     * of the query result rows as a [List] of [PgColumnDescription].
     */
    data class RowDescription(
        val fields: List<PgColumnDescription>,
    ) : PgMessage(ROW_DESCRIPTION_CODE) // B
    /**
     * Frontend message sent with the [PASSWORD_MESSAGE_CODE] header [Byte]. Contains the SASL
     * [mechanism] selected by the client and the [saslData] required by the server to continue the
     * authentication.
     */
    data class SaslInitialResponse(
        val mechanism: String,
        val saslData: String,
    ) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    /**
     * Frontend message sent with the [PASSWORD_MESSAGE_CODE] header [Byte]. Contains the
     * [saslData] required to continue the authentication flow.
     */
    data class SaslResponse(val saslData: String) : PgMessage(PASSWORD_MESSAGE_CODE) // F
    /**
     * Frontend message sent with the no header [Byte]. Contains no data, only signifying that the
     * client wants to initiate the connection with SSL encryption. This is sent before a
     * [StartupMessage] if specified.
     */
    data object SslRequest : PgMessage(ZERO_CODE) // F
    /**
     * Frontend message sent with the no header [Byte]. Contains the [params] the client wants to
     * specify as part of the connection state.
     */
    data class StartupMessage(val params: List<Pair<String, String>>) : PgMessage(ZERO_CODE) // F
    /**
     * Frontend message sent with the [SYNC_CODE] header [Byte]. Contains no data, only signifying
     * that the client want to close the current transaction (if any) after issuing 1 or more
     * extended query requests.
     */
    data object Sync : PgMessage(SYNC_CODE) // F
    /**
     * Frontend message sent with the [TERMINATE_CODE] header [Byte]. Contains no data, only
     * signifying that the client is about to close the TCP connection.
     */
    data object Terminate : PgMessage(TERMINATE_CODE) // F

    /**
     * Special [PgMessage] returned from the message decoders when a decoder doesn't exist. That
     * means this library is not aware of a message type or the message type is being ignored.
     */
    data object UnknownMessage : PgMessage(UNKNOWN)

    companion object {
        const val UNKNOWN: Byte = -1
        const val ZERO_CODE: Byte = 0
        const val AUTHENTICATION_CODE = 'R'.code.toByte()
        const val BACKEND_KEY_DATA_CODE = 'K'.code.toByte()
        const val BIND_CODE = 'B'.code.toByte()
        const val BIND_COMPLETE_CODE = '2'.code.toByte()
        const val CLOSE_CODE = 'C'.code.toByte()
        const val CLOSE_COMPLETE_CODE = '3'.code.toByte()
        const val COMMAND_COMPLETE_CODE = 'C'.code.toByte()
        const val COPY_DATA_CODE = 'd'.code.toByte()
        const val COPY_DONE_CODE = 'c'.code.toByte()
        const val COPY_FAIL_CODE = 'f'.code.toByte()
        const val COPY_IN_RESPONSE_CODE = 'G'.code.toByte()
        const val COPY_OUT_RESPONSE_CODE = 'H'.code.toByte()
        const val COPY_BOTH_RESPONSE_CODE = 'W'.code.toByte()
        const val DATA_ROW_CODE = 'D'.code.toByte()
        const val DESCRIBE_CODE = 'D'.code.toByte()
        const val EMPTY_QUERY_RESPONSE_CODE = 'I'.code.toByte()
        const val ERROR_RESPONSE_CODE = 'E'.code.toByte()
        const val EXECUTE_CODE = 'E'.code.toByte()
        const val FLUSH_CODE = 'H'.code.toByte()
        const val FUNCTION_CALL_CODE = 'F'.code.toByte()
        const val FUNCTION_CALL_RESPONSE_CODE = 'V'.code.toByte()
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
