package io.github.clasicrando.kdbc.mysql.message

import io.github.clasicrando.kdbc.core.connection.IntBitFlags
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import io.github.clasicrando.kdbc.mysql.statement.MySqlArguments
import io.github.clasicrando.kdbc.mysql.type.MySqlType
import kotlinx.io.Buffer

/**
 * Various server/client messages sent within the MySQL C/S protocol. Not all messages are present
 * because we don't use them during driver operation.
 */
internal sealed interface MysqlMessage {
    class Handshake(
        val protocolVersion: Byte,
        val serverVersion: String,
        val connectionId: Int,
        val serverCapabilities: LongBitFlags,
        val serverDefaultCollation: Byte,
        val status: IntBitFlags,
        val authPlugin: AuthPlugin?,
        val authPluginData: ByteArray,
    ) : MysqlMessage

    class HandshakeResponse(
        val database: String?,
        val maxPacketSize: Int,
        val characterSet: Byte,
        val username: String,
        val authPlugin: AuthPlugin?,
        val authResponse: ByteArray?,
        val sessionProperties: Map<String, String>,
    ) : MysqlMessage

    class SslRequest(val maxPacketSize: Int, val characterSet: Byte) : MysqlMessage

    class AuthSwitchRequest(val plugin: AuthPlugin, val data: ByteArray) : MysqlMessage

    class AuthSwitchResponse(val bytes: ByteArray) : MysqlMessage

    class Eof(val warning: Int, val status: IntBitFlags) : MysqlMessage

    class Err(val errorCode: Int, val sqlState: String?, val errorMessage: String) : MysqlMessage {
        override fun toString(): String {
            return "MySqlMessage.Err(errorCode=$errorCode, sqlState=$sqlState, errorMessage='$errorMessage')"
        }
    }

    class Ok(
        val affectedRows: Long,
        val lastInsertId: Long,
        val status: IntBitFlags,
        val warnings: Int,
    ) : MysqlMessage

    class Execute(val statement: Int, val arguments: MySqlArguments) : MysqlMessage

    class Prepare(val query: String) : MysqlMessage

    class PrepareOk(val statementId: Int, val columns: Int, val params: Int, val warnings: Int) :
        MysqlMessage

    class BinaryRow(val row: Array<MySqlValue?>) : MysqlMessage

    class StatementClose(val statementId: Int) : MysqlMessage

    class ColumnDefinition(
        val catalog: ByteArray,
        val schema: ByteArray,
        val tableAlias: ByteArray,
        val table: ByteArray,
        val alias: String,
        val name: String,
        val collation: Int,
        val maxSize: Int,
        val type: MySqlType,
        val flags: IntBitFlags,
        val decimals: Byte,
    ) : MysqlMessage {
        val columnName: String = alias.takeIf { it.isNotEmpty() } ?: name
    }

    data object Ping : MysqlMessage

    data class Query(val sql: String) : MysqlMessage

    data object Quit : MysqlMessage

    class TextRow(val row: Array<MySqlValue?>) : MysqlMessage

    class SingleByte(val byte: Byte) : MysqlMessage

    class MultiByte(val bytes: ByteArray) : MysqlMessage

    data object ResetSession : MysqlMessage

    class LoadLocal(val source: Buffer) : MysqlMessage

    data object Empty : MysqlMessage
}
