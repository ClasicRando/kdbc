package io.github.clasicrando.kdbc.mysql.message

import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import io.github.clasicrando.kdbc.mysql.result.MySqlDataRow
import io.github.clasicrando.kdbc.mysql.statement.MySqlArguments
import io.github.clasicrando.kdbc.mysql.type.MySqlType

internal sealed interface MysqlMessage {
    data class Handshake(
        val protocolVersion: Byte,
        val serverVersion: String,
        val connectionId: Int,
        val serverCapabilities: Capabilities,
        val serverDefaultCollation: Byte,
        val status: Status,
        val authPlugin: AuthPlugin?,
        val authPluginData: ByteArray,
    ) : MysqlMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Handshake) return false

            if (protocolVersion != other.protocolVersion) return false
            if (serverVersion != other.serverVersion) return false
            if (connectionId != other.connectionId) return false
            if (serverCapabilities != other.serverCapabilities) return false
            if (serverDefaultCollation != other.serverDefaultCollation) return false
            if (status != other.status) return false
            if (authPlugin != other.authPlugin) return false
            if (!authPluginData.contentEquals(other.authPluginData)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = (protocolVersion.toInt() and 0xFF)
            result = 31 * result + serverVersion.hashCode()
            result = 31 * result + connectionId
            result = 31 * result + serverCapabilities.hashCode()
            result = 31 * result + serverDefaultCollation
            result = 31 * result + status.hashCode()
            result = 31 * result + (authPlugin?.hashCode() ?: 0)
            result = 31 * result + authPluginData.contentHashCode()
            return result
        }
    }

    data class HandshakeResponse(
        val database: String?,
        val maxPacketSize: Int,
        val collation: Byte,
        val username: String,
        val authPlugin: AuthPlugin?,
        val authResponse: ByteArray?,
    ) : MysqlMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is HandshakeResponse) return false

            if (database != other.database) return false
            if (maxPacketSize != other.maxPacketSize) return false
            if (collation != other.collation) return false
            if (username != other.username) return false
            if (authPlugin != other.authPlugin) return false
            if (authResponse != null) {
                if (other.authResponse == null) return false
                if (!authResponse.contentEquals(other.authResponse)) return false
            } else if (other.authResponse != null) {
                return false
            }

            return true
        }

        override fun hashCode(): Int {
            var result = database?.hashCode() ?: 0
            result = 31 * result + maxPacketSize
            result = 31 * result + collation
            result = 31 * result + username.hashCode()
            result = 31 * result + (authPlugin?.hashCode() ?: 0)
            result = 31 * result + (authResponse?.contentHashCode() ?: 0)
            return result
        }
    }

    data class SslRequest(
        val maxPacketSize: Int,
        val collation: Byte,
    ) : MysqlMessage

    data class AuthSwitchRequest(
        val plugin: AuthPlugin,
        val data: ByteArray,
    ) : MysqlMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is AuthSwitchRequest) return false

            if (plugin != other.plugin) return false
            if (!data.contentEquals(other.data)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = plugin.hashCode()
            result = 31 * result + data.contentHashCode()
            return result
        }
    }

    data class AuthSwitchResponse(
        val bytes: ByteArray,
    ) : MysqlMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is AuthSwitchResponse) return false

            return bytes.contentEquals(other.bytes)
        }

        override fun hashCode(): Int = bytes.contentHashCode()
    }

    data class Eof(
        val warning: Int,
        val status: Status,
    ) : MysqlMessage

    data class Err(
        val errorCode: Int,
        val sqlState: String?,
        val errorMessage: String,
    ) : MysqlMessage

    data class Ok(
        val affectedRows: Long,
        val lastInsertId: Long,
        val status: Status,
        val warnings: Int,
    ) : MysqlMessage

    data class Execute(
        val statement: Int,
        val arguments: MySqlArguments,
        val typeCache: Unit,
    ) : MysqlMessage

    data class Prepare(
        val query: String,
    ) : MysqlMessage

    data class PrepareOk(
        val statementId: Int,
        val columns: Int,
        val params: Int,
        val warnings: Int,
    ) : MysqlMessage

    data class BinaryRow(
        val row: MySqlDataRow,
    ) : MysqlMessage

    data class StatementClose(
        val statementId: Int,
    ) : MysqlMessage

    data class ColumnDefinition(
        val catalog: ByteArray,
        val schema: ByteArray,
        val tableAlias: ByteArray,
        val table: ByteArray,
        val alias: ByteArray,
        val name: ByteArray,
        val collation: Int,
        val maxSize: Int,
        val type: MySqlType,
        val flags: ColumnFlags,
        val decimals: Byte,
    ) : MysqlMessage {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ColumnDefinition) return false

            if (!catalog.contentEquals(other.catalog)) return false
            if (!schema.contentEquals(other.schema)) return false
            if (!tableAlias.contentEquals(other.tableAlias)) return false
            if (!table.contentEquals(other.table)) return false
            if (!alias.contentEquals(other.alias)) return false
            if (!name.contentEquals(other.name)) return false
            if (collation != other.collation) return false
            if (maxSize != other.maxSize) return false
            if (type != other.type) return false
            if (flags != other.flags) return false
            if (decimals != other.decimals) return false

            return true
        }

        override fun hashCode(): Int {
            var result = catalog.contentHashCode()
            result = 31 * result + schema.contentHashCode()
            result = 31 * result + tableAlias.contentHashCode()
            result = 31 * result + table.contentHashCode()
            result = 31 * result + alias.contentHashCode()
            result = 31 * result + name.contentHashCode()
            result = 31 * result + collation
            result = 31 * result + maxSize
            result = 31 * result + type.hashCode()
            result = 31 * result + flags.hashCode()
            result = 31 * result + decimals
            return result
        }
    }

    data object Ping : MysqlMessage

    data class Query(
        val sql: String,
    ) : MysqlMessage

    data object Quit : MysqlMessage

    data class TextRow(
        val row: MySqlDataRow,
    ) : MysqlMessage
}
