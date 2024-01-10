package com.github.clasicrando.sqlserver.message

import com.github.clasicrando.sqlserver.column.BaseMetadataColumn
import com.github.clasicrando.sqlserver.column.ColumnData
import com.github.clasicrando.sqlserver.column.ColumnMetadata
import com.github.clasicrando.sqlserver.type.Collation

sealed class SqlServerMessage(val type: UInt) {
    data class NewResultSet(val columns: List<ColumnMetadata>) : SqlServerMessage(COLUMN_METADATA)
    data class Done(val inner: DoneMessage) : SqlServerMessage(DONE)
    class Row(val data: Array<ColumnData>) : SqlServerMessage(ROW)
    data class DoneInProcedure(val inner: DoneMessage) : SqlServerMessage(DONE_IN_PROCEDURE)
    data class DoneProcedure(val inner: DoneMessage) : SqlServerMessage(DONE_PROCEDURE)
    data class ReturnStatus(val status: UInt) : SqlServerMessage(RETURN_STATUS)
    data class ReturnValue(
        val paramOrdinal: UShort,
        val paramName: String,
        val udf: Boolean,
        val meta: BaseMetadataColumn,
        val value: ColumnData,
    ) : SqlServerMessage(RETURN_VALUE)
    data class Order(val columnIndexes: List<UShort>) : SqlServerMessage(ORDER)
    sealed class EnvironmentChange : SqlServerMessage(ENVIRONMENT_CHANGE) {
        data class Database(val old: String, val new: String) : EnvironmentChange()
        data class PacketSize(val old: UInt, val new: UInt) : EnvironmentChange()
        data class SqlCollation(val old: Collation?, val new: Collation?) : EnvironmentChange()
        class BeginTransaction(val bytes: ByteArray) : EnvironmentChange()
        data object CommitTransaction : EnvironmentChange()
        data object RollbackTransaction : EnvironmentChange()
        data object DefectTransaction : EnvironmentChange()
        data class Routing(val host: String, val port: UShort) : EnvironmentChange()
        data class ChangeMirror(val value: String) : EnvironmentChange()
        data class Ignored(val envChangeType: EnvironmentChangeType) : EnvironmentChange()
    }
    data class Info(
        val number: UInt,
        val state: UByte,
        val infoClass: UByte,
        val message: String,
        val server: String,
        val procedure: String,
        val line: UInt,
    ) : SqlServerMessage(INFO)
    data class LoginAcknowledge(
        val loginInterface: UByte,
        val tdsVersion: FeatureLevel,
        val programName: String,
        val version: UInt,
    ) : SqlServerMessage(LOGIN_ACKNOWLEDGE)
    class Sspi(val bytes: ByteArray) : SqlServerMessage(SSPI)
    data class FeatureExtAcknowledge(
        val features: List<FeatureAcknowledge>,
    ) : SqlServerMessage(FEATURE_EXT_ACKNOWLEDGE)
    data class Error(
        val code: UInt,
        val state: UByte,
        val errorClass: UByte,
        val message: String,
        val server: String,
        val procedure: String,
        val line: UInt,
    ) : SqlServerMessage(ERROR)

    companion object {
        const val RETURN_STATUS: UInt = 0x79u
        const val COLUMN_METADATA: UInt = 0x81u
        const val ERROR: UInt = 0xAAu
        const val INFO: UInt = 0xABu
        const val ORDER: UInt = 0x79u
        const val COLUMN_INFO: UInt = 0xA5u
        const val RETURN_VALUE: UInt = 0xACu
        const val LOGIN_ACKNOWLEDGE: UInt = 0xADu
        const val ROW: UInt = 0xD1u
        const val NBC_ROW: UInt = 0xD2u
        const val SSPI: UInt = 0xEDu
        const val ENVIRONMENT_CHANGE: UInt = 0xE3u
        const val DONE: UInt = 0xFDu
        const val DONE_PROCEDURE: UInt = 0xFEu
        const val DONE_IN_PROCEDURE: UInt = 0xFFu
        const val FEATURE_EXT_ACKNOWLEDGE: UInt = 0xAEu
    }
}
