package com.github.clasicrando.sqlserver.column

import com.github.clasicrando.common.datetime.DateTime as DateTimeOffset
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.uuid.UUID
import java.math.BigDecimal

sealed interface ColumnData {
    data class U8(val inner: UByte?): ColumnData
    data class I16(val inner: Short?): ColumnData
    data class I32(val inner: Int?): ColumnData
    data class I64(val inner: Long?): ColumnData
    data class F32(val inner: Float?): ColumnData
    data class F64(val inner: Double?): ColumnData
    data class Bit(val inner: Boolean?): ColumnData
    data class Str(val inner: String?): ColumnData
    data class Guid(val inner: UUID?): ColumnData
    class Binary(val inner: ByteArray?): ColumnData
    data class Numeric(val inner: BigDecimal?): ColumnData
    data class Xml(val inner: String?): ColumnData
    data class DateTime(val inner: LocalDateTime?): ColumnData
    data class SmallDateTime(val inner: LocalDateTime?): ColumnData
    data class Time(val inner: LocalTime?): ColumnData
    data class Date(val inner: LocalDate?): ColumnData
    data class DateTime2(val inner: Short?): ColumnData
    data class DateTimeOffset(val inner: DateTimeOffset?): ColumnData
}
