package com.github.clasicrando.common.result

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime

interface DataRow {
    fun indexFromColumn(column: String): Int
    operator fun get(index: Int): Any?
    operator fun get(column: String): Any? = get(indexFromColumn(column))
    fun getBoolean(index: Int): Boolean? = getBooleanCoerce(get(index))
    fun getBoolean(column: String): Boolean? = getBooleanCoerce(get(column))
    private fun getBooleanCoerce(value: Any?): Boolean? {
        return when (value) {
            null -> return null
            is Boolean -> value
            is Byte -> value == 1.toByte()
            else -> error("Cannot coerce type to boolean, value '$value'")
        }
    }
    fun getByte(index: Int): Byte? = get(index) as Byte?
    fun getByte(column: String): Byte? = get(column) as Byte?
    fun getShort(index: Int): Short? = get(index) as Short?
    fun getShort(column: String): Short? = get(column) as Short?
    fun getInt(index: Int): Int? = get(index) as Int?
    fun getInt(column: String): Int? = get(column) as Int?
    fun getLong(index: Int): Long? = get(index) as Long?
    fun getLong(column: String): Long? = get(column) as Long?
    fun getFloat(index: Int): Float? = get(index) as Float?
    fun getFloat(column: String): Float? = get(column) as Float?
    fun getDate(index: Int): LocalDate? = get(index) as LocalDate?
    fun getDate(column: String): LocalDate? = get(column) as LocalDate?
    fun getTime(index: Int): LocalTime? = get(index) as LocalTime?
    fun getTime(column: String): LocalTime? = get(column) as LocalTime?
    fun getDateTime(index: Int): LocalDateTime? = get(index) as LocalDateTime?
    fun getDateTIme(column: String): LocalDateTime? = get(column) as LocalDateTime?
    fun getDateTimeTimezone(index: Int): OffsetDateTime? = get(index) as OffsetDateTime?
    fun getDateTImeTimeZone(column: String): OffsetDateTime? = get(column) as OffsetDateTime?
}
