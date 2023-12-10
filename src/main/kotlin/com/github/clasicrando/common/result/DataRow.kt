package com.github.clasicrando.common.result

import com.github.clasicrando.common.datetime.DateTime
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime

interface DataRow {
    fun indexFromColumn(column: String): Int
    operator fun get(index: Int): Any?
}

operator fun DataRow.get(column: String): Any? = get(indexFromColumn(column))
@Suppress("UNCHECKED_CAST")
fun <T : Any> DataRow.getAs(index: Int): T? = get(index) as T?
fun <T : Any> DataRow.getAs(column: String): T? = getAs(indexFromColumn(column))
fun DataRow.getBoolean(index: Int): Boolean? = getBooleanCoerce(get(index))
fun DataRow.getBoolean(column: String): Boolean? = getBooleanCoerce(get(column))
private fun getBooleanCoerce(value: Any?): Boolean? {
    return when (value) {
        null -> return null
        is Boolean -> value
        is Byte -> value == 1.toByte()
        else -> error("Cannot coerce type to boolean, value '$value'")
    }
}
fun DataRow.getByte(index: Int): Byte? = get(index) as Byte?
fun DataRow.getByte(column: String): Byte? = get(column) as Byte?
fun DataRow.getShort(index: Int): Short? = get(index) as Short?
fun DataRow.getShort(column: String): Short? = get(column) as Short?
fun DataRow.getInt(index: Int): Int? = get(index) as Int?
fun DataRow.getInt(column: String): Int? = get(column) as Int?
fun DataRow.getLong(index: Int): Long? = get(index) as Long?
fun DataRow.getLong(column: String): Long? = get(column) as Long?
fun DataRow.getFloat(index: Int): Float? = get(index) as Float?
fun DataRow.getFloat(column: String): Float? = get(column) as Float?
fun DataRow.getDate(index: Int): LocalDate? = get(index) as LocalDate?
fun DataRow.getDate(column: String): LocalDate? = get(column) as LocalDate?
fun DataRow.getTime(index: Int): LocalTime? = get(index) as LocalTime?
fun DataRow.getTime(column: String): LocalTime? = get(column) as LocalTime?
fun DataRow.getDateTime(index: Int): LocalDateTime? = get(index) as LocalDateTime?
fun DataRow.getDateTIme(column: String): LocalDateTime? = get(column) as LocalDateTime?
fun DataRow.getDateTimeTimezone(index: Int): DateTime? = get(index) as DateTime?
fun DataRow.getDateTImeTimeZone(column: String): DateTime? = get(column) as DateTime?
