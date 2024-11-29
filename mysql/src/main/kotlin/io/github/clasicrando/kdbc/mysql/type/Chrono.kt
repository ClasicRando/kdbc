package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.reflect.typeOf
import kotlin.time.Duration
import kotlinx.io.Sink
import kotlinx.io.writeIntLe
import kotlinx.io.writeShortLe

internal object LocalTimeTypeDescription :
    MySqlTypeDescription<LocalTime>(dbType = MySqlType.Time, kType = typeOf<LocalTime>()) {
    private val formatter = DateTimeFormatter.ofPattern("HH:mm:ss[.S]")

    override fun encode(value: LocalTime, buffer: Sink) {
        val length = value.encodedLength()
        buffer.writeByte(length)
        buffer.writeByte(0)
        buffer.writeInt(0)
        buffer.encodeTime(value, length > 8)
    }

    override fun decodeBytes(value: MySqlValue.Binary): LocalTime {
        val time = MySqlTime.decode(value.bytes)
        return LocalTime.of(time.hours, time.minutes, time.seconds, time.microseconds * 1000)
    }

    override fun decodeText(value: MySqlValue.Text): LocalTime {
        return LocalTime.parse(value.text, formatter)
    }
}

internal object DurationTypeDescription :
    MySqlTypeDescription<Duration>(dbType = MySqlType.Time, kType = typeOf<Duration>()) {
    override fun encode(value: Duration, buffer: Sink) {
        MySqlTime.fromDuration(value).encode(buffer)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Duration {
        val time = MySqlTime.decode(value.bytes)
        return time.toDuration()
    }

    override fun decodeText(value: MySqlValue.Text): Duration {
        val time = MySqlTime.decode(value.text)
        return time.toDuration()
    }
}

internal object LocalDateTypeDescription :
    MySqlTypeDescription<LocalDate>(dbType = MySqlType.Date, kType = typeOf<LocalDate>()) {
    override fun encode(value: LocalDate, buffer: Sink) {
        buffer.writeByte(4)
        buffer.encodeDate(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): LocalDate {
        value.bytes.readByte()
        return value.bytes.decodeDate()
    }

    override fun decodeText(value: MySqlValue.Text): LocalDate {
        return LocalDate.parse(value.text)
    }
}

internal object LocalDateTimeTypeDescription :
    MySqlTypeDescription<LocalDateTime>(
        dbType = MySqlType.Datetime,
        kType = typeOf<LocalDateTime>(),
    ) {
    private val formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss[.S]")

    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    override fun encode(value: LocalDateTime, buffer: Sink) {
        val length = value.encodedLength()
        buffer.writeByte(length)
        buffer.encodeDate(value.toLocalDate())

        if (length > 4) {
            buffer.encodeTime(value.toLocalTime(), length > 7)
        }
    }

    override fun decodeBytes(value: MySqlValue.Binary): LocalDateTime {
        val length = value.bytes.readByte()
        val date = value.bytes.decodeDate()
        return if (length > 4) {
            date.atTime(value.bytes.decodeTime(length - 4))
        } else {
            date.atStartOfDay()
        }
    }

    override fun decodeText(value: MySqlValue.Text): LocalDateTime {
        return LocalDateTime.parse(value.text, formatter)
    }
}

internal class OffsetDateTimeTypeDescription(private val zoneOffset: ZoneOffset) :
    MySqlTypeDescription<OffsetDateTime>(
        dbType = MySqlType.Timestamp,
        kType = typeOf<OffsetDateTime>(),
    ) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    override fun encode(value: OffsetDateTime, buffer: Sink) {
        LocalDateTimeTypeDescription.encode(
            value.atZoneSameInstant(zoneOffset).toLocalDateTime(),
            buffer,
        )
    }

    override fun decodeBytes(value: MySqlValue.Binary): OffsetDateTime {
        val local = LocalDateTimeTypeDescription.decode(value)
        return OffsetDateTime.of(local, zoneOffset)
    }

    override fun decodeText(value: MySqlValue.Text): OffsetDateTime {
        val local = LocalDateTimeTypeDescription.decode(value)
        return OffsetDateTime.of(local, zoneOffset)
    }
}

internal object InstantTypeDescription :
    MySqlTypeDescription<Instant>(dbType = MySqlType.Timestamp, kType = typeOf<Instant>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    override fun encode(value: Instant, buffer: Sink) {
        LocalDateTimeTypeDescription.encode(LocalDateTime.ofInstant(value, ZoneOffset.UTC), buffer)
    }

    override fun decodeBytes(value: MySqlValue.Binary): Instant {
        val local = LocalDateTimeTypeDescription.decode(value)
        return local.toInstant(ZoneOffset.UTC)
    }

    override fun decodeText(value: MySqlValue.Text): Instant {
        val local = LocalDateTimeTypeDescription.decode(value)
        return local.toInstant(ZoneOffset.UTC)
    }
}

private val LocalTime.microSecond: Int
    get() = TimeUnit.NANOSECONDS.toMicros(this.nano.toLong()).toInt()

private fun LocalTime.encodedLength(): Byte {
    if (microSecond == 0) {
        return 8
    }
    return 12
}

private fun LocalDateTime.encodedLength(): Byte {
    val microSecond = this.toLocalTime().microSecond
    if (microSecond != 0) {
        return 11
    }
    if (hour != 0 || minute != 0 || second != 0) {
        return 7
    }
    return 4
}

private fun Sink.encodeDate(date: LocalDate) {
    val year = date.year
    if (year !in Short.MIN_VALUE..Short.MAX_VALUE) {
        throw KdbcException(
            "Year of date must be between ${Short.MIN_VALUE} and ${Short.MAX_VALUE} but found $year"
        )
    }
    writeShortLe(year.toShort())
    writeByte(date.monthValue.toByte())
    writeByte(date.dayOfMonth.toByte())
}

private fun Sink.encodeTime(time: LocalTime, includeMicro: Boolean) {
    writeByte(time.hour.toByte())
    writeByte(time.minute.toByte())
    writeByte(time.second.toByte())
    if (includeMicro) {
        writeIntLe(time.microSecond)
    }
}

private fun ByteReadBuffer.decodeDate(): LocalDate {
    val length = remaining()
    if (length == 0) {
        throw KdbcException("Found a zero date when trying to decode DATE value")
    }
    if (length < 4) {
        throw KdbcException("Require 4 bytes to decode a DATE value but only found $length")
    }
    return LocalDate.of(
        readShortLe().toInt() and 0xff_ff,
        readByte().toInt() and 0xff,
        readByte().toInt() and 0xff,
    )
}

private fun ByteReadBuffer.decodeTime(length: Int): LocalTime {
    val hour = readByteAsInt()
    val minute = readByteAsInt()
    val second = readByteAsInt()
    val microSecond = if (length > 3) readIntLe(remaining()) else 0
    return LocalTime.of(hour, minute, second, validateInt(microSecond))
}
