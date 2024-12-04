package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
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

/**
 * Implementation of a [MySqlTypeDescription] for the [LocalTime] type. Accepts `TIME` when
 * decoding.
 */
internal object LocalTimeTypeDescription :
    MySqlTypeDescription<LocalTime>(dbType = MySqlType.Time, kType = typeOf<LocalTime>()) {
    private val formatter = DateTimeFormatter.ofPattern("HH:mm:ss[.S]")

    /**
     * Writes the standard time value where the value is always positive, the days value is 0 and
     * the other values are encoded using the components of a [LocalTime].
     *
     * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time)
     */
    override fun encode(value: LocalTime, buffer: Sink) {
        val length = value.encodedLength()
        buffer.writeByte(length)
        buffer.writeByte(0)
        buffer.writeInt(0)
        buffer.encodeTime(value, length > 8)
    }

    /** Decode using the [MySqlTime.decode], checking to ensure the hours property is valid */
    override fun decodeBytes(value: MySqlValue.Binary): LocalTime {
        val time = MySqlTime.decode(value.bytes)
        checkOrMySqlException(time.hours <= 24) {
            "Tried to decode a TIME value where hours = ${time.hours}"
        }
        return LocalTime.of(time.hours, time.minutes, time.seconds, time.microseconds * 1000)
    }

    /** Parse the text value using 'HH:mm:ss[.S]' */
    override fun decodeText(value: MySqlValue.Text): LocalTime {
        return LocalTime.parse(value.text, formatter)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Duration] type. Accepts `TIME` when decoding.
 */
internal object DurationTypeDescription :
    MySqlTypeDescription<Duration>(dbType = MySqlType.Time, kType = typeOf<Duration>()) {
    /** Converts itself to a [MySqlTime] and encodes using that structure */
    override fun encode(value: Duration, buffer: Sink) {
        MySqlTime.fromDuration(value).encode(buffer)
    }

    /** Decodes as a [MySqlTime] then calls [MySqlTime.toDuration] */
    override fun decodeBytes(value: MySqlValue.Binary): Duration {
        val time = MySqlTime.decode(value.bytes)
        return time.toDuration()
    }

    /** Decodes as a [MySqlTime] then calls [MySqlTime.toDuration] */
    override fun decodeText(value: MySqlValue.Text): Duration {
        val time = MySqlTime.decode(value.text)
        return time.toDuration()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [LocalDate] type. Accepts `DATE` when
 * decoding.
 */
internal object LocalDateTypeDescription :
    MySqlTypeDescription<LocalDate>(dbType = MySqlType.Date, kType = typeOf<LocalDate>()) {
    /**
     * Writes the length as 4 bytes (year = 2, month = 1, day = 1) then encodes by calling
     * [encodeDate]
     *
     * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date)
     */
    override fun encode(value: LocalDate, buffer: Sink) {
        buffer.writeByte(4)
        buffer.encodeDate(value)
    }

    /**
     * Decodes by reading the first byte (assumed to be 4, meaning time portion is ignored) then
     * calls [decodeDate]
     *
     * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date)
     */
    override fun decodeBytes(value: MySqlValue.Binary): LocalDate {
        value.bytes.readByte()
        return value.bytes.decodeDate()
    }

    /** Parses the text assuming an ISO standard date of 'YYYY-MM-DD' */
    override fun decodeText(value: MySqlValue.Text): LocalDate {
        return LocalDate.parse(value.text)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [LocalDateTime] type. Accepts `DATETIME` and
 * `TIMESTAMP` when decoding.
 */
internal object LocalDateTimeTypeDescription :
    MySqlTypeDescription<LocalDateTime>(
        dbType = MySqlType.Datetime,
        kType = typeOf<LocalDateTime>(),
    ) {
    private val formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss[.S]")

    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    /**
     * Finds the encoded length of the datetime, writing the that length, the date using
     * [encodeDate] and then encoding the time using [encodeTime] if the time part is non-zero.
     *
     * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date)
     */
    override fun encode(value: LocalDateTime, buffer: Sink) {
        val length = value.encodedLength()
        buffer.writeByte(length)
        buffer.encodeDate(value.toLocalDate())

        if (length > 4) {
            buffer.encodeTime(value.toLocalTime(), length > 7)
        }
    }

    /**
     * Reads the length of the value, decodes the date using [decodeDate], then optionally decodes
     * the time using [decodeTime] if the time component is non-zero. If no time is present then
     * [LocalDate.atStartOfDay] is used.
     *
     * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date)
     */
    override fun decodeBytes(value: MySqlValue.Binary): LocalDateTime {
        val length = value.bytes.readByte()
        val date = value.bytes.decodeDate()
        return if (length > 4) {
            date.atTime(value.bytes.decodeTime(length - 4))
        } else {
            date.atStartOfDay()
        }
    }

    /** Decodes the returned datetime using a format of 'uuuu-MM-dd HH:mm:ss[.S]' */
    override fun decodeText(value: MySqlValue.Text): LocalDateTime {
        return LocalDateTime.parse(value.text, formatter)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [OffsetDateTime] type. Accepts `DATETIME` and
 * `TIMESTAMP` when decoding. This differs from [LocalDateTimeTypeDescription] since a predefined
 * [ZoneOffset] supplied by the client is used to offset the decoded [LocalDateTime].
 */
internal class OffsetDateTimeTypeDescription(private val zoneOffset: ZoneOffset) :
    MySqlTypeDescription<OffsetDateTime>(
        dbType = MySqlType.Timestamp,
        kType = typeOf<OffsetDateTime>(),
    ) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    /** Changes the offset to [ZoneOffset.UTC] and encodes using [LocalDateTimeTypeDescription] */
    override fun encode(value: OffsetDateTime, buffer: Sink) {
        LocalDateTimeTypeDescription.encode(
            value.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime(),
            buffer,
        )
    }

    /** Decodes using [LocalDateTimeTypeDescription] and applies the [zoneOffset] specified */
    override fun decodeBytes(value: MySqlValue.Binary): OffsetDateTime {
        val local = LocalDateTimeTypeDescription.decode(value)
        return OffsetDateTime.of(local, zoneOffset)
    }

    /** Decodes using [LocalDateTimeTypeDescription] and applies the [zoneOffset] specified */
    override fun decodeText(value: MySqlValue.Text): OffsetDateTime {
        val local = LocalDateTimeTypeDescription.decode(value)
        return OffsetDateTime.of(local, zoneOffset)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Instant] type. Accepts `DATETIME` and
 * `TIMESTAMP` when decoding. This differs from [LocalDateTimeTypeDescription] since a predefined
 * [ZoneOffset] supplied by the client is used to offset the decoded [LocalDateTime].
 */
internal class InstantTypeDescription(private val zoneOffset: ZoneOffset) :
    MySqlTypeDescription<Instant>(dbType = MySqlType.Timestamp, kType = typeOf<Instant>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Timestamp || dbType == MySqlType.Datetime
    }

    /**
     * Calls [LocalDateTime.ofInstant] with [ZoneOffset.UTC] then encodes using
     * [LocalDateTimeTypeDescription]
     */
    override fun encode(value: Instant, buffer: Sink) {
        LocalDateTimeTypeDescription.encode(LocalDateTime.ofInstant(value, ZoneOffset.UTC), buffer)
    }

    /** Decodes using [LocalDateTimeTypeDescription] and applies the [zoneOffset] specified */
    override fun decodeBytes(value: MySqlValue.Binary): Instant {
        val local = LocalDateTimeTypeDescription.decode(value)
        return local.toInstant(zoneOffset)
    }

    /** Decodes using [LocalDateTimeTypeDescription] and applies the [zoneOffset] specified */
    override fun decodeText(value: MySqlValue.Text): Instant {
        val local = LocalDateTimeTypeDescription.decode(value)
        return local.toInstant(zoneOffset)
    }
}

private val LocalTime.microSecond: Int
    get() = TimeUnit.NANOSECONDS.toMicros(this.nano.toLong()).toInt()

/** Returns zero if no microsecond value is present (i.e. time is just hour, minute second) */
private fun LocalTime.encodedLength(): Byte {
    if (microSecond == 0) {
        return 8
    }
    return 12
}

/**
 * Returns 11 if a microsecond value is present, 7 if no microsecond but some time component is
 * non-zero or 4 if all time components are zero
 */
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

/**
 * Encode the [date] by writing the year as a [Short], the month as a [Byte] and the day of the
 * month as a [Byte]
 *
 * @throws io.github.clasicrando.kdbc.mysql.exceptions.MySqlException if the year exceeds a [Short]
 */
private fun Sink.encodeDate(date: LocalDate) {
    val year = date.year
    checkOrMySqlException(year in Short.MIN_VALUE..Short.MAX_VALUE) {
        "Year of date must be between ${Short.MIN_VALUE} and ${Short.MAX_VALUE} but found $year"
    }
    writeShortLe(year.toShort())
    writeByte(date.monthValue.toByte())
    writeByte(date.dayOfMonth.toByte())
}

/** Encode the [time], including the [microSecond] value if requested */
private fun Sink.encodeTime(time: LocalTime, includeMicro: Boolean) {
    writeByte(time.hour.toByte())
    writeByte(time.minute.toByte())
    writeByte(time.second.toByte())
    if (includeMicro) {
        writeIntLe(time.microSecond)
    }
}

/**
 * Decode a [LocalDate] from this buffer as the year ([Short]), month ([Byte]) and day ([Byte])
 *
 * @throws io.github.clasicrando.kdbc.mysql.exceptions.MySqlException if the date is a zero date or
 *   the buffer has 3 or fewer bytes.
 */
private fun ByteReadBuffer.decodeDate(): LocalDate {
    val length = remaining()
    checkOrMySqlException(length > 0) { "Found a zero date when trying to decode DATE value" }
    checkOrMySqlException(length >= 4) {
        "Require 4 bytes to decode a DATE value but only found $length"
    }
    return LocalDate.of(
        readShortLe().toInt() and 0xff_ff,
        readByte().toInt() and 0xff,
        readByte().toInt() and 0xff,
    )
}

/**
 * Decode a [LocalTime] from this buffer as the hour ([Byte]), minute ([Byte]), second ([Byte]) and
 * optional microsecond ([Long] where the number of bytes used is defined as [length] - 3).
 *
 * @throws io.github.clasicrando.kdbc.core.exceptions.KdbcException if the microsecond value ends up
 *   being larger than [Int.MAX_VALUE].
 */
private fun ByteReadBuffer.decodeTime(length: Int): LocalTime {
    val hour = readByteAsInt()
    val minute = readByteAsInt()
    val second = readByteAsInt()
    val microSecond = if (length > 3) readIntLe(remaining()) else 0
    return LocalTime.of(hour, minute, second, validateInt(microSecond))
}
