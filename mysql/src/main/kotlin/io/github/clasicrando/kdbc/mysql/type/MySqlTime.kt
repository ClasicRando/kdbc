package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.validateInt
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import kotlin.math.absoluteValue
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Format of time only related data returned from MySQL. This type allows going past a regular
 * 24-hour day since it's possible to represent a duration using `TIME`.
 */
internal data class MySqlTime(
    val isNegative: Boolean,
    /** Total number of hours within the time. Must be between 0 and 838 */
    val hours: Int,
    /** Total number of minutes within the time. Must be between 0 and 59 */
    val minutes: Int,
    /** Total number of seconds within the time. Must be between 0 and 59 */
    val seconds: Int,
    /** Total number of microseconds within the time. Must be between 0 and 999_999 */
    val microseconds: Int,
) {
    init {
        checkOrMySqlException(hours in 0..838) { "Hours must be between 0 and 838. Found $hours" }
        checkOrMySqlException(minutes in 0..59) {
            "Minutes must be between 0 and 59. Found $minutes"
        }
        checkOrMySqlException(seconds in 0..59) {
            "Seconds must be between 0 and 59. Found $seconds"
        }
        checkOrMySqlException(microseconds in 0..999_999) {
            "Microseconds must be between 0 and 999_999. Found $microseconds"
        }
    }

    private val encodedLength: Byte = if (this == ZERO) 0 else if (microseconds == 0) 8 else 12

    /** Convert this value to it's equivalent [Duration] */
    fun toDuration(): Duration {
        val totalSeconds = hours * 3600 + minutes * 60 + seconds
        val totalNanoseconds = microseconds * 1000
        return totalSeconds.toDuration(DurationUnit.SECONDS) +
            totalNanoseconds.toDuration(DurationUnit.NANOSECONDS)
    }

    /** Encode this value into the supplied [sink] */
    fun encode(sink: ByteWriteBuffer) {
        if (this == ZERO) {
            sink.writeByte(0)
            return
        }

        sink.writeByte(this.encodedLength)
        sink.writeByte(if (this.isNegative) 1 else 0)
        val days = this.hours / 24
        val hours = this.hours % 24
        sink.writeIntLe(days)
        sink.writeByte(hours.toByte())
        sink.writeByte(minutes.toByte())
        sink.writeByte(seconds.toByte())

        if (microseconds != 0) {
            sink.writeIntLe(microseconds)
        }
    }

    companion object {
        val ZERO: MySqlTime =
            MySqlTime(isNegative = false, hours = 0, minutes = 0, seconds = 0, microseconds = 0)

        /**
         * Decode a [MySqlTime] from this byte [buffer].
         *
         * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time)
         */
        fun decode(buffer: ByteReadBuffer): MySqlTime {
            val length = buffer.readByteAsInt()
            if (length == 0) {
                return ZERO
            }
            val isNegative = buffer.readByteAsInt() == 1
            val days = buffer.readIntLe()
            val hours = buffer.readByteAsInt() + days * 24
            val minutes = buffer.readByteAsInt()
            val seconds = buffer.readByteAsInt()
            val microseconds =
                if (length == 12) {
                    buffer.readIntLe()
                } else {
                    0
                }
            return MySqlTime(
                isNegative = isNegative,
                hours = hours,
                minutes = minutes,
                seconds = seconds,
                microseconds = microseconds,
            )
        }

        /**
         * Decode a [MySqlTime] from this time string. Expected format is
         * `[0-9]+:[0-9]{1,2}:[0-9]{1,2}(.[0-9]+)?` (NOTE: THIS IS A TEMPLATE, THERE IS NO REGEX
         * USED)
         */
        fun decode(value: String): MySqlTime {
            val parts = value.split(':')
            checkOrMySqlException(parts.size == 3) {
                "Parsing of MySQL TIME requires 3 parts separated by ':'"
            }
            val hours = parts[0].toInt()
            val minutes = parts[1].toInt()
            val secondsStr = parts[2]
            val (seconds, microseconds) = splitSecondsPart(secondsStr)

            return MySqlTime(
                isNegative = hours < 0,
                hours = hours.absoluteValue,
                minutes = minutes,
                seconds = seconds,
                microseconds = microseconds,
            )
        }

        /**
         * Split seconds that may include a fractional component into whole seconds and whole
         * microseconds. If there is a nanoseconds component, that component is dropped.
         */
        private fun splitSecondsPart(secondsStr: String): Pair<Int, Int> {
            if (!secondsStr.contains('.')) {
                return secondsStr.toInt() to 0
            }
            val (sec, micro) = secondsStr.split('.', limit = 2)
            val microseconds =
                when (micro.length) {
                    0 -> 0
                    in 1..6 -> micro.padEnd(length = 6, padChar = '0').toInt()
                    else -> secondsStr.substring(0, 6).toInt()
                }
            return sec.toInt() to microseconds
        }

        /**
         * Convert [Duration] to [MySqlTime]. If the duration contains any nanoseconds that
         * precision is lost since MySQL only retains precision up to microseconds.
         */
        fun fromDuration(value: Duration): MySqlTime {
            return value.toComponents { days, hours, minutes, seconds, nanoseconds ->
                val totalHours = days * 24 + hours
                val microseconds = nanoseconds / 1000

                MySqlTime(
                    isNegative = !value.isPositive(),
                    hours = validateInt(totalHours),
                    minutes = minutes,
                    seconds = seconds,
                    microseconds = microseconds,
                )
            }
        }
    }
}
