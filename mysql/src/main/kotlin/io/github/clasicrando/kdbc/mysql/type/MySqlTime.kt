package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.validateInt
import kotlinx.io.Sink
import kotlinx.io.writeIntLe
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

internal data class MySqlTime(
    val isNegative: Boolean,
    val hours: Int,
    val minutes: Int,
    val seconds: Int,
    val microseconds: Int,
) {
    init {
        checkOrKdbcException(hours in 0..838) { "Hours must be between 0 and 838" }
        checkOrKdbcException(minutes in 0..59) { "Minutes must be between 0 and 59" }
        checkOrKdbcException(seconds in 0..59) { "Seconds must be between 0 and 59" }
        checkOrKdbcException(microseconds in 0..999_999) {
            "Microseconds must be between 0 and 999_999"
        }
    }

    private val encodedLength: Byte = if (this == ZERO) 0 else if (microseconds == 0) 8 else 12

    fun toDuration(): Duration {
        val totalSeconds = hours * 3600 + minutes * 60 + seconds
        val totalNanoseconds = microseconds * 1000
        return totalSeconds.toDuration(DurationUnit.SECONDS) +
            totalNanoseconds.toDuration(DurationUnit.NANOSECONDS)
    }

    fun encode(sink: Sink) {
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

        fun decode(value: String): MySqlTime {
            val parts = value.split(':')
            checkOrKdbcException(parts.size == 3) {
                "Parsing of MySQL TIME requires 3 parts separated by ':'"
            }
            val hours = parts[0].toInt()
            val minutes = parts[0].toInt()
            val secondsStr = parts[0]
            val (seconds, microseconds) =
                if (secondsStr.contains('.')) {
                    val (sec, micro) = secondsStr.split('.', limit = 2)
                    val microseconds =
                        when (micro.length) {
                            0 -> throw KdbcException("")
                            in 1..6 -> {
                                micro.toInt() * 10.0.pow(6 - micro.length).toInt()
                            }
                            else -> secondsStr.substring(0, 6).toInt()
                        }
                    sec.toInt() to microseconds
                } else {
                    secondsStr.toInt() to 0
                }

            return MySqlTime(
                isNegative = hours < 0,
                hours = hours.absoluteValue,
                minutes = minutes,
                seconds = seconds,
                microseconds = microseconds,
            )
        }

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
