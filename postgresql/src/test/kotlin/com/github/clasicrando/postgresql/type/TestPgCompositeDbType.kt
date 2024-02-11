package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.datetime.DateTime

class TestPgCompositeDbType {
    data class CompositeType(
        val int: Int,
        val float: Float,
        val string: String,
        val dateTime: DateTime,
    )
}
