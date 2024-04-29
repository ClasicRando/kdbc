package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow

object PostDataClassRowParser : RowParser<PostDataClass> {
    override fun fromRow(row: DataRow): PostDataClass {
        return PostDataClass(
            row.getInt(0)!!,
            row.getString(1)!!,
            row.getLocalDateTime(2)!!,
            row.getLocalDateTime(3)!!,
            row.getInt(4),
            row.getInt(5),
            row.getInt(6),
            row.getInt(7),
            row.getInt(8),
            row.getInt(9),
            row.getInt(10),
            row.getInt(11),
            row.getInt(12),
        )
    }
}
