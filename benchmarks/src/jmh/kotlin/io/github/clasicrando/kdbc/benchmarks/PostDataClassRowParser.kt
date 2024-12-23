package io.github.clasicrando.kdbc.benchmarks

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getIntNonNull
import io.github.clasicrando.kdbc.core.result.getLocalDateTimeNonNull
import io.github.clasicrando.kdbc.core.result.getStringNonNull

object PostDataClassRowParser : RowParser<PostDataClass> {
    override fun fromRow(row: DataRow): PostDataClass {
        return PostDataClass(
            row.getIntNonNull(0),
            row.getStringNonNull(1),
            row.getLocalDateTimeNonNull(2),
            row.getLocalDateTimeNonNull(3),
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
