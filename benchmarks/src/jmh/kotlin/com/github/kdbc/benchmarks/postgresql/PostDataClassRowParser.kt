package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.result.getAsNonNull

object PostDataClassRowParser : RowParser<PostDataClass> {
    override fun fromRow(row: DataRow): PostDataClass {
        return PostDataClass(
            row.getAsNonNull(0),
            row.getAsNonNull(1),
            row.getAsNonNull(2),
            row.getAsNonNull(3),
            row.getAs(4),
            row.getAs(5),
            row.getAs(6),
            row.getAs(7),
            row.getAs(8),
            row.getAs(9),
            row.getAs(10),
            row.getAs(11),
            row.getAs(12),
        )
    }
}
