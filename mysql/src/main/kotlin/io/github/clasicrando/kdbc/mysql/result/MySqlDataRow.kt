package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.result.DataRow
import kotlin.reflect.KType

internal class MySqlDataRow(
    private val values: Array<MySqlValue?>,
) : DataRow {
    override fun indexFromColumn(column: String): Int {
        TODO("Not yet implemented")
    }

    override fun get(
        index: Int,
        type: KType,
    ): Any? {
        TODO("Not yet implemented")
    }
}
