package io.github.clasicrando.kdbc.core.exceptions

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow

/** [KdbcException] thrown when a [RowParser] fails to convert a [DataRow] into the required type */
class RowParseError(
    rowParser: RowParser<*>,
    exception: Throwable? = null,
    reason: String? = null,
) : KdbcException(
    "Failed to parse row into type using row parser ${rowParser::class.simpleName}"
    + if (reason != null) ".$reason" else ""
) {
    init {
        addSuppressed(exception)
    }
}
