package io.github.clasicrando.kdbc.postgresql.copy

/**
 * Implementors of this interface can supply its data fields as row fields for a CSV or text based
 * copy operation. The values within the [List] must be ordered to match the table that is the copy
 * target.
 */
interface PgCsvCopyRow {
    /**
     * The values passed to a CSV/text COPY operation. These values will have [Any.toString] to
     * resolve as a CSV/text value. This property should be calculated once per class instance to
     * avoid [List] allocations for each call to this method. If you are not sure if this property
     * will be called, you can also delegate the field value using a [lazy] delegate.
     */
    val values: List<Any?>
}
