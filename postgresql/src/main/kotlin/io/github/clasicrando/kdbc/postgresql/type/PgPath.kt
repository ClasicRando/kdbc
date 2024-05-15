package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS path type represented as a list of connected [points]. Paths can either be closed or
 * open.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-PATHS)
 */
data class PgPath(val isClosed: Boolean, val points: List<PgPoint>) : PgGeometryType {
    override val postGisLiteral: String
        get() = buildString {
            append(if (isClosed) '(' else '[')
            for ((i, point) in points.withIndex()) {
                if (i > 0) {
                    append(',')
                }
                append(point.postGisLiteral)
            }
            append(if (isClosed) ')' else ']')
        }
}
