package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS line type represented as a linear equation:
 *
 * [a]x + [b]y + [c] = 0
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-LINE)
 */
data class PgLine(val a: Double, val b: Double, val c: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "{$a,$b,$c}"
}
