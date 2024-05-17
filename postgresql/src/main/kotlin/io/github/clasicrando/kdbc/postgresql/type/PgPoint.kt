package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS point type represented as a pair of coordinates in a two-dimensional space.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-POINTS)
 */
data class PgPoint(val x: Double, val y: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "($x,$y)"
}
