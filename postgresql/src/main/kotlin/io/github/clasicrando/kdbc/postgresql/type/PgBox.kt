package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS box type represented as the opposite corners of the box.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-BOXES)
 */
data class PgBox(val high: PgPoint, val low: PgPoint) : PgGeometryType {
    override val postGisLiteral: String
        get() = "(${high.postGisLiteral},${low.postGisLiteral})"
}
