package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS circle type represented as the [center] of the circle and the [radius].
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-CIRCLE)
 */
data class PgCircle(val center: PgPoint, val radius: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "<${center.postGisLiteral},$radius>"
}
