package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS line segment type represented as a pair of points that are endpoints of the segment.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-LSEG)
 */
data class PgLineSegment(val point1: PgPoint, val point2: PgPoint) : PgGeometryType {
    override val postGisLiteral: String
        get() = "(${point1.postGisLiteral},${point2.postGisLiteral})"
}
