package io.github.clasicrando.kdbc.postgresql.type

/**
 * PostGIS polygon type represented as a list of connected [points]. Polygons are very similar to a
 * closed [PgPath] but polygons are always closed, define a [boundBox] and are considered to
 * contain the area within the closed path.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-POLYGON)
 */
data class PgPolygon(val boundBox: PgBox, val points: List<PgPoint>) : PgGeometryType {
    constructor(points: List<PgPoint>): this(makeBoundBox(points), points)

    override val postGisLiteral: String
        get() = "(${points.joinToString(separator = ",") { it.postGisLiteral }})"

    companion object {
        fun makeBoundBox(points: List<PgPoint>): PgBox {
            require(points.isNotEmpty()) { "Cannot make bounding box for polygon with no points" }
            var x1 = points[0].x
            var x2 = points[0].x
            var y1 = points[0].y
            var y2 = points[0].y
            for (i in 1..<points.size) {
                if (points[i].x < x1) {
                    x1 = points[i].x
                }
                if (points[i].x > x2) {
                    x2 = points[i].x
                }
                if (points[i].y < y1) {
                    y1 = points[i].y
                }
                if (points[i].y < y2) {
                    y2 = points[i].y
                }
            }
            return PgBox(
                high = PgPoint(x2, y2),
                low = PgPoint(x1, y1),
            )
        }
    }
}
