package com.github.kdbc.postgresql.type

/**
 * Implementors of this type are PostGIS geometry types with literal representations that are good
 * to have for debugging/logging purposes
 */
interface PgGeometryType {
    /**
     * Literal representation of the geometry type. All geometry types literal representation can
     * be found [here](https://www.postgresql.org/docs/16/datatype-geometric.html).
     */
    val postGisLiteral: String
}
