package io.github.clasicrando.kdbc.core

import kotlinx.uuid.UUID

interface UniqueResourceId {
    /** Name of the resource type to include in the log event */
    val resourceType: String

    /**
     * Unique identifier for this resource, utilized for logging to signify log messages as coming
     * from the same resource. Defaults to an auto-generated UUID.
     */
    val resourceId: UUID

    /**
     * String version of [resourceId] that can be cached to avoid repeated call to [UUID.toString]
     * for logging.
     */
    val resourceIdAsString: String
}
