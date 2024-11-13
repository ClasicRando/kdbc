package io.github.clasicrando.kdbc.core

import kotlin.uuid.Uuid

public interface UniqueResourceId {
    /** Name of the resource type to include in the log event */
    public val resourceType: String

    /**
     * Unique identifier for this resource, utilized for logging to signify log messages as coming
     * from the same resource. Defaults to an auto-generated Uuid.
     */
    public val resourceId: Uuid

    /**
     * String version of [resourceId] that can be cached to avoid repeated call to [Uuid.toString]
     * for logging.
     */
    public val resourceIdAsString: String
}
