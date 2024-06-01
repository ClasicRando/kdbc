package io.github.clasicrando.kdbc.core

import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

abstract class DefaultUniqueResourceId : UniqueResourceId {
    final override val resourceId: UUID = UUID.generateUUID()

    final override val resourceIdAsString: String by lazy { resourceId.toString() }
}
