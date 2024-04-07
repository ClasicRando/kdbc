package com.github.kdbc.core

import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

abstract class DefaultUniqueResourceId : UniqueResourceId {
    override val resourceId: UUID = UUID.generateUUID()

    override val resourceIdAsString: String by lazy { resourceId.toString() }
}
