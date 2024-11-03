package io.github.clasicrando.kdbc.core

import kotlin.uuid.Uuid

public abstract class DefaultUniqueResourceId : UniqueResourceId {
    final override val resourceId: Uuid = Uuid.random()

    final override val resourceIdAsString: String by lazy { resourceId.toString() }
}
