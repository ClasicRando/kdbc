package io.github.clasicrando.kdbc.core.pool

internal interface ConcurrentBagEntry {
    /**
     * Compares to the current [EntryStatus] and only updates the status to the [newStatus] if the
     * [expectedStatus] is the current value.
     *
     * @return true if the operation updated the value, otherwise returns false
     */
    fun compareAndSetStatus(expectedStatus: EntryStatus, newStatus: EntryStatus): Boolean

    /** Update the [status] without checking the current state */
    fun setStatus(status: EntryStatus)

    /** Return the current [EntryStatus] */
    fun getStatus(): EntryStatus
}
