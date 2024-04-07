package com.github.kdbc.core

/**
 * Interface to specify the type contains resources that needs to be cleaned up after usage or
 * reset before reuse.
 *
 * The [release] method does not prescribe if the type is fully releasing resources such that the
 * instance should not be used anymore (e.g. closing a physical TCP connection) or simply cleaning
 * up the resources for reuse (e.g. clearing a buffer but not disposing). For more clear behaviour
 * of this method, consult the implementing class' definition.
 */
interface AutoRelease {
    /**
     * Release/cleanup resources that back this type. It is not an error to call this method
     * multiple times.
     *
     * This method does not prescribe if the type is fully releasing resources such that the
     * instance should not be used anymore (e.g. closing a physical TCP connection) or simply
     * cleaning up the resources for reuse (e.g. clearing a buffer but not disposing). For more
     * clear behaviour of this method, consult the implementing class' definition.
     */
    fun release()
}

/**
 * Use the [AutoRelease] instance for the duration of the [block] (supplied as lambda's single
 * parameter) and always call its [AutoRelease.release] method before exiting this method using a
 * `finally` block. Errors thrown within the block or the [AutoRelease.release] method are
 * rethrown, suppressing the [AutoRelease.release] error if an error has already been thrown.
 */
inline fun <A : AutoRelease, R> A.use(block: (A) -> R): R {
    var cause: Throwable? = null
    return try {
        block(this)
    } catch (ex: Throwable) {
        cause = ex
        throw cause
    } finally {
        when (cause) {
            null -> this.release()
            else -> try {
                this.release()
            } catch (ex2: Throwable) {
                cause.addSuppressed(ex2)
            }
        }
    }
}
