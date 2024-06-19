package io.github.clasicrando.kdbc.core

/**
 * An object that may hold resources (such as file or socket handles) until it is closed. The
 * [close] method of an [AutoCloseableAsync] object should rarely be called itself unless you
 * handle the lifetime of the object in another [AutoCloseableAsync] object scope. Otherwise, an
 * [AutoCloseableAsync] object should be used within the scope of an [AutoCloseableAsync.use] block
 * where the resource is always cleaned up before exiting (even if exceptions are thrown).
 */
interface AutoCloseableAsync {
    /**
     * Close this resource and any underlining resource held by this object. If the closing of
     * underlining resource might fail, attempt to catch and rethrow error once the resource have
     * been released. Implementors must also keep in mind the blocking nature of sub resource
     * releasing and properly delegate such actions with a [kotlinx.coroutines.withContext] block.
     */
    suspend fun close()
}

/**
 * Use an [AutoCloseableAsync] resource within the specified [block], allowing for the
 * [AutoCloseableAsync] to always call [AutoCloseableAsync.close], even if the [block] throws an
 * exception. This is similar to the functionality that [AutoCloseable] provides where the
 * resources are always cleaned up before returning from the function. Note, this does not catch
 * the exception, rather it rethrows after cleaning up resources if an exception was thrown.
 */
suspend inline fun <R, A : AutoCloseableAsync> A.use(block: (A) -> R): R {
    var cause: Throwable? = null
    return try {
        block(this)
    } catch (ex: Throwable) {
        cause = ex
        throw ex
    } finally {
        try {
            close()
        } catch (ex: Throwable) {
            cause?.addSuppressed(ex)
        }
    }
}

/**
 * Use an [AutoCloseableAsync] resource within the specified [block], allowing for the
 * [AutoCloseableAsync] to always call [AutoCloseableAsync.close], even if the [block] throws an
 * exception. This is similar to the functionality that [AutoCloseable] provides where the
 * resources are always cleaned up before returning from the function. Note, this does catch the
 * exception and wraps that is a [Result.failure]. Otherwise, it returns a [Result.success] with
 * the result of [block].
 */
suspend inline fun <R, A : AutoCloseableAsync> A.useCatching(block: (A) -> R): Result<R> {
    var cause: Throwable? = null
    return try {
        Result.success(block(this))
    } catch (ex: Throwable) {
        cause = ex
        Result.failure(ex)
    } finally {
        try {
            close()
        } catch (ex: Throwable) {
            cause?.addSuppressed(ex)
        }
    }
}
