package com.github.clasicrando.common

interface AutoRelease {
    fun release()
}

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
