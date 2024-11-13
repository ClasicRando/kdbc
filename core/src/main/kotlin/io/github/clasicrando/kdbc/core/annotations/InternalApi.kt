package io.github.clasicrando.kdbc.core.annotations

@RequiresOptIn(message = "Internal API. Use at own risk and prefer standard API")
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.FUNCTION)
public annotation class InternalApi
