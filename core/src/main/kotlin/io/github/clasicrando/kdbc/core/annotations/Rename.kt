package io.github.clasicrando.kdbc.core.annotations

@Target(AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FIELD)
public annotation class Rename(val value: String)
