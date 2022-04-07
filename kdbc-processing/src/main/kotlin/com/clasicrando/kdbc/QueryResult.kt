package com.clasicrando.kdbc

import kotlin.reflect.KClass

@Target(AnnotationTarget.CLASS)
annotation class QueryResult(val parser: KClass<ResultSetParser<*>> = ResultSetParser::class)
