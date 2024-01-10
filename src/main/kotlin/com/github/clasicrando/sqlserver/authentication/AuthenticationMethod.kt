package com.github.clasicrando.sqlserver.authentication

sealed interface AuthenticationMethod {
    data class SqlServer(val user: String, val password: String) : AuthenticationMethod
    data class Windows(
        val user: String,
        val password: String,
        val domain: String?,
    ) : AuthenticationMethod
    data object Integrated : AuthenticationMethod
    data class AADToken(val token: String) : AuthenticationMethod
    data object None : AuthenticationMethod

    companion object
}
