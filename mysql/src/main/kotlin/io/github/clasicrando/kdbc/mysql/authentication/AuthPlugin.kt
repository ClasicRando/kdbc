package io.github.clasicrando.kdbc.mysql.authentication

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

private const val MY_SQL_NATIVE_PASSWORD = "mysql_native_password"
private const val CACHING_SHA2_PASSWORD = "caching_sha2_password"
private const val SHA256_PASSWORD = "sha256_password"
private const val MY_SQL_CLEAR_PASSWORD = "mysql_clear_password"

public enum class AuthPlugin(public val pluginName: String) {
    MySqlNativePassword(MY_SQL_NATIVE_PASSWORD),
    CachingSha2Password(CACHING_SHA2_PASSWORD),
    Sha256Password(SHA256_PASSWORD),
    MySqlClearPassword(MY_SQL_CLEAR_PASSWORD);

    public companion object {
        public fun fromName(name: String): AuthPlugin =
            when (name) {
                MY_SQL_NATIVE_PASSWORD -> MySqlNativePassword
                CACHING_SHA2_PASSWORD -> CachingSha2Password
                SHA256_PASSWORD -> Sha256Password
                MY_SQL_CLEAR_PASSWORD -> MySqlClearPassword
                else -> throw KdbcException("Unknown auth plugin: '$name'")
            }
    }
}
