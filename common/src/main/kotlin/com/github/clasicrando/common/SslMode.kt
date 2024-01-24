package com.github.clasicrando.common

enum class SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,;

    fun acceptInvalidCerts(): Boolean = this != com.github.clasicrando.common.SslMode.VerifyCa && this != com.github.clasicrando.common.SslMode.VerifyFull

    fun acceptInvalidHostnames(): Boolean = this != com.github.clasicrando.common.SslMode.VerifyFull

    companion object {
        val DEFAULT = com.github.clasicrando.common.SslMode.Prefer

        fun fromString(str: String): com.github.clasicrando.common.SslMode {
            return when (str.lowercase()) {
                "disable" -> com.github.clasicrando.common.SslMode.Disable
                "allow" -> com.github.clasicrando.common.SslMode.Allow
                "prefer" -> com.github.clasicrando.common.SslMode.Prefer
                "require" -> com.github.clasicrando.common.SslMode.Require
                "verify-ca" -> com.github.clasicrando.common.SslMode.VerifyCa
                "verify-full" -> com.github.clasicrando.common.SslMode.VerifyFull
                else -> error("Unknown value $str for 'ssl_mode'")
            }
        }
    }
}
