package com.github.clasicrando.common

enum class SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,;

    companion object {
        val DEFAULT = Prefer

        fun fromString(str: String): SslMode {
            return when (str.lowercase()) {
                "disable" -> Disable
                "allow" -> Allow
                "prefer" -> Prefer
                "require" -> Require
                "verify-ca" -> VerifyCa
                "verify-full" -> VerifyFull
                else -> error("Unknown value $str for 'ssl_mode'")
            }
        }
    }
}
