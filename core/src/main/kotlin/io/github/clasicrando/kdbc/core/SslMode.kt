package io.github.clasicrando.kdbc.core

enum class SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,;

    fun acceptInvalidCerts(): Boolean = this != VerifyCa && this != VerifyFull

    fun acceptInvalidHostnames(): Boolean = this != VerifyFull

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
