package io.github.clasicrando.kdbc.core

public enum class SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull;

    public fun acceptInvalidCerts(): Boolean = this != VerifyCa && this != VerifyFull

    public fun acceptInvalidHostnames(): Boolean = this != VerifyFull

    public companion object {
        public val DEFAULT: SslMode = Prefer

        public fun fromString(str: String): SslMode {
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
