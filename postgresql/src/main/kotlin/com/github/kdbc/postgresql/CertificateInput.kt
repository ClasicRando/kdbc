package com.github.kdbc.postgresql

import com.github.kdbc.postgresql.CertificateInput.File
import com.github.kdbc.postgresql.CertificateInput.Inline
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray

/**
 * Variants of method to obtain an SSL certificate. The options are either [Inline] as a literal
 * [String] converted to a [ByteArray] or a [File] which contains a [Path] to a certificate file.
 */
sealed interface CertificateInput {
    /** Literal SSL certificate where the certificate data is stored as [bytes] */
    class Inline(val bytes: ByteArray): CertificateInput
    /**
     * File based SSL certificate where the [path] points to the file containing the SSL
     * certificate data
     */
    data class File(val path: Path): CertificateInput

    /**
     * Return the certificate data. If this [CertificateInput] is [File] based, then it will read
     * the file as a [ByteArray]. Otherwise, the [Inline] variant just returns its [Inline.bytes].
     */
    fun data(): ByteArray {
        return when (this) {
            is Inline -> this.bytes
            is File -> SystemFileSystem.source(this.path)
                .buffered()
                .readByteArray()
        }
    }

    companion object {
        private const val BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----"
        private const val END_CERTIFICATE = "-----END CERTIFICATE-----"

        /**
         * Convert this [String] to a [CertificateInput]. If the [str] value looks like a literal
         * certificate then an [Inline] certificate will be returned. Otherwise, it will be
         * interpreted as a [Path] with a [File] certificate returned.
         */
        fun fromString(str: String): CertificateInput {
            val trimmed = str.trim()
            if (trimmed.startsWith(BEGIN_CERTIFICATE) && trimmed.contains(END_CERTIFICATE)) {
                return Inline(str.toByteArray())
            }

            return File(Path(str))
        }
    }
}
