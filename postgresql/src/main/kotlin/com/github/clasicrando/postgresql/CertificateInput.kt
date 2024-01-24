package com.github.clasicrando.postgresql

import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray

sealed interface CertificateInput {
    class Inline(val bytes: ByteArray): com.github.clasicrando.postgresql.CertificateInput
    data class File(val path: Path): com.github.clasicrando.postgresql.CertificateInput

    fun data(): ByteArray {
        return when (this) {
            is com.github.clasicrando.postgresql.CertificateInput.Inline -> this.bytes
            is com.github.clasicrando.postgresql.CertificateInput.File -> SystemFileSystem.source(this.path)
                .buffered()
                .readByteArray()
        }
    }

    companion object {
        private const val BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----"
        private const val END_CERTIFICATE = "-----END CERTIFICATE-----"

        fun fromString(str: String): com.github.clasicrando.postgresql.CertificateInput {
            val trimmed = str.trim()
            return if (trimmed.startsWith(com.github.clasicrando.postgresql.CertificateInput.Companion.BEGIN_CERTIFICATE) && trimmed.contains(
                    com.github.clasicrando.postgresql.CertificateInput.Companion.END_CERTIFICATE
                )) {
                com.github.clasicrando.postgresql.CertificateInput.Inline(str.toByteArray())
            } else {
                com.github.clasicrando.postgresql.CertificateInput.File(Path(str))
            }
        }
    }
}
