package com.github.clasicrando.postgresql

import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray

sealed interface CertificateInput {
    class Inline(val bytes: ByteArray): CertificateInput
    data class File(val path: Path): CertificateInput

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

        fun fromString(str: String): CertificateInput {
            val trimmed = str.trim()
            return if (trimmed.startsWith(BEGIN_CERTIFICATE) && trimmed.contains(END_CERTIFICATE)) {
                Inline(str.toByteArray())
            } else {
                File(Path(str))
            }
        }
    }
}
