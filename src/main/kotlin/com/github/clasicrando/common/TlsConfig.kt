package com.github.clasicrando.common

import com.github.clasicrando.postgresql.CertificateInput

data class TlsConfig(
    val acceptInvalidCerts: Boolean,
    val acceptInvalidHostnames: Boolean,
    val hostName: String,
    val rootCertPath: CertificateInput?,
    val clientCerPath: CertificateInput?,
    val clientKeyPath: CertificateInput,
)
