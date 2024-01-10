package com.github.clasicrando.sqlserver.connection

import kotlinx.io.files.Path

sealed interface TrustConfiguration {
    data class CaCertificateLocation(val path: Path): TrustConfiguration
    data object TrustAll : TrustConfiguration
    data object Default : TrustConfiguration
}
