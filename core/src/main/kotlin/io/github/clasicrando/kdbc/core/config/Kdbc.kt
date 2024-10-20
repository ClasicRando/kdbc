package io.github.clasicrando.kdbc.core.config

import io.github.clasicrando.kdbc.core.IOUtils
import io.github.oshai.kotlinlogging.Level
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.readString
import kotlinx.serialization.json.Json

object Kdbc {
    private const val ENV_VARIABLE_NAME = "KDBC_CONFIG"
    private const val FILE_NAME = "kdbc_config.json"
    private val options: KdbcOptions by lazy {
        val jsonData =
            System
                .getenv(ENV_VARIABLE_NAME)
                ?.let {
                    val path = Path(it)
                    IOUtils.source(path).buffered().readString()
                }
                ?: Kdbc::class.java
                    .classLoader
                    .getResourceAsStream(FILE_NAME)
                    ?.asSource()
                    ?.buffered()
                    ?.readString()
                ?: Path(".", FILE_NAME)
                    .takeIf { IOUtils.pathExists(it) }
                    ?.let(IOUtils::source)
                    ?.buffered()
                    ?.readString()
                ?: return@lazy KdbcOptions(detailedLogging = Level.OFF)
        Json.decodeFromString(jsonData)
    }

    val detailedLogging: Level get() = options.detailedLogging
}
