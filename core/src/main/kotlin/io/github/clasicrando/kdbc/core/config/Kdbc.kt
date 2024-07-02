package io.github.clasicrando.kdbc.core.config

import io.github.clasicrando.kdbc.core.IOUtils
import io.github.oshai.kotlinlogging.Level
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.readString
import kotlinx.serialization.json.Json

object Kdbc {
    private val options: KdbcOptions by lazy {
        val pathToConfig =
            System
                .getenv("KDBC_CONFIG")
                ?.let { Path(it) }
                ?: Path(".", "kdbc_config.json")
        if (!IOUtils.pathExists(pathToConfig)) {
            return@lazy KdbcOptions(detailedLogging = Level.OFF)
        }
        val jsonData = IOUtils.source(pathToConfig).buffered().readString()
        Json.decodeFromString(jsonData)
    }

    val detailedLogging: Level get() = options.detailedLogging
}
