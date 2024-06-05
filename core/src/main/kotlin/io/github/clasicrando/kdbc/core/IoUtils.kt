package io.github.clasicrando.kdbc.core

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.io.RawSink
import kotlinx.io.RawSource
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import java.nio.file.Files

private val logger = KotlinLogging.logger {}

object IoUtils {
    fun pathExists(path: Path): Boolean {
        return SystemFileSystem.exists(path = path)
    }

    fun createIfNotExists(path: Path) {
        if (pathExists(path = path)) {
            return
        }

        val parent = java.nio.file.Path.of(path.parent!!.toString())
        Files.createDirectories(parent)
        Files.createFile(parent.resolve(path.name))
    }

    fun delete(path: Path, mustExist: Boolean = true) {
        SystemFileSystem.delete(path = path, mustExist = mustExist)
    }

    fun deleteCatching(path: Path, mustExist: Boolean = true) {
        try {
            SystemFileSystem.delete(path = path, mustExist = mustExist)
        } catch (ex: Exception) {
            logger.warn(ex) { "Error while deleting file at $path" }
        }
    }

    fun sink(path: Path, append: Boolean = false): RawSink {
        return SystemFileSystem.sink(path = path, append = append)
    }

    fun source(path: Path): RawSource {
        return SystemFileSystem.source(path = path)
    }
}
