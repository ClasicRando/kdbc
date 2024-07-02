package io.github.clasicrando.kdbc.postgresql

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.io.RawSink
import kotlinx.io.RawSource
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import java.nio.file.Files

private val logger = KotlinLogging.logger {}

/**
 * Helper object for IO related operations. Generally a wrapper for `kotlinx-io` operations but
 * leverages standard `java.nio` operations when the kotlinx implementation does not contain the
 * required behaviour.
 *
 * This exists since the `kotlinx-io` is in flux, and while we want to use multiplatform code where
 * possible, the API is missing some helpful features.
 */
object IOUtils {
    /**
     * Returns true if the [path] exists in the actual file system.
     *
     * @throws kotlinx.io.IOException
     */
    fun pathExists(path: Path): Boolean {
        return SystemFileSystem.exists(path = path)
    }

    /**
     * Creates the file defined by the [path] if it doesn't already exist.
     *
     * @throws kotlinx.io.IOException
     */
    fun createFileIfNotExists(path: Path) {
        if (pathExists(path = path)) {
            return
        }

        val parent = java.nio.file.Path.of(path.parent!!.toString())
        try {
            Files.createDirectories(parent)
            Files.createFile(parent.resolve(path.name))
        } catch (ex: java.io.IOException) {
            throw kotlinx.io.IOException(ex)
        }
    }

    /**
     * Deletes the file/directory specified. Fails if the file/directory does not exist and
     * [mustExist] is true or if the [path] is a directory and that directory is not empty.
     *
     * @throws kotlinx.io.files.FileNotFoundException if the file/directory does not exist and
     * [mustExist] is true
     * @throws kotlinx.io.IOException
     */
    fun delete(path: Path, mustExist: Boolean = true) {
        SystemFileSystem.delete(path = path, mustExist = mustExist)
    }

    /**
     * Executes [delete], catching any [kotlinx.io.IOException] exception and logging as an error.
     * The underlining exception is effectively ignored.
     */
    fun deleteCatching(path: Path, mustExist: Boolean = true) {
        try {
            delete(path = path, mustExist = mustExist)
        } catch (ex: kotlinx.io.IOException) {
            logger.warn(ex) { "Error while deleting file at $path" }
        }
    }

    /**
     * Open the [path] as a writable [RawSink]. If [append] is true, all contents written to the
     * [RawSink] are added to the end of the file.
     */
    fun sink(path: Path, append: Boolean = false): RawSink {
        return SystemFileSystem.sink(path = path, append = append)
    }

    /** Open the [path] as a readable [RawSource] */
    fun source(path: Path): RawSource {
        return SystemFileSystem.source(path = path)
    }
}