package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

private val logger = KotlinLogging.logger {}

inline fun <reified T : Any> pgCompositeDbType(innerTypes: Array<out DbType>): DbType {
    require(T::class.isData) { "Only data classes are available as composites" }
    require(innerTypes.isNotEmpty()) { "Composite must have 1 or more inner types" }
    return PgCompositeDbType(T::class, innerTypes)
}

class PgCompositeDbType<T : Any> @PublishedApi internal constructor(
    private val compositeType: KClass<T>,
    private val innerTypes: Array<out DbType>,
) : DbType {
    private val primaryConstructor = compositeType.primaryConstructor
        ?: error("Composite type must have primary constructor")

    init {
        require(innerTypes.size == primaryConstructor.parameters.size) {
            "Composite's number of innerTypes must match the parameter size in primary constructor"
        }
    }

    internal val properties = primaryConstructor.parameters
        .map { parameter ->
            compositeType.memberProperties.first { it.name == parameter.name }
        }
        .zip(innerTypes)

    override fun decode(type: ColumnData, value: String): Any {
        val attributes = PgCompositeLiteralParser.parse(value)
            .mapIndexed { i, str ->
                if (str == null) {
                    return@mapIndexed null
                }
                innerTypes.getOrNull(i)
                    ?.decode(type, str)
                    ?: columnDecodeError(type, value)
            }
            .toList()
            .toTypedArray()
        return try {
            primaryConstructor.call(*attributes)
        } catch (ex: Throwable) {
            logger.atError {
                message = "Could not construct a new instance of $encodeType using {attributes}"
                cause = ex
                payload = mapOf("attributes" to attributes)
            }
            columnDecodeError(type, value)
        }
    }

    override val encodeType: KClass<*> = compositeType

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        throw NotImplementedError("Composite encoding is not handled in each type instance")
    }
}
