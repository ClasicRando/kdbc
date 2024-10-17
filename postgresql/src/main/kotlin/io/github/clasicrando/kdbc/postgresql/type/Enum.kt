package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnMetadata
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.KType

/** Implementation of [PgTypeDescription] for custom enum types in a postgresql database */
internal class EnumTypeDescription<E : Enum<E>>(
    pgType: PgType,
    kType: KType,
    private val values: Array<E>,
) : PgTypeDescription<E>(
    dbType = pgType,
    kType = kType,
) {
    private val nameMap: Map<E, String>
    internal val entryLookup: Map<String, E>
    init {
        val renameMap: Map<String, String> = values.firstOrNull()
            ?.declaringJavaClass
            ?.fields
            ?.mapNotNull { field ->
                if (!field.isEnumConstant) {
                    return@mapNotNull null
                }
                val renameAnnotation = field.annotations
                    .asSequence()
                    .mapNotNull { it as? Rename }
                    .firstOrNull()
                    ?: return@mapNotNull null
                field.name to renameAnnotation.value
            }
            ?.toMap()
            ?: mapOf()
        nameMap = values.associateWith { renameMap[it.name] ?: it.name }
        entryLookup = nameMap.asSequence()
            .associate { it.value to it.key }
    }
    /**
     * Writes the [Enum.name] property as text to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L179)
     */
    override fun encode(value: E, buffer: ByteWriteBuffer) {
        buffer.writeText(nameMap[value]!!)
    }

    private fun getLabel(text: String, type: ColumnMetadata): E {
        return entryLookup[text]
            ?: columnDecodeError(
                kType = kType,
                type = type,
                reason = "Could not find enum value for '$text'",
            )
    }

    /**
     * Reads all the bytes as a UTF-8 encoded [String]. Then find the enum value that matches that
     * [String] by [Enum.name]. If no match is found, throw a
     * [io.github.clasicrando.kdbc.core.column.ColumnDecodeError].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L155)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if a variant of [E] cannot
     * be found by [Enum.name] from the
     * decoded [String] value
     */
    override fun decodeBytes(value: PgValue.Binary): E {
        return getLabel(value.bytes.readText(), value.typeData)
    }

    /**
     * Use the [String] value to find the enum value that matches that [String] by [Enum.name]. If
     * no match is found, throw a [io.github.clasicrando.kdbc.core.column.ColumnDecodeError].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L221)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if a variant of [E] cannot
     * be found by [Enum.name] from the
     * decoded [String] value
     */
    override fun decodeText(value: PgValue.Text): E {
        return getLabel(value.text, value.typeData)
    }
}

/** Implementation of an [ArrayTypeDescription] for custom enum types in a postgresql database */
internal class EnumArrayTypeDescription<E : Enum<E>>(
    pgType: PgType,
    innerType: EnumTypeDescription<E>,
    innerNullable: Boolean,
) : ArrayTypeDescription<E>(pgType = pgType, innerType = innerType, innerNullable = innerNullable)
