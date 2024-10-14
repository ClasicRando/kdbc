package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the
 * `text`/`name`/`bpchar`/`varchar`/`xml` types in a postgresql database.
 */
internal abstract class AbstractVarcharTypeDescription(pgType: PgType) : PgTypeDescription<String>(
    pgType = pgType,
    kType = typeOf<String>(),
) {
    /** Simply writes the [String] value to the buffer in UTF8 encoding */
    override fun encode(value: String, buffer: ByteWriteBuffer) {
        buffer.writeText(value)
    }

    /** Read the bytes as text using UFT8 encoding */
    override fun decodeBytes(value: PgValue.Binary): String {
        return value.bytes.readText()
    }

    /** Return the [PgValue.Text.text] value directly */
    override fun decodeText(value: PgValue.Text): String {
        return value.text
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `text` types in
 * a postgresql database.
 */
internal object TextTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Text)

/**
 * Implementation of an [ArrayTypeDescription] for [String]. This maps to the `text[]` types in a
 * postgresql database.
 */
internal object TextArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.TextArray,
    innerType = TextTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `varchar` types
 * in a postgresql database.
 */
internal object VarcharTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Varchar)

/**
 * Implementation of an [ArrayTypeDescription] for [String]. This maps to the `varchar[]` types in a
 * postgresql database.
 */
internal object VarcharArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.VarcharArray,
    innerType = VarcharTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `xml` types in a
 * postgresql database.
 */
internal object XmlTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Xml)

/**
 * Implementation of an [ArrayTypeDescription] for [String]. This maps to the `xml[]` types in a
 * postgresql database.
 */
internal object XmlArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.XmlArray,
    innerType = XmlTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `name` types in
 * a postgresql database.
 */
internal object NameTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Name)

/**
 * Implementation of an [ArrayTypeDescription] for [String]. This maps to the `name[]` types in a
 * postgresql database.
 */
internal object NameArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.NameArray,
    innerType = TextTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `bpchar` types
 * in a postgresql database.
 */
internal object BpcharTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Bpchar)

/**
 * Implementation of an [ArrayTypeDescription] for [String]. This maps to the `bpchar[]` types in a
 * postgresql database.
 */
internal object BpcharArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.BpcharArray,
    innerType = TextTypeDescription,
)
