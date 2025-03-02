package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.typeOf
import kotlinx.io.Sink
import kotlinx.io.writeString

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the
 * `text`/`name`/`bpchar`/`varchar`/`xml` types in a postgresql database.
 */
internal object VarcharTypeDescription :
    PgTypeDescription<String>(dbType = PgType.Varchar, kType = typeOf<String>()) {
    override fun isCompatible(dbType: PgType): Boolean {
        return dbType.oid == PgType.Text.oid ||
            dbType.oid == PgType.Varchar.oid ||
            dbType.oid == PgType.Xml.oid ||
            dbType.oid == PgType.Name.oid ||
            dbType.oid == PgType.Bpchar.oid
    }

    /** Simply writes the [String] value to the buffer in UTF8 encoding */
    override fun encode(value: String, buffer: Sink) {
        buffer.writeString(value)
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
