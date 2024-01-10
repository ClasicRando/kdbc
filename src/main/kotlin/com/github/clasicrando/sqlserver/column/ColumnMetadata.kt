package com.github.clasicrando.sqlserver.column

import com.github.clasicrando.sqlserver.type.TypeInfo
import com.github.clasicrando.sqlserver.type.VariableLengthType

data class ColumnMetadata(val baseMetadataColumn: BaseMetadataColumn, val name: String) {
    fun nullValue(): ColumnData {
        return when (val type = baseMetadataColumn.type) {
            TypeInfo.FixedLenType.Null -> ColumnData.I32(null)
            TypeInfo.FixedLenType.Int1 -> ColumnData.U8(null)
            TypeInfo.FixedLenType.Bit -> ColumnData.Bit(null)
            TypeInfo.FixedLenType.Int2 -> ColumnData.I16(null)
            TypeInfo.FixedLenType.Int4 -> ColumnData.I32(null)
            TypeInfo.FixedLenType.DateTime4 -> ColumnData.SmallDateTime(null)
            TypeInfo.FixedLenType.Float4 -> ColumnData.F32(null)
            TypeInfo.FixedLenType.Money -> ColumnData.F64(null)
            TypeInfo.FixedLenType.DateTime -> ColumnData.DateTime(null)
            TypeInfo.FixedLenType.Float8 -> ColumnData.F64(null)
            TypeInfo.FixedLenType.Money4 -> ColumnData.F32(null)
            TypeInfo.FixedLenType.Int8 -> ColumnData.I64(null)
            is TypeInfo.VariableSizePrecision -> when (type.type) {
                VariableLengthType.Guid -> ColumnData.Guid(null)
                VariableLengthType.IntN -> when (type.size) {
                    1L -> ColumnData.U8(null)
                    2L -> ColumnData.I16(null)
                    4L -> ColumnData.I32(null)
                    else -> ColumnData.I64(null)
                }
                VariableLengthType.BitN -> ColumnData.Bit(null)
                VariableLengthType.DecimalN -> ColumnData.Numeric(null)
                VariableLengthType.NumericN -> ColumnData.Numeric(null)
                VariableLengthType.FloatN -> when (type.size) {
                    4L -> ColumnData.F32(null)
                    else -> ColumnData.F64(null)
                }
                VariableLengthType.Money -> ColumnData.F64(null)
                VariableLengthType.DateTimeN -> ColumnData.DateTime(null)
                VariableLengthType.BigVarBin -> ColumnData.Binary(null)
                VariableLengthType.BigVarChar -> ColumnData.Str(null)
                VariableLengthType.BigBinary -> ColumnData.Binary(null)
                VariableLengthType.BigChar -> ColumnData.Str(null)
                VariableLengthType.NVarchar -> ColumnData.Str(null)
                VariableLengthType.NChar -> ColumnData.Str(null)
                VariableLengthType.Xml -> ColumnData.Xml(null)
                VariableLengthType.Udt -> error("User defined types are not supported...yet")
                VariableLengthType.Text -> ColumnData.Str(null)
                VariableLengthType.Image -> ColumnData.Binary(null)
                VariableLengthType.NText -> ColumnData.Str(null)
                VariableLengthType.SSVariant -> error("SSVariant type is not supported...yet")
            }
            is TypeInfo.VariableSized -> when (type.type) {
                VariableLengthType.Guid -> ColumnData.Guid(null)
                VariableLengthType.IntN -> when (type.size) {
                    1L -> ColumnData.U8(null)
                    2L -> ColumnData.I16(null)
                    4L -> ColumnData.I32(null)
                    else -> ColumnData.I64(null)
                }
                VariableLengthType.BitN -> ColumnData.Bit(null)
                VariableLengthType.DecimalN -> ColumnData.Numeric(null)
                VariableLengthType.NumericN -> ColumnData.Numeric(null)
                VariableLengthType.FloatN -> when (type.size) {
                    4L -> ColumnData.F32(null)
                    else -> ColumnData.F64(null)
                }
                VariableLengthType.Money -> ColumnData.F64(null)
                VariableLengthType.DateTimeN -> ColumnData.DateTime(null)
                VariableLengthType.BigVarBin -> ColumnData.Binary(null)
                VariableLengthType.BigVarChar -> ColumnData.Str(null)
                VariableLengthType.BigBinary -> ColumnData.Binary(null)
                VariableLengthType.BigChar -> ColumnData.Str(null)
                VariableLengthType.NVarchar -> ColumnData.Str(null)
                VariableLengthType.NChar -> ColumnData.Str(null)
                VariableLengthType.Xml -> ColumnData.Xml(null)
                VariableLengthType.Udt -> error("User defined types are not supported...yet")
                VariableLengthType.Text -> ColumnData.Str(null)
                VariableLengthType.Image -> ColumnData.Binary(null)
                VariableLengthType.NText -> ColumnData.Str(null)
                VariableLengthType.SSVariant -> error("SSVariant type is not supported...yet")
            }
            is TypeInfo.Xml -> ColumnData.Xml(null)
        }
    }
}
