package com.github.clasicrando.sqlserver.column

import com.github.clasicrando.common.USHORT_ZERO

enum class ColumnFlag(private val inner: UShort) {
    Nullable(0b0000000000000001u), // 1 << 0
    CaseSensitive(0b0000000000000010u), // 1 << 1
    Updatable(0b0000000000001000u), // 1 << 3
    UpdatableUnknown(0b0000000000010000u), // 1 << 4,
    Identity(0b0000000000100000u), // 1 << 5,
    Computed(0b0000000010000000u), // 1 << 7,
    FixedLenClrType(0b0000010000000000u), // 1 << 10,
    SparseColumnSet(0b0000100000000000u), // 1 << 11,
    Encrypted(0b0001000000000000u), // 1 << 12,
    Hidden(0b0010000000000000u), // 1 << 13,
    Key(0b0100000000000000u), // 1 << 14,
    NullableUnknown(0b1000000000000000u), // 1 << 15,
    ;

    companion object {
        fun fromUShort(value: UShort): List<ColumnFlag> {
            return ColumnFlag.entries.filter { it.inner and value != USHORT_ZERO }
        }
    }
}
