package com.github.clasicrando.sqlserver.message

import com.github.clasicrando.common.USHORT_ZERO

enum class DoneStatus(private val inner: UShort) {
    More(0b0000000000000001u), // 1 << 0
    Error(0b0000000000000010u), // 1 << 1
    Inexact(0b0000000000000100u), // 1 << 2
    Count(0b0000000000010000u), // 1 << 4,
    Attention(0b0000000000100000u), // 1 << 5,
    RpcInBatch(0b0000000010000000u), // 1 << 7,
    ServerError(0b0000000100000000u), // 1 << 8,
    ;

    companion object {
        fun fromUShort(value: UShort): List<DoneStatus> {
            return DoneStatus.entries.filter { it.inner and value != USHORT_ZERO }
        }
    }
}
