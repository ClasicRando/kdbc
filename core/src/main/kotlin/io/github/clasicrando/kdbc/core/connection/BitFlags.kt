package io.github.clasicrando.kdbc.core.connection

/**
 * Bit Flags wrapper type over an [Int]. Input values can be [Byte] and [Short] but the value is
 * always internally stored as an [Int]. When using/writing the bits, use the [asByte], [asShort]
 * or [asInt] to extract the value you need but remember that the internal value is not checked for
 * bits outside the range you request. The output will always truncate other bits.
 */
@JvmInline
public value class IntBitFlags(internal val flags: Int) {
    public constructor(flags: Short): this(flags.toInt() and 0xff_ff)
    public constructor(flags: Byte): this(flags.toInt() and 0xff)

    /**
     * Returns true if the specific status is present in this value (i.e. [Int.and] equals the
     * supplied [mask])
     */
    public operator fun get(mask: IntBitFlags): Boolean {
        return this.flags and mask.flags == mask.flags
    }

    /**
     * Add all bits from the donor [IntBitFlags] while preserving all exists flags. This is
     * equivalent to a [Int.or]
     */
    public operator fun plus(other: IntBitFlags): IntBitFlags {
        return IntBitFlags(this.flags or other.flags)
    }

    /** Remove all bits supplied by settings the bit positions to zero */
    public operator fun minus(status: IntBitFlags): IntBitFlags {
        return IntBitFlags(this.flags and (status.flags.inv()))
    }

    /** `and` these two [IntBitFlags] to get a result that is [Int.and] */
    public infix fun and(status: IntBitFlags): IntBitFlags {
        return IntBitFlags(this.flags and status.flags)
    }

    /**
     * Extract a [Byte] from this bit flags value. This just calls `Int.toByte()` so any bits found
     * to the left of the first 8 are truncated
     */
    public fun asByte(): Byte {
        return flags.toByte()
    }

    /**
     * Extract a [Short] from this bit flags value. This just calls `Int.toShort()` so any bits
     * found to the left of the first 16 are truncated
     */
    public fun asShort(): Short {
        return flags.toShort()
    }

    /** Simply returns the inner [Int] */
    public fun asInt(): Int {
        return flags
    }

    public companion object {
        public val ZERO: IntBitFlags = IntBitFlags(0)
    }
}

/**
 * Bit Flags wrapper type over a [Long]. Input values can be [Byte], [Short], [Int] but the value is
 * always internally stored as an [Long]. When using/writing the bits, use the [lowInt], [highInt]
 * to extract the first 4 and second 4 bits respectively (starting from the right).
 */
@JvmInline
public value class LongBitFlags(internal val flags: Long) {
    public constructor(flags: Int): this(flags.toLong() and 0xff_ff_ff_ff)
    public constructor(flags: Short): this(flags.toLong() and 0xff_ff)
    public constructor(flags: Byte): this(flags.toLong() and 0xff)

    /**
     * Returns true if the specific status is present in this value (i.e. [Int.and] equals the
     * supplied [mask])
     */
    public operator fun get(mask: LongBitFlags): Boolean {
        return this.flags and mask.flags == mask.flags
    }

    /**
     * Add all bits from the donor [LongBitFlags] while preserving all exists flags. This is
     * equivalent to a [Int.or]
     */
    public operator fun plus(other: LongBitFlags): LongBitFlags {
        return LongBitFlags(this.flags or other.flags)
    }

    /** Remove all bits supplied by settings the bit positions to zero */
    public operator fun minus(status: LongBitFlags): LongBitFlags {
        return LongBitFlags(this.flags and (status.flags.inv()))
    }

    /** `and` these two [LongBitFlags] to get a result that is [Int.and] */
    public infix fun and(status: LongBitFlags): LongBitFlags {
        return LongBitFlags(this.flags and status.flags)
    }

    /**
     * Extract the first 32 bits of this bit flags type (starting from the right). Simply calls
     * [Long.toInt].
     */
    public fun lowInt(): Int {
        return flags.toInt()
    }

    /**
     * Extract the last 32 bits of this bit flags type (starting from the right). Shifts the last 32
     * bits over 32 bits then calls [Long.toInt].
     */
    public fun highInt(): Int {
        return (flags shr 32 and 0xff_ff_ff_ff).toInt()
    }

    public companion object {
        public val ZERO: LongBitFlags = LongBitFlags(0)
    }
}
