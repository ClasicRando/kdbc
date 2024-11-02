package io.github.clasicrando.kdbc.core

import kotlin.random.Random
import kotlinx.datetime.Clock

val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
val random = Random(Clock.System.now().epochSeconds)

private fun <T> List<T>.randomItem(): T = this[random.nextInt(0, this.size)]

fun randomString(length: Int = 255): String = buildString {
    for (i in 1..length) {
        append(charPool.randomItem())
    }
}
