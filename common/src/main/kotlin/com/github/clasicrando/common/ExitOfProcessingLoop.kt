package com.github.clasicrando.common

/**
 * [Exception] thrown when a processing loop is exited before the expected exit condition is met.
 * This generally happens when the loop is initiated but the parent coroutine/scope is cancelled.
 */
class ExitOfProcessingLoop(expectedCase: String) : Exception(
    "Unexpected exit of processing loop before expected exit condition is met. Waited for " +
            expectedCase
)