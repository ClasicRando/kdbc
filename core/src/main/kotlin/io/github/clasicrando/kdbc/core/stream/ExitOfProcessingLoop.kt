package io.github.clasicrando.kdbc.core.stream

/**
 * [Exception] thrown when a processing loop is exited before the expected exit condition is met.
 * This generally happens when the loop is initiated but the parent coroutine/scope is cancelled.
 */
public class ExitOfProcessingLoop(expectedCase: String) :
    EndOfStream(
        "Unexpected exit of processing loop before expected exit condition is met. Waited for " +
            expectedCase
    )
