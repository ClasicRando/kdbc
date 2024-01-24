package com.github.clasicrando.common.exceptions

class CouldNotInitializeConnection(connectOptions: String)
    : Throwable(
        "Could not acquire an initial valid connection using the provided options: $connectOptions"
    )