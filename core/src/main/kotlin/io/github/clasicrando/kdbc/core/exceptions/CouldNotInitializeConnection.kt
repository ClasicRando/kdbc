package io.github.clasicrando.kdbc.core.exceptions

public class CouldNotInitializeConnection(connectOptions: String) :
    KdbcException(
        "Could not acquire an initial valid connection using the provided options: $connectOptions"
    )
