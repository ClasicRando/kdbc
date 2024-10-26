package io.github.clasicrando.kdbc.postgresql.message.information

sealed class SqlState(
    val errorCode: String,
    val conditionName: String,
) {
    data object SuccessfulCompletion : SqlState(SUCCESSFUL_COMPLETION_CODE, "successful_completion")

    data object Warning : SqlState(WARNING_CODE, "warning")

    data object DynamicResultSetsReturned : SqlState(
        DYNAMIC_RESULT_SETS_RETURNED_CODE,
        "dynamic_result_sets_returned",
    )

    data object ImplicitZeroBitPadding : SqlState(
        IMPLICIT_ZERO_BIT_PADDING_CODE,
        "implicit_zero_bit_padding",
    )

    data object NullValueEliminatedInSetFunction :
        SqlState(
            NULL_VALUE_ELIMINATED_IN_SET_FUNCTION_CODE,
            "null_value_eliminated_in_set_function",
        )

    data object PrivilegeNotGranted : SqlState(PRIVILEGE_NOTE_GRANTED, "privilege_not_granted")

    data object PrivilegeNotRevoked : SqlState(PRIVILEGE_NOT_REVOKED, "privilege_not_revoked")

    data object StringDataRightTruncation : SqlState(
        STRING_DATA_RIGHT_TRUNCATION,
        "string_data_right_truncation",
    )

    data object DeprecatedFeature : SqlState(DEPRECATED_FEATURE, "deprecated_feature")

    data object NoData : SqlState(NO_DATA, "no_data")

    data object NoAdditionalDynamicResultSetsReturned :
        SqlState(
            NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED,
            "no_additional_dynamic_result_sets_returned",
        )

    data object SqlStatementNotYetComplete : SqlState(
        SQL_STATEMENT_NOT_YET_COMPLETE,
        "sql_statement_not_yet_complete",
    )

    data object ConnectionException : SqlState(CONNECTION_EXCEPTION, "connection_exception")

    data object ConnectionDoesNotExist : SqlState(
        CONNECTION_DOES_NOT_EXIST,
        "connection_does_not_exist",
    )

    data object ConnectionFailure : SqlState(CONNECTION_FAILURE, "connection_failure")

    data object SqlClientUnableToEstablishSqlConnection :
        SqlState(
            SQL_CLIENT_UNABLE_TO_ESTABLISH_SQL_CONNECTION,
            "sqlclient_unable_to_establish_sqlconnection",
        )

    data object SqlServerRejectedEstablishmentOfSqlConnection :
        SqlState(
            SQL_SERVER_REJECTED_ESTABLISHMENT_OF_SQL_CONNECTION,
            "sqlserver_rejected_establishment_of_sqlconnection",
        )

    data object TransactionResolutionUnknown : SqlState(
        TRANSACTION_RESOLUTION_UNKNOWN,
        "transaction_resolution_unknown",
    )

    data object ProtocolViolation : SqlState(PROTOCOL_VIOLATION, "protocol_violation")

    data object TriggeredActionException : SqlState(
        TRIGGERED_ACTION_EXCEPTION,
        "triggered_action_exception",
    )

    data object FeatureNotSupported : SqlState(FEATURE_NOT_SUPPORTED, "feature_not_supported")

    data object InvalidTransactionInitiation : SqlState(
        INVALID_TRANSACTION_INITIATION,
        "invalid_transaction_initiation",
    )

    data object LocatorException : SqlState(LOCATOR_EXCEPTION, "locator_exception")

    data object InvalidLocatorSpecification : SqlState(
        INVALID_LOCATOR_SPECIFICATION,
        "invalid_locator_specification",
    )

    data object InvalidGrantor : SqlState(INVALID_GRANTOR, "invalid_grantor")

    data object InvalidGrantOperation : SqlState(INVALID_GRANT_OPERATION, "invalid_grant_operation")

    data object InvalidRoleSpecification : SqlState(
        INVALID_ROLE_SPECIFICATION,
        "invalid_role_specification",
    )

    data object DiagnosticsException : SqlState(DIAGNOSTICS_EXCEPTION, "diagnostics_exception")

    data object StackedDiagnosticsAccessedWithoutActiveHandler :
        SqlState(
            STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER,
            "stacked_diagnostics_accessed_without_active_handler",
        )

    data object CaseNotFound : SqlState(CASE_NOT_FOUND, "case_not_found")

    data object CardinalityViolation : SqlState(CARDINALITY_VIOLATION, "cardinality_violation")

    data object DataException : SqlState(DATA_EXCEPTION, "data_exception")

    data object ArraySubscriptError : SqlState(ARRAY_SUBSCRIPT_ERROR, "array_subscript_error")

    data object CharacterNotInRepertoire : SqlState(
        CHARACTER_NOT_IN_REPERTOIRE,
        "character_not_in_repertoire",
    )

    data object DatetimeFieldOverflow : SqlState(DATETIME_FIELD_OVERFLOW, "datetime_field_overflow")

    data object DivisionByZero : SqlState(DIVISION_BY_ZERO, "division_by_zero")

    data object ErrorInAssignment : SqlState(ERROR_IN_ASSIGNMENT, "error_in_assignment")

    data object EscapeCharacterConflict : SqlState(
        ESCAPE_CHARACTER_CONFLICT,
        "escape_character_conflict",
    )

    data object IndicatorOverflow : SqlState(INDICATOR_OVERFLOW, "indicator_overflow")

    data object IntervalFieldOverflow : SqlState(INTERVAL_FIELD_OVERFLOW, "interval_field_overflow")

    data object InvalidArgumentForLogarithm : SqlState(
        INVALID_ARGUMENT_FOR_LOGARITHM,
        "invalid_argument_for_logarithm",
    )

    data object InvalidArgumentForNtileFunction :
        SqlState(INVALID_ARGUMENT_FOR_NTILE_FUNCTION, "invalid_argument_for_ntile_function")

    data object InvalidArgumentForNthValueFunction :
        SqlState(INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION, "invalid_argument_for_nth_value_function")

    data object InvalidArgumentForPowerFunction :
        SqlState(INVALID_ARGUMENT_FOR_POWER_FUNCTION, "invalid_argument_for_power_function")

    data object InvalidArgumentForWidthBucketFunction :
        SqlState(
            INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,
            "invalid_argument_for_width_bucket_function",
        )

    data object InvalidCharacterValueForCast : SqlState(
        INVALID_CHARACTER_VALUE_FOR_CAST,
        "invalid_character_value_for_cast",
    )

    data object InvalidDatetimeFormat : SqlState(INVALID_DATETIME_FORMAT, "invalid_datetime_format")

    data object InvalidEscapeCharacter : SqlState(
        INVALID_ESCAPE_CHARACTER,
        "invalid_escape_character",
    )

    data object InvalidEscapeOctet : SqlState(INVALID_ESCAPE_OCTET, "invalid_escape_octet")

    data object InvalidEscapeSequence : SqlState(INVALID_ESCAPE_SEQUENCE, "invalid_escape_sequence")

    data object NonstandardUseOfEscapeCharacter :
        SqlState(NONSTANDARD_USE_OF_ESCAPE_CHARACTER, "nonstandard_use_of_escape_character")

    data object InvalidIndicatorParameterValue :
        SqlState(INVALID_INDICATOR_PARAMETER_VALUE, "invalid_indicator_parameter_value")

    data object InvalidParameterValue : SqlState(INVALID_PARAMETER_VALUE, "invalid_parameter_value")

    data object InvalidPrecedingOrFollowingSize :
        SqlState(INVALID_PRECEDING_OR_FOLLOWING_SIZE, "invalid_preceding_or_following_size")

    data object InvalidRegularExpression : SqlState(
        INVALID_REGULAR_EXPRESSION,
        "invalid_regular_expression",
    )

    data object InvalidRowCountInLimitClause :
        SqlState(INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, "invalid_row_count_in_limit_clause")

    data object InvalidRowCountInResultOffsetClause :
        SqlState(
            INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
            "invalid_row_count_in_result_offset_clause",
        )

    data object InvalidTableSampleArgument : SqlState(
        INVALID_TABLE_SAMPLE_ARGUMENT,
        "invalid_tablesample_argument",
    )

    data object InvalidTableSampleRepeat : SqlState(
        INVALID_TABLE_SAMPLE_REPEAT,
        "invalid_tablesample_repeat",
    )

    data object InvalidTimeZoneDisplacementValue :
        SqlState(INVALID_TIME_ZONE_DISPLACEMENT_VALUE, "invalid_time_zone_displacement_value")

    data object InvalidUseOfEscapeCharacter : SqlState(
        INVALID_USE_OF_ESCAPE_CHARACTER,
        "invalid_use_of_escape_character",
    )

    data object MostSpecificTypeMismatch : SqlState(
        MOST_SPECIFIC_TYPE_MISMATCH,
        "most_specific_type_mismatch",
    )

    data object NullValueNotAllowed : SqlState(NULL_VALUE_NOT_ALLOWED, "null_value_not_allowed")

    data object NullValueNoIndicatorParameter :
        SqlState(NULL_VALUE_NO_INDICATOR_PARAMETER, "null_value_no_indicator_parameter")

    data object NumericValueOutOfRange : SqlState(
        NUMERIC_VALUE_OUT_OF_RANGE,
        "numeric_value_out_of_range",
    )

    data object SequenceGeneratorLimitExceeded :
        SqlState(SEQUENCE_GENERATOR_LIMIT_EXCEEDED, "sequence_generator_limit_exceeded")

    data object StringDataLengthMismatch : SqlState(
        STRING_DATA_LENGTH_MISMATCH,
        "string_data_length_mismatch",
    )

    data object StringDataRightTruncation2 : SqlState(
        STRING_DATA_RIGHT_TRUNCATION2,
        "string_data_right_truncation",
    )

    data object SubstringError : SqlState(SUBSTRING_ERROR, "substring_error")

    data object TrimError : SqlState(TRIM_ERROR, "trim_error")

    data object UnterminatedCString : SqlState(UNTERMINATED_C_STRING, "unterminated_c_string")

    data object ZeroLengthCharacterString : SqlState(
        ZERO_LENGTH_CHARACTER_STRING,
        "zero_length_character_string",
    )

    data object FloatingPointException : SqlState(
        FLOATING_POINT_EXCEPTION,
        "floating_point_exception",
    )

    data object InvalidTextRepresentation : SqlState(
        INVALID_TEXT_REPRESENTATION,
        "invalid_text_representation",
    )

    data object InvalidBinaryRepresentation : SqlState(
        INVALID_BINARY_REPRESENTATION,
        "invalid_binary_representation",
    )

    data object BadCopyFileFormat : SqlState(BAD_COPY_FILE_FORMAT, "bad_copy_file_format")

    data object UntranslatableCharacter : SqlState(
        UNTRANSLATABLE_CHARACTER,
        "untranslatable_character",
    )

    data object NotAnXmlDocument : SqlState(NOT_AN_XML_DOCUMENT, "not_an_xml_document")

    data object InvalidXmlDocument : SqlState(INVALID_XML_DOCUMENT, "invalid_xml_document")

    data object InvalidXmlContent : SqlState(INVALID_XML_CONTENT, "invalid_xml_content")

    data object InvalidXmlComment : SqlState(INVALID_XML_COMMENT, "invalid_xml_comment")

    data object InvalidXmlProcessingInstruction :
        SqlState(INVALID_XML_PROCESSING_INSTRUCTION, "invalid_xml_processing_instruction")

    data object DuplicateJsonObjectKeyValue : SqlState(
        DUPLICATE_JSON_OBJECT_KEY_VALUE,
        "duplicate_json_object_key_value",
    )

    data object InvalidArgumentForSqlJsonDatetimeFunction :
        SqlState(
            INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION,
            "invalid_argument_for_sql_json_datetime_function",
        )

    data object InvalidJsonText : SqlState(INVALID_JSON_TEXT, "invalid_json_text")

    data object InvalidSqlJsonSubscript : SqlState(
        INVALID_SQL_JSON_SUBSCRIPT,
        "invalid_sql_json_subscript",
    )

    data object MoreThanOneSqlJsonItem : SqlState(
        MORE_THAN_ONE_SQL_JSON_ITEM,
        "more_than_one_sql_json_item",
    )

    data object NoSqlJsonItem : SqlState(NO_SQL_JSON_ITEM, "no_sql_json_item")

    data object NonNumericSqlJsonItem : SqlState(
        NON_NUMERIC_SQL_JSON_ITEM,
        "non_numeric_sql_json_item",
    )

    data object NonUniqueKeysInAJsonObject : SqlState(
        NON_UNIQUE_KEYS_IN_A_JSON_OBJECT,
        "non_unique_keys_in_a_json_object",
    )

    data object SingletonSqlJsonItemRequired : SqlState(
        SINGLETON_SQL_JSON_ITEM_REQUIRED,
        "singleton_sql_json_item_required",
    )

    data object SqlJsonArrayNotFound : SqlState(
        SQL_JSON_ARRAY_NOT_FOUND,
        "sql_json_array_not_found",
    )

    data object SqlJsonMemberNotFound : SqlState(
        SQL_JSON_MEMBER_NOT_FOUND,
        "sql_json_member_not_found",
    )

    data object SqlJsonNumberNotFound : SqlState(
        SQL_JSON_NUMBER_NOT_FOUND,
        "sql_json_number_not_found",
    )

    data object SqlJsonObjectNotFound : SqlState(
        SQL_JSON_OBJECT_NOT_FOUND,
        "sql_json_object_not_found",
    )

    data object TooManyJsonArrayElements : SqlState(
        TOO_MANY_JSON_ARRAY_ELEMENTS,
        "too_many_json_array_elements",
    )

    data object TooManyJsonObjectMembers : SqlState(
        TOO_MANY_JSON_OBJECT_MEMBERS,
        "too_many_json_object_members",
    )

    data object SqlJsonScalarRequired : SqlState(
        SQL_JSON_SCALAR_REQUIRED,
        "sql_json_scalar_required",
    )

    data object SqlJsonItemCannotBeCastToTargetType :
        SqlState(
            SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE,
            "sql_json_item_cannot_be_cast_to_target_type",
        )

    data object IntegrityConstraintViolation : SqlState(
        INTEGRITY_CONSTRAINT_VIOLATION,
        "integrity_constraint_violation",
    )

    data object RestrictViolation : SqlState(RESTRICT_VIOLATION, "restrict_violation")

    data object NotNullViolation : SqlState(NOT_NULL_VIOLATION, "not_null_violation")

    data object ForeignKeyViolation : SqlState(FOREIGN_KEY_VIOLATION, "foreign_key_violation")

    data object UniqueViolation : SqlState(UNIQUE_VIOLATION, "unique_violation")

    data object CheckViolation : SqlState(CHECK_VIOLATION, "check_violation")

    data object ExclusionViolation : SqlState(EXCLUSION_VIOLATION, "exclusion_violation")

    data object InvalidCursorState : SqlState(INVALID_CURSOR_STATE, "invalid_cursor_state")

    data object InvalidTransactionState : SqlState(
        INVALID_TRANSACTION_STATE,
        "invalid_transaction_state",
    )

    data object ActiveSqlTransaction : SqlState(ACTIVE_SQL_TRANSACTION, "active_sql_transaction")

    data object BranchTransactionAlreadyActive :
        SqlState(BRANCH_TRANSACTION_ALREADY_ACTIVE, "branch_transaction_already_active")

    data object HeldCursorRequiresSameIsolationLevel :
        SqlState(
            HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL,
            "held_cursor_requires_same_isolation_level",
        )

    data object InappropriateAccessModeForBranchTransaction :
        SqlState(
            INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION,
            "inappropriate_access_mode_for_branch_transaction",
        )

    data object InappropriateIsolationLevelForBranchTransaction :
        SqlState(
            INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION,
            "inappropriate_isolation_level_for_branch_transaction",
        )

    data object NoActiveSqlTransactionForBranchTransaction :
        SqlState(
            NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION,
            "no_active_sql_transaction_for_branch_transaction",
        )

    data object ReadOnlySqlTransaction : SqlState(
        READ_ONLY_SQL_TRANSACTION,
        "read_only_sql_transaction",
    )

    data object SchemaAndDataStatementMixingNotSupported :
        SqlState(
            SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED,
            "schema_and_data_statement_mixing_not_supported",
        )

    data object NoActiveSqlTransaction : SqlState(
        NO_ACTIVE_SQL_TRANSACTION,
        "no_active_sql_transaction",
    )

    data object InFailedSqlTransaction : SqlState(
        IN_FAILED_SQL_TRANSACTION,
        "in_failed_sql_transaction",
    )

    data object IdleInTransactionSessionTimeout :
        SqlState(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, "idle_in_transaction_session_timeout")

    data object InvalidSqlStatementName : SqlState(
        INVALID_SQL_STATEMENT_NAME,
        "invalid_sql_statement_name",
    )

    data object TriggeredDataChangeViolation : SqlState(
        TRIGGERED_DATA_CHANGE_VIOLATION,
        "triggered_data_change_violation",
    )

    data object InvalidAuthorizationSpecification :
        SqlState(INVALID_AUTHORIZATION_SPECIFICATION, "invalid_authorization_specification")

    data object InvalidPassword : SqlState(INVALID_PASSWORD, "invalid_password")

    data object DependentPrivilegeDescriptorsStillExist :
        SqlState(
            DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST,
            "dependent_privilege_descriptors_still_exist",
        )

    data object DependentObjectsStillExist : SqlState(
        DEPENDENT_OBJECTS_STILL_EXIST,
        "dependent_objects_still_exist",
    )

    data object InvalidTransactionTermination : SqlState(
        INVALID_TRANSACTION_TERMINATION,
        "invalid_transaction_termination",
    )

    data object SqlRoutineException : SqlState(SQL_ROUTINE_EXCEPTION, "sql_routine_exception")

    data object FunctionExecutedNoReturnStatement :
        SqlState(FUNCTION_EXECUTED_NO_RETURN_STATEMENT, "function_executed_no_return_statement")

    data object ModifyingSqlDataNotPermitted : SqlState(
        MODIFYING_SQL_DATA_NOT_PERMITTED,
        "modifying_sql_data_not_permitted",
    )

    data object ProhibitedSqlStatementAttempted :
        SqlState(PROHIBITED_SQL_STATEMENT_ATTEMPTED, "prohibited_sql_statement_attempted")

    data object ReadingSqlDataNotPermitted : SqlState(
        READING_SQL_DATA_NOT_PERMITTED,
        "reading_sql_data_not_permitted",
    )

    data object InvalidCursorName : SqlState(INVALID_CURSOR_NAME, "invalid_cursor_name")

    data object ExternalRoutineException : SqlState(
        EXTERNAL_ROUTINE_EXCEPTION,
        "external_routine_exception",
    )

    data object ContainingSqlNotPermitted : SqlState(
        CONTAINING_SQL_NOT_PERMITTED,
        "containing_sql_not_permitted",
    )

    data object ModifyingSqlDataNotPermitted2 :
        SqlState(MODIFYING_SQL_DATA_NOT_PERMITTED2, "modifying_sql_data_not_permitted")

    data object ProhibitedSqlStatementAttempted2 :
        SqlState(PROHIBITED_SQL_STATEMENT_ATTEMPTED2, "prohibited_sql_statement_attempted")

    data object ReadingSqlDataNotPermitted2 : SqlState(
        READING_SQL_DATA_NOT_PERMITTED2,
        "reading_sql_data_not_permitted",
    )

    data object ExternalRoutineInvocationException :
        SqlState(EXTERNAL_ROUTINE_INVOCATION_EXCEPTION, "external_routine_invocation_exception")

    data object InvalidSqlstateReturned : SqlState(
        INVALID_SQLSTATE_RETURNED,
        "invalid_sqlstate_returned",
    )

    data object NullValueNotAllowed2 : SqlState(NULL_VALUE_NOT_ALLOWED2, "null_value_not_allowed")

    data object TriggerProtocolViolated : SqlState(
        TRIGGER_PROTOCOL_VIOLATED,
        "trigger_protocol_violated",
    )

    data object SrfProtocolViolated : SqlState(SRF_PROTOCOL_VIOLATED, "srf_protocol_violated")

    data object EventTriggerProtocolViolated : SqlState(
        EVENT_TRIGGER_PROTOCOL_VIOLATED,
        "event_trigger_protocol_violated",
    )

    data object SavepointException : SqlState(SAVEPOINT_EXCEPTION, "savepoint_exception")

    data object InvalidSavepointSpecification : SqlState(
        INVALID_SAVEPOINT_SPECIFICATION,
        "invalid_savepoint_specification",
    )

    data object InvalidCatalogName : SqlState(INVALID_CATALOG_NAME, "invalid_catalog_name")

    data object InvalidSchemaName : SqlState(INVALID_SCHEMA_NAME, "invalid_schema_name")

    data object TransactionRollback : SqlState(TRANSACTION_ROLLBACK, "transaction_rollback")

    data object TransactionIntegrityConstraintViolation :
        SqlState(
            TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION,
            "transaction_integrity_constraint_violation",
        )

    data object SerializationFailure : SqlState(SERIALIZATION_FAILURE, "serialization_failure")

    data object StatementCompletionUnknown : SqlState(
        STATEMENT_COMPLETION_UNKNOWN,
        "statement_completion_unknown",
    )

    data object DeadlockDetected : SqlState(DEADLOCK_DETECTED, "deadlock_detected")

    data object SyntaxErrorOrAccessRuleViolation :
        SqlState(SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "syntax_error_or_access_rule_violation")

    data object SyntaxError : SqlState(SYNTAX_ERROR, "syntax_error")

    data object InsufficientPrivilege : SqlState(INSUFFICIENT_PRIVILEGE, "insufficient_privilege")

    data object CannotCoerce : SqlState(CANNOT_COERCE, "cannot_coerce")

    data object GroupingError : SqlState(GROUPING_ERROR, "grouping_error")

    data object WindowingError : SqlState(WINDOWING_ERROR, "windowing_error")

    data object InvalidRecursion : SqlState(INVALID_RECURSION, "invalid_recursion")

    data object InvalidForeignKey : SqlState(INVALID_FOREIGN_KEY, "invalid_foreign_key")

    data object InvalidName : SqlState(INVALID_NAME, "invalid_name")

    data object NameTooLong : SqlState(NAME_TOO_LONG, "name_too_long")

    data object ReservedName : SqlState(RESERVED_NAME, "reserved_name")

    data object DatatypeMismatch : SqlState(DATATYPE_MISMATCH, "datatype_mismatch")

    data object IndeterminateDatatype : SqlState(INDETERMINATE_DATATYPE, "indeterminate_datatype")

    data object CollationMismatch : SqlState(COLLATION_MISMATCH, "collation_mismatch")

    data object IndeterminateCollation : SqlState(
        INDETERMINATE_COLLATION,
        "indeterminate_collation",
    )

    data object WrongObjectType : SqlState(WRONG_OBJECT_TYPE, "wrong_object_type")

    data object GeneratedAlways : SqlState(GENERATED_ALWAYS, "generated_always")

    data object UndefinedColumn : SqlState(UNDEFINED_COLUMN, "undefined_column")

    data object UndefinedFunction : SqlState(UNDEFINED_FUNCTION, "undefined_function")

    data object UndefinedTable : SqlState(UNDEFINED_TABLE, "undefined_table")

    data object UndefinedParameter : SqlState(UNDEFINED_PARAMETER, "undefined_parameter")

    data object UndefinedObject : SqlState(UNDEFINED_OBJECT, "undefined_object")

    data object DuplicateColumn : SqlState(DUPLICATE_COLUMN, "duplicate_column")

    data object DuplicateCursor : SqlState(DUPLICATE_CURSOR, "duplicate_cursor")

    data object DuplicateDatabase : SqlState(DUPLICATE_DATABASE, "duplicate_database")

    data object DuplicateFunction : SqlState(DUPLICATE_FUNCTION, "duplicate_function")

    data object DuplicatePreparedStatement : SqlState(
        DUPLICATE_PREPARED_STATEMENT,
        "duplicate_prepared_statement",
    )

    data object DuplicateSchema : SqlState(DUPLICATE_SCHEMA, "duplicate_schema")

    data object DuplicateTable : SqlState(DUPLICATE_TABLE, "duplicate_table")

    data object DuplicateAlias : SqlState(DUPLICATE_ALIAS, "duplicate_alias")

    data object DuplicateObject : SqlState(DUPLICATE_OBJECT, "duplicate_object")

    data object AmbiguousColumn : SqlState(AMBIGUOUS_COLUMN, "ambiguous_column")

    data object AmbiguousFunction : SqlState(AMBIGUOUS_FUNCTION, "ambiguous_function")

    data object AmbiguousParameter : SqlState(AMBIGUOUS_PARAMETER, "ambiguous_parameter")

    data object AmbiguousAlias : SqlState(AMBIGUOUS_ALIAS, "ambiguous_alias")

    data object InvalidColumnReference : SqlState(
        INVALID_COLUMN_REFERENCE,
        "invalid_column_reference",
    )

    data object InvalidColumnDefinition : SqlState(
        INVALID_COLUMN_DEFINITION,
        "invalid_column_definition",
    )

    data object InvalidCursorDefinition : SqlState(
        INVALID_CURSOR_DEFINITION,
        "invalid_cursor_definition",
    )

    data object InvalidDatabaseDefinition : SqlState(
        INVALID_DATABASE_DEFINITION,
        "invalid_database_definition",
    )

    data object InvalidFunctionDefinition : SqlState(
        INVALID_FUNCTION_DEFINITION,
        "invalid_function_definition",
    )

    data object InvalidPreparedStatementDefinition :
        SqlState(INVALID_PREPARED_STATEMENT_DEFINITION, "invalid_prepared_statement_definition")

    data object InvalidSchemaDefinition : SqlState(
        INVALID_SCHEMA_DEFINITION,
        "invalid_schema_definition",
    )

    data object InvalidTableDefinition : SqlState(
        INVALID_TABLE_DEFINITION,
        "invalid_table_definition",
    )

    data object InvalidObjectDefinition : SqlState(
        INVALID_OBJECT_DEFINITION,
        "invalid_object_definition",
    )

    data object WithCheckOptionViolation : SqlState(
        WITH_CHECK_OPTION_VIOLATION,
        "with_check_option_violation",
    )

    data object InsufficientResources : SqlState(INSUFFICIENT_RESOURCES, "insufficient_resources")

    data object DiskFull : SqlState(DISK_FULL, "disk_full")

    data object OutOfMemory : SqlState(OUT_OF_MEMORY, "out_of_memory")

    data object TooManyConnections : SqlState(TOO_MANY_CONNECTIONS, "too_many_connections")

    data object ConfigurationLimitExceeded : SqlState(
        CONFIGURATION_LIMIT_EXCEEDED,
        "configuration_limit_exceeded",
    )

    data object ProgramLimitExceeded : SqlState(PROGRAM_LIMIT_EXCEEDED, "program_limit_exceeded")

    data object StatementTooComplex : SqlState(STATEMENT_TOO_COMPLEX, "statement_too_complex")

    data object TooManyColumns : SqlState(TOO_MANY_COLUMNS, "too_many_columns")

    data object TooManyArguments : SqlState(TOO_MANY_ARGUMENTS, "too_many_arguments")

    data object ObjectNotInPrerequisiteState : SqlState(
        OBJECT_NOT_IN_PREREQUISITE_STATE,
        "object_not_in_prerequisite_state",
    )

    data object ObjectInUse : SqlState(OBJECT_IN_USE, "object_in_use")

    data object CantChangeRuntimeParam : SqlState(
        CANT_CHANGE_RUNTIME_PARAM,
        "cant_change_runtime_param",
    )

    data object LockNotAvailable : SqlState(LOCK_NOT_AVAILABLE, "lock_not_available")

    data object UnsafeNewEnumValueUsage : SqlState(
        UNSAFE_NEW_ENUM_VALUE_USAGE,
        "unsafe_new_enum_value_usage",
    )

    data object OperatorIntervention : SqlState(OPERATOR_INTERVENTION, "operator_intervention")

    data object QueryCanceled : SqlState(QUERY_CANCELED, "query_canceled")

    data object AdminShutdown : SqlState(ADMIN_SHUTDOWN, "admin_shutdown")

    data object CrashShutdown : SqlState(CRASH_SHUTDOWN, "crash_shutdown")

    data object CannotConnectNow : SqlState(CANNOT_CONNECT_NOW, "cannot_connect_now")

    data object DatabaseDropped : SqlState(DATABASE_DROPPED, "database_dropped")

    data object IdleSessionTimeout : SqlState(IDLE_SESSION_TIMEOUT, "idle_session_timeout")

    data object SystemError : SqlState(SYSTEM_ERROR, "system_error")

    data object IoError : SqlState(IO_ERROR, "io_error")

    data object UndefinedFile : SqlState(UNDEFINED_FILE, "undefined_file")

    data object DuplicateFile : SqlState(DUPLICATE_FILE, "duplicate_file")

    data object SnapshotTooOld : SqlState(SNAPSHOT_TOO_OLD, "snapshot_too_old")

    class Unknown(
        code: String,
    ) : SqlState(code, "Unknown code") {
        override fun toString(): String = "Unknown(code=$errorCode)"
    }

    companion object {
        private const val SUCCESSFUL_COMPLETION_CODE = "00000"
        private const val WARNING_CODE = "01000"
        private const val DYNAMIC_RESULT_SETS_RETURNED_CODE = "0100C"
        private const val IMPLICIT_ZERO_BIT_PADDING_CODE = "01008"
        private const val NULL_VALUE_ELIMINATED_IN_SET_FUNCTION_CODE = "01003"
        private const val PRIVILEGE_NOTE_GRANTED = "01007"
        private const val PRIVILEGE_NOT_REVOKED = "01006"
        private const val STRING_DATA_RIGHT_TRUNCATION = "01004"
        private const val DEPRECATED_FEATURE = "01P01"
        private const val NO_DATA = "02000"
        private const val NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED = "02001"
        private const val SQL_STATEMENT_NOT_YET_COMPLETE = "03000"
        private const val CONNECTION_EXCEPTION = "08000"
        private const val CONNECTION_DOES_NOT_EXIST = "08003"
        private const val CONNECTION_FAILURE = "08006"
        private const val SQL_CLIENT_UNABLE_TO_ESTABLISH_SQL_CONNECTION = "08001"
        private const val SQL_SERVER_REJECTED_ESTABLISHMENT_OF_SQL_CONNECTION = "08004"
        private const val TRANSACTION_RESOLUTION_UNKNOWN = "08007"
        private const val PROTOCOL_VIOLATION = "08P01"
        private const val TRIGGERED_ACTION_EXCEPTION = "09000"
        private const val FEATURE_NOT_SUPPORTED = "0A000"
        private const val INVALID_TRANSACTION_INITIATION = "0B000"
        private const val LOCATOR_EXCEPTION = "0F000"
        private const val INVALID_LOCATOR_SPECIFICATION = "0F001"
        private const val INVALID_GRANTOR = "0L000"
        private const val INVALID_GRANT_OPERATION = "0LP01"
        private const val INVALID_ROLE_SPECIFICATION = "0P000"
        private const val DIAGNOSTICS_EXCEPTION = "0Z000"
        private const val STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER = "0Z002"
        private const val CASE_NOT_FOUND = "20000"
        private const val CARDINALITY_VIOLATION = "21000"
        private const val DATA_EXCEPTION = "22000"
        private const val ARRAY_SUBSCRIPT_ERROR = "2202E"
        private const val CHARACTER_NOT_IN_REPERTOIRE = "22021"
        private const val DATETIME_FIELD_OVERFLOW = "22008"
        private const val DIVISION_BY_ZERO = "22012"
        private const val ERROR_IN_ASSIGNMENT = "22005"
        private const val ESCAPE_CHARACTER_CONFLICT = "2200B"
        private const val INDICATOR_OVERFLOW = "22022"
        private const val INTERVAL_FIELD_OVERFLOW = "22015"
        private const val INVALID_ARGUMENT_FOR_LOGARITHM = "2201E"
        private const val INVALID_ARGUMENT_FOR_NTILE_FUNCTION = "22014"
        private const val INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION = "22016"
        private const val INVALID_ARGUMENT_FOR_POWER_FUNCTION = "2201F"
        private const val INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION = "2201G"
        private const val INVALID_CHARACTER_VALUE_FOR_CAST = "22018"
        private const val INVALID_DATETIME_FORMAT = "22007"
        private const val INVALID_ESCAPE_CHARACTER = "22019"
        private const val INVALID_ESCAPE_OCTET = "2200D"
        private const val INVALID_ESCAPE_SEQUENCE = "22025"
        private const val NONSTANDARD_USE_OF_ESCAPE_CHARACTER = "22P06"
        private const val INVALID_INDICATOR_PARAMETER_VALUE = "22010"
        private const val INVALID_PARAMETER_VALUE = "22023"
        private const val INVALID_PRECEDING_OR_FOLLOWING_SIZE = "22013"
        private const val INVALID_REGULAR_EXPRESSION = "2201B"
        private const val INVALID_ROW_COUNT_IN_LIMIT_CLAUSE = "2201W"
        private const val INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE = "2201X"
        private const val INVALID_TABLE_SAMPLE_ARGUMENT = "2202H"
        private const val INVALID_TABLE_SAMPLE_REPEAT = "2202G"
        private const val INVALID_TIME_ZONE_DISPLACEMENT_VALUE = "22009"
        private const val INVALID_USE_OF_ESCAPE_CHARACTER = "2200C"
        private const val MOST_SPECIFIC_TYPE_MISMATCH = "2200G"
        private const val NULL_VALUE_NOT_ALLOWED = "22004"
        private const val NULL_VALUE_NO_INDICATOR_PARAMETER = "22002"
        private const val NUMERIC_VALUE_OUT_OF_RANGE = "22003"
        private const val SEQUENCE_GENERATOR_LIMIT_EXCEEDED = "2200H"
        private const val STRING_DATA_LENGTH_MISMATCH = "22026"
        private const val STRING_DATA_RIGHT_TRUNCATION2 = "22001"
        private const val SUBSTRING_ERROR = "22011"
        private const val TRIM_ERROR = "22027"
        private const val UNTERMINATED_C_STRING = "22024"
        private const val ZERO_LENGTH_CHARACTER_STRING = "2200F"
        private const val FLOATING_POINT_EXCEPTION = "22P01"
        private const val INVALID_TEXT_REPRESENTATION = "22P02"
        private const val INVALID_BINARY_REPRESENTATION = "22P03"
        private const val BAD_COPY_FILE_FORMAT = "22P04"
        private const val UNTRANSLATABLE_CHARACTER = "22P05"
        private const val NOT_AN_XML_DOCUMENT = "2200L"
        private const val INVALID_XML_DOCUMENT = "2200M"
        private const val INVALID_XML_CONTENT = "2200N"
        private const val INVALID_XML_COMMENT = "2200S"
        private const val INVALID_XML_PROCESSING_INSTRUCTION = "2200T"
        private const val DUPLICATE_JSON_OBJECT_KEY_VALUE = "22030"
        private const val INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION = "22031"
        private const val INVALID_JSON_TEXT = "22032"
        private const val INVALID_SQL_JSON_SUBSCRIPT = "22033"
        private const val MORE_THAN_ONE_SQL_JSON_ITEM = "22034"
        private const val NO_SQL_JSON_ITEM = "22035"
        private const val NON_NUMERIC_SQL_JSON_ITEM = "22036"
        private const val NON_UNIQUE_KEYS_IN_A_JSON_OBJECT = "22037"
        private const val SINGLETON_SQL_JSON_ITEM_REQUIRED = "22038"
        private const val SQL_JSON_ARRAY_NOT_FOUND = "22039"
        private const val SQL_JSON_MEMBER_NOT_FOUND = "2203A"
        private const val SQL_JSON_NUMBER_NOT_FOUND = "2203B"
        private const val SQL_JSON_OBJECT_NOT_FOUND = "2203C"
        private const val TOO_MANY_JSON_ARRAY_ELEMENTS = "2203D"
        private const val TOO_MANY_JSON_OBJECT_MEMBERS = "2203E"
        private const val SQL_JSON_SCALAR_REQUIRED = "2203F"
        private const val SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE = "2203G"
        private const val INTEGRITY_CONSTRAINT_VIOLATION = "23000"
        private const val RESTRICT_VIOLATION = "23001"
        private const val NOT_NULL_VIOLATION = "23502"
        private const val FOREIGN_KEY_VIOLATION = "23503"
        private const val UNIQUE_VIOLATION = "23505"
        private const val CHECK_VIOLATION = "23514"
        private const val EXCLUSION_VIOLATION = "23P01"
        private const val INVALID_CURSOR_STATE = "24000"
        private const val INVALID_TRANSACTION_STATE = "25000"
        private const val ACTIVE_SQL_TRANSACTION = "25001"
        private const val BRANCH_TRANSACTION_ALREADY_ACTIVE = "25002"
        private const val HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL = "25008"
        private const val INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION = "25003"
        private const val INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION = "25004"
        private const val NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION = "25005"
        private const val READ_ONLY_SQL_TRANSACTION = "25006"
        private const val SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED = "25007"
        private const val NO_ACTIVE_SQL_TRANSACTION = "25P01"
        private const val IN_FAILED_SQL_TRANSACTION = "25P02"
        private const val IDLE_IN_TRANSACTION_SESSION_TIMEOUT = "25P03"
        private const val INVALID_SQL_STATEMENT_NAME = "26000"
        private const val TRIGGERED_DATA_CHANGE_VIOLATION = "27000"
        private const val INVALID_AUTHORIZATION_SPECIFICATION = "28000"
        private const val INVALID_PASSWORD = "28P01"
        private const val DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST = "2B000"
        private const val DEPENDENT_OBJECTS_STILL_EXIST = "2BP01"
        private const val INVALID_TRANSACTION_TERMINATION = "2D000"
        private const val SQL_ROUTINE_EXCEPTION = "2F000"
        private const val FUNCTION_EXECUTED_NO_RETURN_STATEMENT = "2F005"
        private const val MODIFYING_SQL_DATA_NOT_PERMITTED = "2F002"
        private const val PROHIBITED_SQL_STATEMENT_ATTEMPTED = "2F003"
        private const val READING_SQL_DATA_NOT_PERMITTED = "2F004"
        private const val INVALID_CURSOR_NAME = "34000"
        private const val EXTERNAL_ROUTINE_EXCEPTION = "38000"
        private const val CONTAINING_SQL_NOT_PERMITTED = "38001"
        private const val MODIFYING_SQL_DATA_NOT_PERMITTED2 = "38002"
        private const val PROHIBITED_SQL_STATEMENT_ATTEMPTED2 = "38003"
        private const val READING_SQL_DATA_NOT_PERMITTED2 = "38004"
        private const val EXTERNAL_ROUTINE_INVOCATION_EXCEPTION = "39000"
        private const val INVALID_SQLSTATE_RETURNED = "39001"
        private const val NULL_VALUE_NOT_ALLOWED2 = "39004"
        private const val TRIGGER_PROTOCOL_VIOLATED = "39P01"
        private const val SRF_PROTOCOL_VIOLATED = "39P02"
        private const val EVENT_TRIGGER_PROTOCOL_VIOLATED = "39P03"
        private const val SAVEPOINT_EXCEPTION = "3B000"
        private const val INVALID_SAVEPOINT_SPECIFICATION = "3B001"
        private const val INVALID_CATALOG_NAME = "3D000"
        private const val INVALID_SCHEMA_NAME = "3F000"
        private const val TRANSACTION_ROLLBACK = "40000"
        private const val TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION = "40002"
        private const val SERIALIZATION_FAILURE = "40001"
        private const val STATEMENT_COMPLETION_UNKNOWN = "40003"
        private const val DEADLOCK_DETECTED = "40P01"
        private const val SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION = "42000"
        private const val SYNTAX_ERROR = "42601"
        private const val INSUFFICIENT_PRIVILEGE = "42501"
        private const val CANNOT_COERCE = "42846"
        private const val GROUPING_ERROR = "42803"
        private const val WINDOWING_ERROR = "42P20"
        private const val INVALID_RECURSION = "42P19"
        private const val INVALID_FOREIGN_KEY = "42830"
        private const val INVALID_NAME = "42602"
        private const val NAME_TOO_LONG = "42622"
        private const val RESERVED_NAME = "42939"
        private const val DATATYPE_MISMATCH = "42804"
        private const val INDETERMINATE_DATATYPE = "42P18"
        private const val COLLATION_MISMATCH = "42P21"
        private const val INDETERMINATE_COLLATION = "42P22"
        private const val WRONG_OBJECT_TYPE = "42809"
        private const val GENERATED_ALWAYS = "428C9"
        private const val UNDEFINED_COLUMN = "42703"
        private const val UNDEFINED_FUNCTION = "42883"
        private const val UNDEFINED_TABLE = "42P01"
        private const val UNDEFINED_PARAMETER = "42P02"
        private const val UNDEFINED_OBJECT = "42704"
        private const val DUPLICATE_COLUMN = "42701"
        private const val DUPLICATE_CURSOR = "42P03"
        private const val DUPLICATE_DATABASE = "42P04"
        private const val DUPLICATE_FUNCTION = "42723"
        private const val DUPLICATE_PREPARED_STATEMENT = "42P05"
        private const val DUPLICATE_SCHEMA = "42P06"
        private const val DUPLICATE_TABLE = "42P07"
        private const val DUPLICATE_ALIAS = "42712"
        private const val DUPLICATE_OBJECT = "42710"
        private const val AMBIGUOUS_COLUMN = "42702"
        private const val AMBIGUOUS_FUNCTION = "42725"
        private const val AMBIGUOUS_PARAMETER = "42P08"
        private const val AMBIGUOUS_ALIAS = "42P09"
        private const val INVALID_COLUMN_REFERENCE = "42P10"
        private const val INVALID_COLUMN_DEFINITION = "42611"
        private const val INVALID_CURSOR_DEFINITION = "42P11"
        private const val INVALID_DATABASE_DEFINITION = "42P12"
        private const val INVALID_FUNCTION_DEFINITION = "42P13"
        private const val INVALID_PREPARED_STATEMENT_DEFINITION = "42P14"
        private const val INVALID_SCHEMA_DEFINITION = "42P15"
        private const val INVALID_TABLE_DEFINITION = "42P16"
        private const val INVALID_OBJECT_DEFINITION = "42P17"
        private const val WITH_CHECK_OPTION_VIOLATION = "44000"
        private const val INSUFFICIENT_RESOURCES = "53000"
        private const val DISK_FULL = "53100"
        private const val OUT_OF_MEMORY = "53200"
        private const val TOO_MANY_CONNECTIONS = "53300"
        private const val CONFIGURATION_LIMIT_EXCEEDED = "53400"
        private const val PROGRAM_LIMIT_EXCEEDED = "54000"
        private const val STATEMENT_TOO_COMPLEX = "54001"
        private const val TOO_MANY_COLUMNS = "54011"
        private const val TOO_MANY_ARGUMENTS = "54023"
        private const val OBJECT_NOT_IN_PREREQUISITE_STATE = "55000"
        private const val OBJECT_IN_USE = "55006"
        private const val CANT_CHANGE_RUNTIME_PARAM = "55P02"
        private const val LOCK_NOT_AVAILABLE = "55P03"
        private const val UNSAFE_NEW_ENUM_VALUE_USAGE = "55P04"
        private const val OPERATOR_INTERVENTION = "57000"
        private const val QUERY_CANCELED = "57014"
        private const val ADMIN_SHUTDOWN = "57P01"
        private const val CRASH_SHUTDOWN = "57P02"
        private const val CANNOT_CONNECT_NOW = "57P03"
        private const val DATABASE_DROPPED = "57P04"
        private const val IDLE_SESSION_TIMEOUT = "57P05"
        private const val SYSTEM_ERROR = "58000"
        private const val IO_ERROR = "58030"
        private const val UNDEFINED_FILE = "58P01"
        private const val DUPLICATE_FILE = "58P02"
        private const val SNAPSHOT_TOO_OLD = "72000"

        fun fromCode(value: String): SqlState =
            when (value) {
                SUCCESSFUL_COMPLETION_CODE -> SuccessfulCompletion
                WARNING_CODE -> Warning
                DYNAMIC_RESULT_SETS_RETURNED_CODE -> DynamicResultSetsReturned
                IMPLICIT_ZERO_BIT_PADDING_CODE -> ImplicitZeroBitPadding
                NULL_VALUE_ELIMINATED_IN_SET_FUNCTION_CODE -> NullValueEliminatedInSetFunction
                PRIVILEGE_NOTE_GRANTED -> PrivilegeNotGranted
                PRIVILEGE_NOT_REVOKED -> PrivilegeNotRevoked
                STRING_DATA_RIGHT_TRUNCATION -> StringDataRightTruncation
                DEPRECATED_FEATURE -> DeprecatedFeature
                NO_DATA -> NoData
                NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED -> NoAdditionalDynamicResultSetsReturned
                SQL_STATEMENT_NOT_YET_COMPLETE -> SqlStatementNotYetComplete
                CONNECTION_EXCEPTION -> ConnectionException
                CONNECTION_DOES_NOT_EXIST -> ConnectionDoesNotExist
                CONNECTION_FAILURE -> ConnectionFailure
                SQL_CLIENT_UNABLE_TO_ESTABLISH_SQL_CONNECTION ->
                    SqlClientUnableToEstablishSqlConnection
                SQL_SERVER_REJECTED_ESTABLISHMENT_OF_SQL_CONNECTION ->
                    SqlServerRejectedEstablishmentOfSqlConnection
                TRANSACTION_RESOLUTION_UNKNOWN -> TransactionResolutionUnknown
                PROTOCOL_VIOLATION -> ProtocolViolation
                TRIGGERED_ACTION_EXCEPTION -> TriggeredActionException
                FEATURE_NOT_SUPPORTED -> FeatureNotSupported
                INVALID_TRANSACTION_INITIATION -> InvalidTransactionInitiation
                LOCATOR_EXCEPTION -> LocatorException
                INVALID_LOCATOR_SPECIFICATION -> InvalidLocatorSpecification
                INVALID_GRANTOR -> InvalidGrantor
                INVALID_GRANT_OPERATION -> InvalidGrantOperation
                INVALID_ROLE_SPECIFICATION -> InvalidRoleSpecification
                DIAGNOSTICS_EXCEPTION -> DiagnosticsException
                STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER ->
                    StackedDiagnosticsAccessedWithoutActiveHandler
                CASE_NOT_FOUND -> CaseNotFound
                CARDINALITY_VIOLATION -> CardinalityViolation
                DATA_EXCEPTION -> DataException
                ARRAY_SUBSCRIPT_ERROR -> ArraySubscriptError
                CHARACTER_NOT_IN_REPERTOIRE -> CharacterNotInRepertoire
                DATETIME_FIELD_OVERFLOW -> DatetimeFieldOverflow
                DIVISION_BY_ZERO -> DivisionByZero
                ERROR_IN_ASSIGNMENT -> ErrorInAssignment
                ESCAPE_CHARACTER_CONFLICT -> EscapeCharacterConflict
                INDICATOR_OVERFLOW -> IndicatorOverflow
                INTERVAL_FIELD_OVERFLOW -> IntervalFieldOverflow
                INVALID_ARGUMENT_FOR_LOGARITHM -> InvalidArgumentForLogarithm
                INVALID_ARGUMENT_FOR_NTILE_FUNCTION -> InvalidArgumentForNtileFunction
                INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION -> InvalidArgumentForNthValueFunction
                INVALID_ARGUMENT_FOR_POWER_FUNCTION -> InvalidArgumentForPowerFunction
                INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION -> InvalidArgumentForWidthBucketFunction
                INVALID_CHARACTER_VALUE_FOR_CAST -> InvalidCharacterValueForCast
                INVALID_DATETIME_FORMAT -> InvalidDatetimeFormat
                INVALID_ESCAPE_CHARACTER -> InvalidEscapeCharacter
                INVALID_ESCAPE_OCTET -> InvalidEscapeOctet
                INVALID_ESCAPE_SEQUENCE -> InvalidEscapeSequence
                NONSTANDARD_USE_OF_ESCAPE_CHARACTER -> NonstandardUseOfEscapeCharacter
                INVALID_INDICATOR_PARAMETER_VALUE -> InvalidIndicatorParameterValue
                INVALID_PARAMETER_VALUE -> InvalidParameterValue
                INVALID_PRECEDING_OR_FOLLOWING_SIZE -> InvalidPrecedingOrFollowingSize
                INVALID_REGULAR_EXPRESSION -> InvalidRegularExpression
                INVALID_ROW_COUNT_IN_LIMIT_CLAUSE -> InvalidRowCountInLimitClause
                INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE -> InvalidRowCountInResultOffsetClause
                INVALID_TABLE_SAMPLE_ARGUMENT -> InvalidTableSampleArgument
                INVALID_TABLE_SAMPLE_REPEAT -> InvalidTableSampleRepeat
                INVALID_TIME_ZONE_DISPLACEMENT_VALUE -> InvalidTimeZoneDisplacementValue
                INVALID_USE_OF_ESCAPE_CHARACTER -> InvalidUseOfEscapeCharacter
                MOST_SPECIFIC_TYPE_MISMATCH -> MostSpecificTypeMismatch
                NULL_VALUE_NOT_ALLOWED -> NullValueNotAllowed
                NULL_VALUE_NO_INDICATOR_PARAMETER -> NullValueNoIndicatorParameter
                NUMERIC_VALUE_OUT_OF_RANGE -> NumericValueOutOfRange
                SEQUENCE_GENERATOR_LIMIT_EXCEEDED -> SequenceGeneratorLimitExceeded
                STRING_DATA_LENGTH_MISMATCH -> StringDataLengthMismatch
                STRING_DATA_RIGHT_TRUNCATION2 -> StringDataRightTruncation2
                SUBSTRING_ERROR -> SubstringError
                TRIM_ERROR -> TrimError
                UNTERMINATED_C_STRING -> UnterminatedCString
                ZERO_LENGTH_CHARACTER_STRING -> ZeroLengthCharacterString
                FLOATING_POINT_EXCEPTION -> FloatingPointException
                INVALID_TEXT_REPRESENTATION -> InvalidTextRepresentation
                INVALID_BINARY_REPRESENTATION -> InvalidBinaryRepresentation
                BAD_COPY_FILE_FORMAT -> BadCopyFileFormat
                UNTRANSLATABLE_CHARACTER -> UntranslatableCharacter
                NOT_AN_XML_DOCUMENT -> NotAnXmlDocument
                INVALID_XML_DOCUMENT -> InvalidXmlDocument
                INVALID_XML_CONTENT -> InvalidXmlContent
                INVALID_XML_COMMENT -> InvalidXmlComment
                INVALID_XML_PROCESSING_INSTRUCTION -> InvalidXmlProcessingInstruction
                DUPLICATE_JSON_OBJECT_KEY_VALUE -> DuplicateJsonObjectKeyValue
                INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION ->
                    InvalidArgumentForSqlJsonDatetimeFunction
                INVALID_JSON_TEXT -> InvalidJsonText
                INVALID_SQL_JSON_SUBSCRIPT -> InvalidSqlJsonSubscript
                MORE_THAN_ONE_SQL_JSON_ITEM -> MoreThanOneSqlJsonItem
                NO_SQL_JSON_ITEM -> NoSqlJsonItem
                NON_NUMERIC_SQL_JSON_ITEM -> NonNumericSqlJsonItem
                NON_UNIQUE_KEYS_IN_A_JSON_OBJECT -> NonUniqueKeysInAJsonObject
                SINGLETON_SQL_JSON_ITEM_REQUIRED -> SingletonSqlJsonItemRequired
                SQL_JSON_ARRAY_NOT_FOUND -> SqlJsonArrayNotFound
                SQL_JSON_MEMBER_NOT_FOUND -> SqlJsonMemberNotFound
                SQL_JSON_NUMBER_NOT_FOUND -> SqlJsonNumberNotFound
                SQL_JSON_OBJECT_NOT_FOUND -> SqlJsonObjectNotFound
                TOO_MANY_JSON_ARRAY_ELEMENTS -> TooManyJsonArrayElements
                TOO_MANY_JSON_OBJECT_MEMBERS -> TooManyJsonObjectMembers
                SQL_JSON_SCALAR_REQUIRED -> SqlJsonScalarRequired
                SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE -> SqlJsonItemCannotBeCastToTargetType
                INTEGRITY_CONSTRAINT_VIOLATION -> IntegrityConstraintViolation
                RESTRICT_VIOLATION -> RestrictViolation
                NOT_NULL_VIOLATION -> NotNullViolation
                FOREIGN_KEY_VIOLATION -> ForeignKeyViolation
                UNIQUE_VIOLATION -> UniqueViolation
                CHECK_VIOLATION -> CheckViolation
                EXCLUSION_VIOLATION -> ExclusionViolation
                INVALID_CURSOR_STATE -> InvalidCursorState
                INVALID_TRANSACTION_STATE -> InvalidTransactionState
                ACTIVE_SQL_TRANSACTION -> ActiveSqlTransaction
                BRANCH_TRANSACTION_ALREADY_ACTIVE -> BranchTransactionAlreadyActive
                HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL -> HeldCursorRequiresSameIsolationLevel
                INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION ->
                    InappropriateAccessModeForBranchTransaction
                INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION ->
                    InappropriateIsolationLevelForBranchTransaction
                NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION ->
                    NoActiveSqlTransactionForBranchTransaction
                READ_ONLY_SQL_TRANSACTION -> ReadOnlySqlTransaction
                SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED ->
                    SchemaAndDataStatementMixingNotSupported
                NO_ACTIVE_SQL_TRANSACTION -> NoActiveSqlTransaction
                IN_FAILED_SQL_TRANSACTION -> InFailedSqlTransaction
                IDLE_IN_TRANSACTION_SESSION_TIMEOUT -> IdleInTransactionSessionTimeout
                INVALID_SQL_STATEMENT_NAME -> InvalidSqlStatementName
                TRIGGERED_DATA_CHANGE_VIOLATION -> TriggeredDataChangeViolation
                INVALID_AUTHORIZATION_SPECIFICATION -> InvalidAuthorizationSpecification
                INVALID_PASSWORD -> InvalidPassword
                DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST ->
                    DependentPrivilegeDescriptorsStillExist
                DEPENDENT_OBJECTS_STILL_EXIST -> DependentObjectsStillExist
                INVALID_TRANSACTION_TERMINATION -> InvalidTransactionTermination
                SQL_ROUTINE_EXCEPTION -> SqlRoutineException
                FUNCTION_EXECUTED_NO_RETURN_STATEMENT -> FunctionExecutedNoReturnStatement
                MODIFYING_SQL_DATA_NOT_PERMITTED -> ModifyingSqlDataNotPermitted
                PROHIBITED_SQL_STATEMENT_ATTEMPTED -> ProhibitedSqlStatementAttempted
                READING_SQL_DATA_NOT_PERMITTED -> ReadingSqlDataNotPermitted
                INVALID_CURSOR_NAME -> InvalidCursorName
                EXTERNAL_ROUTINE_EXCEPTION -> ExternalRoutineException
                CONTAINING_SQL_NOT_PERMITTED -> ContainingSqlNotPermitted
                MODIFYING_SQL_DATA_NOT_PERMITTED2 -> ModifyingSqlDataNotPermitted2
                PROHIBITED_SQL_STATEMENT_ATTEMPTED2 -> ProhibitedSqlStatementAttempted2
                READING_SQL_DATA_NOT_PERMITTED2 -> ReadingSqlDataNotPermitted2
                EXTERNAL_ROUTINE_INVOCATION_EXCEPTION -> ExternalRoutineInvocationException
                INVALID_SQLSTATE_RETURNED -> InvalidSqlstateReturned
                NULL_VALUE_NOT_ALLOWED2 -> NullValueNotAllowed2
                TRIGGER_PROTOCOL_VIOLATED -> TriggerProtocolViolated
                SRF_PROTOCOL_VIOLATED -> SrfProtocolViolated
                EVENT_TRIGGER_PROTOCOL_VIOLATED -> EventTriggerProtocolViolated
                SAVEPOINT_EXCEPTION -> SavepointException
                INVALID_SAVEPOINT_SPECIFICATION -> InvalidSavepointSpecification
                INVALID_CATALOG_NAME -> InvalidCatalogName
                INVALID_SCHEMA_NAME -> InvalidSchemaName
                TRANSACTION_ROLLBACK -> TransactionRollback
                TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION ->
                    TransactionIntegrityConstraintViolation
                SERIALIZATION_FAILURE -> SerializationFailure
                STATEMENT_COMPLETION_UNKNOWN -> StatementCompletionUnknown
                DEADLOCK_DETECTED -> DeadlockDetected
                SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION -> SyntaxErrorOrAccessRuleViolation
                SYNTAX_ERROR -> SyntaxError
                INSUFFICIENT_PRIVILEGE -> InsufficientPrivilege
                CANNOT_COERCE -> CannotCoerce
                GROUPING_ERROR -> GroupingError
                WINDOWING_ERROR -> WindowingError
                INVALID_RECURSION -> InvalidRecursion
                INVALID_FOREIGN_KEY -> InvalidForeignKey
                INVALID_NAME -> InvalidName
                NAME_TOO_LONG -> NameTooLong
                RESERVED_NAME -> ReservedName
                DATATYPE_MISMATCH -> DatatypeMismatch
                INDETERMINATE_DATATYPE -> IndeterminateDatatype
                COLLATION_MISMATCH -> CollationMismatch
                INDETERMINATE_COLLATION -> IndeterminateCollation
                WRONG_OBJECT_TYPE -> WrongObjectType
                GENERATED_ALWAYS -> GeneratedAlways
                UNDEFINED_COLUMN -> UndefinedColumn
                UNDEFINED_FUNCTION -> UndefinedFunction
                UNDEFINED_TABLE -> UndefinedTable
                UNDEFINED_PARAMETER -> UndefinedParameter
                UNDEFINED_OBJECT -> UndefinedObject
                DUPLICATE_COLUMN -> DuplicateColumn
                DUPLICATE_CURSOR -> DuplicateCursor
                DUPLICATE_DATABASE -> DuplicateDatabase
                DUPLICATE_FUNCTION -> DuplicateFunction
                DUPLICATE_PREPARED_STATEMENT -> DuplicatePreparedStatement
                DUPLICATE_SCHEMA -> DuplicateSchema
                DUPLICATE_TABLE -> DuplicateTable
                DUPLICATE_ALIAS -> DuplicateAlias
                DUPLICATE_OBJECT -> DuplicateObject
                AMBIGUOUS_COLUMN -> AmbiguousColumn
                AMBIGUOUS_FUNCTION -> AmbiguousFunction
                AMBIGUOUS_PARAMETER -> AmbiguousParameter
                AMBIGUOUS_ALIAS -> AmbiguousAlias
                INVALID_COLUMN_REFERENCE -> InvalidColumnReference
                INVALID_COLUMN_DEFINITION -> InvalidColumnDefinition
                INVALID_CURSOR_DEFINITION -> InvalidCursorDefinition
                INVALID_DATABASE_DEFINITION -> InvalidDatabaseDefinition
                INVALID_FUNCTION_DEFINITION -> InvalidFunctionDefinition
                INVALID_PREPARED_STATEMENT_DEFINITION -> InvalidPreparedStatementDefinition
                INVALID_SCHEMA_DEFINITION -> InvalidSchemaDefinition
                INVALID_TABLE_DEFINITION -> InvalidTableDefinition
                INVALID_OBJECT_DEFINITION -> InvalidObjectDefinition
                WITH_CHECK_OPTION_VIOLATION -> WithCheckOptionViolation
                INSUFFICIENT_RESOURCES -> InsufficientResources
                DISK_FULL -> DiskFull
                OUT_OF_MEMORY -> OutOfMemory
                TOO_MANY_CONNECTIONS -> TooManyConnections
                CONFIGURATION_LIMIT_EXCEEDED -> ConfigurationLimitExceeded
                PROGRAM_LIMIT_EXCEEDED -> ProgramLimitExceeded
                STATEMENT_TOO_COMPLEX -> StatementTooComplex
                TOO_MANY_COLUMNS -> TooManyColumns
                TOO_MANY_ARGUMENTS -> TooManyArguments
                OBJECT_NOT_IN_PREREQUISITE_STATE -> ObjectNotInPrerequisiteState
                OBJECT_IN_USE -> ObjectInUse
                CANT_CHANGE_RUNTIME_PARAM -> CantChangeRuntimeParam
                LOCK_NOT_AVAILABLE -> LockNotAvailable
                UNSAFE_NEW_ENUM_VALUE_USAGE -> UnsafeNewEnumValueUsage
                OPERATOR_INTERVENTION -> OperatorIntervention
                QUERY_CANCELED -> QueryCanceled
                ADMIN_SHUTDOWN -> AdminShutdown
                CRASH_SHUTDOWN -> CrashShutdown
                CANNOT_CONNECT_NOW -> CannotConnectNow
                DATABASE_DROPPED -> DatabaseDropped
                IDLE_SESSION_TIMEOUT -> IdleSessionTimeout
                SYSTEM_ERROR -> SystemError
                IO_ERROR -> IoError
                UNDEFINED_FILE -> UndefinedFile
                DUPLICATE_FILE -> DuplicateFile
                SNAPSHOT_TOO_OLD -> SnapshotTooOld
                else -> Unknown(code = value)
            }
    }
}
