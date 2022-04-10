package com.clasicrando.kdbc

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSFunctionDeclaration
import com.google.devtools.ksp.symbol.KSValueParameter
import com.google.devtools.ksp.symbol.KSVisitorVoid
import com.google.devtools.ksp.validate

class QueryResultProcessor (
    val codeGenerator: CodeGenerator,
): SymbolProcessor {
    override fun process(resolver: Resolver): List<KSAnnotated> {
        val symbols = resolver.getSymbolsWithAnnotation(QueryResult::class.qualifiedName!!)
        val ret = symbols.filter { !it.validate() }.toList()
        val validSymbols = symbols.filter { it is KSClassDeclaration && it.validate() }
        for (symbol in validSymbols) {
            symbol.accept(QueryResultVisitor(), Unit)
        }
        return ret
    }

    inner class QueryResultVisitor: KSVisitorVoid() {

        private fun getResultSetExtract(param: KSValueParameter, index: Int): String {
            val type = param.type.resolve()
            return when (val typeName = type.makeNotNullable().declaration.simpleName.asString()) {
                "Boolean" -> "rs.getBoolean(${index + 1})"
                "Int" -> "rs.getInt(${index + 1})"
                "Double" -> "rs.getDouble(${index + 1})"
                "Float" -> "rs.getFloat(${index + 1})"
                "Long" -> "rs.getLong(${index + 1})"
                "Date" -> "rs.getDate(${index + 1})"
                "Short" -> "rs.getShort(${index + 1})"
                "String" -> "rs.getString(${index + 1})"
                "Instant" -> "rs.getTimestamp(${index + 1}).toInstant()"
                "LocalDate" -> "rs.getDate(${index + 1}).toLocalDate()"
                "LocalDateTime" -> "rs.getTimestamp(${index + 1}).toLocalDateTime()"
                "List" -> "rs.getArray(${index + 1}).getList()"
                else -> {
                    val isEnum = (type.declaration as KSClassDeclaration).modifiers.any { it.name == "ENUM" }
                    when {
                        isEnum -> "${typeName}.valueOf(rs.getString(${index + 1}))"
                        else -> error("Could not find an extraction function for type $typeName")
                    }
                }
            }
        }

        override fun visitClassDeclaration(classDeclaration: KSClassDeclaration, data: Unit) {
            classDeclaration.primaryConstructor!!.accept(this, data)
        }

        override fun visitFunctionDeclaration(function: KSFunctionDeclaration, data: Unit) {
            val parent = function.parentDeclaration!! as KSClassDeclaration
            val packageName = parent.containingFile!!.packageName.asString()
            val className = "${parent.simpleName.asString()}ResultSetParser"
            val file = codeGenerator.createNewFile(
                dependencies = Dependencies(true, function.containingFile!!),
                packageName = "com.clasicrando.kdbc" ,
                fileName = className,
            )
            file.appendText("package com.clasicrando.kdbc\n\n")
            file.appendText("import ${packageName}.${parent.simpleName.asString()}\n")
            file.appendText("import java.sql.ResultSet\n\n")
            file.appendText("object $className: ResultSetParser<${parent.simpleName.asString()}>{\n")
            file.appendText("    private const val columnCount = ${function.parameters.size}\n")
            file.appendText("    override fun parse(rs: ResultSet): ${parent.simpleName.asString()} {\n")
            file.appendText("        require(rs.metaData.columnCount == columnCount) {\n")
            file.appendText("            \"Expected ${'$'}columnCount columns, got ${'$'}{rs.metaData.columnCount}\"\n")
            file.appendText("        }\n")
            file.appendText("        return ${parent.simpleName.asString()}(\n")
            for ((i, param) in function.parameters.withIndex()) {
                file.appendText("            ${getResultSetExtract(param, i)},\n")
            }
            file.appendText("        )\n")
            file.appendText("    }\n")
            file.appendText("}\n")
            file.close()
        }
    }

}

class QueryResultProcessorProvider : SymbolProcessorProvider {
    override fun create(
        environment: SymbolProcessorEnvironment
    ): SymbolProcessor {
        return QueryResultProcessor(environment.codeGenerator)
    }
}
