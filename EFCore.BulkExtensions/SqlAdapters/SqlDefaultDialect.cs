using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains the table alias and SQL query
/// </summary>
public class ExtractedTableAlias
{
#pragma warning disable CS1591 // No XML comments required
    public string TableAlias { get; set; } = null!;
    public string TableAliasSuffixAs { get; set; } = null!;
    public string Sql { get; set; } = null!;
#pragma warning restore CS1591 // No XML comments required
}

/// <summary>
/// Default SQL dialect implementation
/// </summary>
public class SqlDefaultDialect
{
    private static readonly int SelectStatementLength = "SELECT".Length;

    /// <inheritdoc/>
    public virtual List<DbParameter> ReloadSqlParameters(DbContext context, List<DbParameter> sqlParameters)
    {
        var sqlParametersReloaded = new List<DbParameter>();
        foreach (var parameter in sqlParameters)
        {
            var sqlParameter = parameter;

            try
            {
                var dt = sqlParameter.DbType;
                if (sqlParameter.DbType == DbType.DateTime)
                {
                    sqlParameter.DbType = DbType.DateTime2; // sets most specific parameter DbType possible for so that precision is not lost
                }
            }
            catch (Exception ex)
            {
                string noMappingText = "No mapping exists from object type "; // Fixes for Batch ops on PostgreSQL with:
                if (!ex.Message.StartsWith(noMappingText + "System.Collections.Generic.List") &&             // - Contains
                    !ex.Message.StartsWith(noMappingText + "System.Int32[]") &&                              // - Contains
                    !ex.Message.StartsWith(noMappingText + "System.Int64[]") &&                              // - Contains
                    !ex.Message.StartsWith(noMappingText + "System.Guid[]") &&                               // - Contains
                    !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonElement).FullName) && // - JsonElement param
                    !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonDocument).FullName))  // - JsonElement param
                {
                    throw;
                }
            }
            sqlParametersReloaded.Add(sqlParameter);
        }
        return sqlParametersReloaded;
    }

    /// <inheritdoc/>
    public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
    {
        return "+";
    }

    /// <inheritdoc/>
    public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
    {
        // PostgreSQL is the only supported database type
        var escapeSymbolEnd = ".";
        var escapeSymbolStart = " "; // PostgreSQL format
        
        var tableAliasEnd = sqlQuery[SelectStatementLength..sqlQuery.IndexOf(escapeSymbolEnd, StringComparison.Ordinal)]; // " table_alias"
        var tableAliasStartIndex = tableAliasEnd.IndexOf(escapeSymbolStart, StringComparison.Ordinal);
        var tableAlias = tableAliasEnd[(tableAliasStartIndex + escapeSymbolStart.Length)..]; // "table_alias"
        var topStatement = tableAliasEnd[..tableAliasStartIndex].TrimStart(); // if TOP not present in query this will be a Substring(0,0) == ""
        return (tableAlias, topStatement);
    }

    /// <inheritdoc/>
    public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs)
    {
        return new ExtractedTableAlias
        {
            TableAlias = tableAlias,
            TableAliasSuffixAs = tableAliasSuffixAs,
            Sql = fullQuery
        };
    }
}
