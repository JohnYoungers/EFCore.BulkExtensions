using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods to generate PostgreSQL SQL queries and adapters
/// </summary>
public static class SqlQueryBuilder
{
    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public static string RestructureForBatch(string sql, bool isDelete = false)
    {
        sql = sql.Replace("[", @"""").Replace("]", @"""");
        string firstLetterOfTable = sql.Substring(7, 1);

        if (isDelete)
        {
            //FROM
            // DELETE i FROM "Item" AS i WHERE i."ItemId" <= 1"
            //TO
            // DELETE FROM "Item" AS i WHERE i."ItemId" <= 1"
            //WOULD ALSO WORK
            // DELETE FROM "Item" WHERE "ItemId" <= 1

            sql = sql.Replace($"DELETE {firstLetterOfTable}", "DELETE ");
        }
        else
        {
            //FROM
            // UPDATE i SET "Description" = @Description, "Price\" = @Price FROM "Item" AS i WHERE i."ItemId" <= 1
            //TO
            // UPDATE "Item" AS i SET "Description" = 'Update N', "Price" = 1.5 WHERE i."ItemId" <= 1
            //WOULD ALSO WORK
            // UPDATE "Item" SET "Description" = 'Update N', "Price" = 1.5 WHERE "ItemId" <= 1

            string tableAS = sql.Substring(sql.IndexOf("FROM") + 4, sql.IndexOf($"AS {firstLetterOfTable}") - sql.IndexOf("FROM"));

            if (!sql.Contains("JOIN"))
            {
                sql = sql.Replace($"AS {firstLetterOfTable}", "");
                //According to postgreDoc sql-update: "Do not repeat the target table as a from_item unless you intend a self-join"
                string fromClause = sql.Substring(sql.IndexOf("FROM"), sql.IndexOf("WHERE") - sql.IndexOf("FROM"));
                sql = sql.Replace(fromClause, "");
            }
            else
            {
                int positionFROM = sql.IndexOf("FROM");
                int positionEndJOIN = sql.IndexOf("JOIN ") + "JOIN ".Length;
                int positionON = sql.IndexOf(" ON");
                int positionEndON = positionON + " ON".Length;
                int positionWHERE = sql.IndexOf("WHERE");
                string oldSqlSegment = sql[positionFROM..positionWHERE];
                string newSqlSegment = "FROM " + sql[positionEndJOIN..positionON];
                string equalsPkFk = sql[positionEndON..positionWHERE];
                sql = sql.Replace(oldSqlSegment, newSqlSegment);
                sql = sql.Replace("WHERE", " WHERE");
                sql = sql + " AND" + equalsPkFk;
            }

            sql = sql.Replace($"UPDATE {firstLetterOfTable}", "UPDATE" + tableAS);
        }

        return sql;
    }

    /// <summary>
    /// Returns a DbParameter instantiated for PostgreSQL
    /// </summary>
    public static DbParameter CreateParameter(string parameterName, object? parameterValue = null)
    {
        return new NpgsqlParameter(parameterName, parameterValue);
    }

    /// <summary>
    /// Returns a DbCommand instantiated for PostgreSQL
    /// </summary>
    public static DbCommand CreateCommand()
    {
        return new NpgsqlCommand();
    }

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSQL parameters
    /// </summary>
    /// <returns></returns>
    public static DbType Dbtype()
    {
        return (DbType)NpgsqlTypes.NpgsqlDbType.Jsonb;
    }

    /// <summary>
    /// Sets NpgsqlDbType for PostgreSQL parameters
    /// </summary>
    public static void SetDbTypeParam(DbParameter parameter, DbType dbType)
    {
        ((NpgsqlParameter)parameter).NpgsqlDbType = (NpgsqlTypes.NpgsqlDbType)dbType;
    }

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var q = $"SELECT {GetCommaSeparatedColumns(columnsNames)} " +
                $"FROM {tableInfo.FullTempOutputTableName} " +
                $"WHERE [{tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()}] IS NOT NULL";
        return q;
    }

    /// <summary>
    /// Generates SQL query to create a table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="tableInfo"></param>
    /// <param name="isOutputTable"></param>
    /// <returns></returns>
    public static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool isOutputTable = false)
    {
        // TODO: (optionaly) if CalculateStats = True then Columns could be omitted from Create and from MergeOutput
        List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict
                                                   : tableInfo.PropertyColumnNamesDict
                                                   ).Values.ToList();
        string timeStampColumn = "";
        if (tableInfo.TimeStampColumnName != null)
        {
            columnsNames.Remove(tableInfo.TimeStampColumnName);
            timeStampColumn = $", [{tableInfo.TimeStampColumnName}] = CAST('' AS {TableInfo.TimeStampOutColumnType})"; // tsType:varbinary(8)
        }
        string temporalTableColumns = "";
        // Temporal columns support removed

        string statsColumn = (tableInfo.BulkConfig.CalculateStats && isOutputTable) ? $", [{tableInfo.SqlActionIUD}] = CAST('' AS char(1))" : "";

        var q = $"SELECT TOP 0 {GetCommaSeparatedColumns(columnsNames, "T")}" + timeStampColumn + temporalTableColumns + statsColumn + " " +
                $"INTO {newTableName} FROM {existingTableName} AS T " +
                $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constrain
        return q;
    }

    /// <summary>
    /// Generates SQL query to alter table columns to nullables
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string AlterTableColumnsToNullable(string tableName, TableInfo tableInfo)
    {
        string q = "";
        foreach (var column in tableInfo.ColumnNamesTypesDict)
        {
            string columnName = column.Key;
            string columnType = column.Value;
            if (columnName == tableInfo.TimeStampColumnName)
            {
                columnType = TableInfo.TimeStampOutColumnType;
            }
            columnName = columnName.Replace("]", "]]");
            q += $"ALTER TABLE {tableName} ALTER COLUMN [{columnName}] {columnType}; ";
        }
        return q;
    }

    // Not used for TableCopy since order of columns is not the same as of original table, that is required for the MERGE
    // (instead after creation, columns are Altered to Nullable)
    /// <summary>
    /// Generates SQL query to create table
    /// </summary>
    /// <param name="newTableName"></param>
    /// <param name="tableInfo"></param>
    /// <param name="isOutputTable"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static string CreateTable(string newTableName, TableInfo tableInfo, bool isOutputTable = false)
    {
        List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict
                                                   : tableInfo.PropertyColumnNamesDict
                                                   ).Values.ToList();
        if (tableInfo.TimeStampColumnName != null)
        {
            columnsNames.Remove(tableInfo.TimeStampColumnName);
        }
        var columnsNamesAndTypes = new List<Tuple<string, string>>();
        foreach (var columnName in columnsNames)
        {
            if (!tableInfo.ColumnNamesTypesDict.TryGetValue(columnName, out string? columnType))
            {
                throw new InvalidOperationException($"Column Type not found in ColumnNamesTypesDict for column: '{columnName}'");
            }
            columnsNamesAndTypes.Add(new Tuple<string, string>(columnName, columnType));
        }
        if (tableInfo.BulkConfig.CalculateStats && isOutputTable)
        {
            columnsNamesAndTypes.Add(new Tuple<string, string>("[SqlActionIUD]", "char(1)"));
        }
        var q = $"CREATE TABLE {newTableName} ({GetCommaSeparatedColumnsAndTypes(columnsNamesAndTypes)});";
        return q;
    }

    /// <summary>
    /// Generates SQL query to add a column
    /// </summary>
    /// <param name="fullTableName"></param>
    /// <param name="columnName"></param>
    /// <param name="columnType"></param>
    /// <returns></returns>
    public static string AddColumn(string fullTableName, string columnName, string columnType)
    {
        var q = $"ALTER TABLE {fullTableName} ADD [{columnName}] {columnType};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to select count updated from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectCountIsUpdateFromOutputTable(TableInfo tableInfo)
    {
        return SelectCountColumnFromOutputTable(tableInfo, "SqlActionIUD", "U");
    }

    /// <summary>
    /// Generates SQL query to select count deleted from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectCountIsDeleteFromOutputTable(TableInfo tableInfo)
    {
        return SelectCountColumnFromOutputTable(tableInfo, "SqlActionIUD", "D");
    }

    /// <summary>
    /// Generates SQL query to select column count from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="columnName"></param>
    /// <param name="columnValue"></param>
    /// <returns></returns>
    public static string SelectCountColumnFromOutputTable(TableInfo tableInfo, string columnName, string columnValue)
    {
        var q = $"SELECT COUNT(*) FROM {tableInfo.FullTempOutputTableName} WHERE [{columnName}] = '{columnValue}'";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public static string DropTable(string tableName, bool isTempTable)
    {
        string q;
        if (isTempTable)
        {
            q = $"IF OBJECT_ID ('tempdb..[#{tableName.Split('#')[1]}', 'U') IS NOT NULL DROP TABLE {tableName}";
        }
        else
        {
            q = $"IF OBJECT_ID ('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
        }
        return q;
    }

    /// <summary>
    /// Generates SQL query to to select identity column
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="schemaName"></param>
    /// <returns></returns>
    public static string SelectIdentityColumnName(string tableName, string schemaName) // No longer used
    {
        var q = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                $"WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 " +
                $"AND TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{schemaName}'";
        return q;
    }

    /// <summary>
    /// Generates SQL query to check whether a table exists
    /// </summary>
    /// <param name="fullTableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public static string CheckTableExist(string fullTableName, bool isTempTable)
    {
        string q;
        if (isTempTable)
        {
            q = $"IF OBJECT_ID ('tempdb..[#{fullTableName.Split('#')[1]}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
        }
        else
        {
            q = $"IF OBJECT_ID ('{fullTableName}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
        }
        return q;
    }

    /// <summary>
    /// Generates SQL query to join table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectJoinTable(TableInfo tableInfo)
    {
        string sourceTable = tableInfo.FullTableName;
        string joinTable = tableInfo.FullTempTableName;
        List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> selectByPropertyNames = tableInfo.PropertyColumnNamesDict.Where(a => tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();

        var q = $"SELECT {GetCommaSeparatedColumns(columnsNames, "S")} " +
                $"FROM {sourceTable} AS S " +
                $"JOIN {joinTable} AS J " +
                $"ON {GetANDSeparatedColumns(selectByPropertyNames, "S", "J", tableInfo.UpdateByPropertiesAreNullable)}";
        return q;
    }

    /// <summary>
    /// Generates SQL query to set identity insert
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="identityInsert"></param>
    /// <returns></returns>
    public static string SetIdentityInsert(string tableName, bool identityInsert)
    {
        string ON_OFF = identityInsert ? "ON" : "OFF";
        var q = $"SET IDENTITY_INSERT {tableName} {ON_OFF};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to merge table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="entityPropertyWithDefaultValue"></param>
    /// <returns></returns>
    /// <exception cref="InvalidBulkConfigException"></exception>
    public static (string sql, IEnumerable<object> parameters) MergeTable<T>(DbContext? context, TableInfo tableInfo, OperationType operationType,
                                                                             IEnumerable<string>? entityPropertyWithDefaultValue = default) where T : class
    {
        List<object> parameters = new();
        string targetTable = tableInfo.FullTableName;
        string sourceTable = tableInfo.FullTempTableName;
        bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();
        List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> columnsNamesOnCompare = tableInfo.PropertyColumnNamesCompareDict.Values.ToList();
        List<string> columnsNamesOnUpdate = tableInfo.PropertyColumnNamesUpdateDict.Values.ToList();
        List<string> outputColumnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        List<string> nonIdentityColumnsNames = columnsNames.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> compareColumnNames = columnsNamesOnCompare.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> updateColumnNames = columnsNamesOnUpdate.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> insertColumnsNames = (tableInfo.HasIdentity && !keepIdentity) ? nonIdentityColumnsNames : columnsNames;

        if (tableInfo.DefaultValueProperties.Any()) // Properties with DefaultValue exclude OnInsert but keep OnUpdate
        {
            var defaults = insertColumnsNames.Where(a => tableInfo.DefaultValueProperties.Contains(a)).ToList();
            //If the entities assign value to properties with default value, don't skip this property 
            if (entityPropertyWithDefaultValue != default)
                defaults = defaults.Where(x => entityPropertyWithDefaultValue.Contains(x)).ToList();
            insertColumnsNames = insertColumnsNames.Where(a => !defaults.Contains(a)).ToList();
        }

        string isUpdateStatsValue = "";
        if (tableInfo.BulkConfig.CalculateStats)
            isUpdateStatsValue = ",SUBSTRING($action, 1, 1)";

        // PreserveInsertOrder functionality removed

        string textWITH_HOLDLOCK = tableInfo.BulkConfig.WithHoldlock ? " WITH (HOLDLOCK)" : string.Empty;

        var q = $"MERGE {targetTable}{textWITH_HOLDLOCK} AS T " +
                $"USING {sourceTable} AS S " +
                $"ON {GetANDSeparatedColumns(primaryKeys, "T", "S", tableInfo.UpdateByPropertiesAreNullable)}";
        q += (primaryKeys.Count == 0) ? "1=0" : string.Empty;

        if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            q += $" WHEN NOT MATCHED BY TARGET " +
                 $"THEN INSERT ({GetCommaSeparatedColumns(insertColumnsNames)}) " +
                 $"VALUES ({GetCommaSeparatedColumns(insertColumnsNames, "S")})";
        }

        q = q.Replace("INSERT () VALUES ()", "INSERT DEFAULT VALUES"); // case when table has only one column that is Identity

        if (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            if (updateColumnNames.Count == 0 && operationType == OperationType.Update)
            {
                throw new InvalidBulkConfigException($"'Bulk{operationType}' operation can not have zero columns to update.");
            }
            else if (updateColumnNames.Count > 0)
            {
                q += $" WHEN MATCHED" +
                     (tableInfo.BulkConfig.OmitClauseExistsExcept || tableInfo.HasSpatialType ? string.Empty : // The data type Geography (Spatial) cannot be used as an operand to the UNION, INTERSECT or EXCEPT operators because it is not comparable
                      $" AND EXISTS (SELECT {GetCommaSeparatedColumns(compareColumnNames, "S")}" + // EXISTS better handles nulls
                      $" EXCEPT SELECT {GetCommaSeparatedColumns(compareColumnNames, "T")})"       // EXCEPT does not update if all values are same
                     ) +
                     (!tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged || tableInfo.TimeStampColumnName == null ? string.Empty :
                      $" AND S.[{tableInfo.TimeStampColumnName}] = T.[{tableInfo.TimeStampColumnName}]"
                     ) +
                     (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null ? $" AND {tableInfo.BulkConfig.OnConflictUpdateWhereSql("T", "S")}" : string.Empty) +
                     $" THEN UPDATE SET {GetCommaSeparatedColumns(updateColumnNames, "T", "S")}";
            }
        }

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            string syncFilterCondition = string.Empty;
            if (tableInfo.BulkConfig.SynchronizeFilter != null)
            {
                if (context is null)
                {
                    throw new ArgumentNullException(nameof(context));
                }
                var querable = context.Set<T>().IgnoreQueryFilters().IgnoreAutoIncludes()
                                               .Where((Expression<Func<T, bool>>)tableInfo.BulkConfig.SynchronizeFilter);

                var (Sql, TableAlias, TableAliasSufixAs, TopStatement, LeadingComments, InnerParameters) = BatchUtil.GetBatchSql(querable, context, false);
                var whereClause = $"{Environment.NewLine}WHERE ";
                int wherePos = Sql.IndexOf(whereClause, StringComparison.OrdinalIgnoreCase);
                if (wherePos > 0)
                {
                    var sqlWhere = Sql[(wherePos + whereClause.Length)..];
                    sqlWhere = sqlWhere.Replace($"[{TableAlias}].", string.Empty);

                    syncFilterCondition = " AND " + sqlWhere;
                    parameters.AddRange(InnerParameters);
                }
                else
                {
                    throw new InvalidBulkConfigException($"'Bulk{operationType}' SynchronizeFilter expression can not be translated to SQL");
                }
            }

            q += " WHEN NOT MATCHED BY SOURCE" + syncFilterCondition;

            string softDeleteAssignment = string.Empty;
            if (tableInfo.BulkConfig.SynchronizeSoftDelete != null)
            {
                if (context is null)
                {
                    throw new ArgumentNullException(nameof(context));
                }
                var querable = context.Set<T>().IgnoreQueryFilters().IgnoreAutoIncludes();
                var expression = (Expression<Func<T, T>>)tableInfo.BulkConfig.SynchronizeSoftDelete;
                var (sqlOriginal, sqlParameters) = BatchUtil.GetSqlUpdate(querable, context, typeof(T), expression);
                var (tableAlias, _) = SqlQueryBuilder.GetBatchSqlReformatTableAliasAndTopStatement(sqlOriginal);

                var sql = sqlOriginal.Replace($"[{tableAlias}]", "T");
                int indexFrom = sql.IndexOf(".") - 1;
                int indexTo = sql.IndexOf(Environment.NewLine) - indexFrom;
                softDeleteAssignment = sql.Substring(indexFrom, indexTo);
                softDeleteAssignment = softDeleteAssignment.TrimEnd();
                parameters.AddRange(sqlParameters);
            }

            q += (softDeleteAssignment != string.Empty) ? $" THEN UPDATE SET {softDeleteAssignment}"
                                                        : $" THEN DELETE";
        }
        if (operationType == OperationType.Delete)
        {
            q += " WHEN MATCHED THEN DELETE";
        }
        if (tableInfo.CreateOutputTable)
        {
            string commaSeparatedColumnsNames;
            if (operationType == OperationType.InsertOrUpdateOrDelete || operationType == OperationType.Delete)
            {
                commaSeparatedColumnsNames = string.Join(", ", outputColumnsNames.Select(x => $"COALESCE(INSERTED.[{x}], DELETED.[{x}])"));
            }
            else
            {
                commaSeparatedColumnsNames = GetCommaSeparatedColumns(outputColumnsNames, "INSERTED");
            }
            q += $" OUTPUT {commaSeparatedColumnsNames}" + isUpdateStatsValue +
                 $" INTO {tableInfo.FullTempOutputTableName}";
        }
        // UseOptionLoopJoin functionality removed (SQL Server specific optimization)
        
        q += ";";

        Dictionary<string, string> sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns ?? new();
        if (tableInfo.BulkConfig.CustomSourceTableName != null
            && sourceDestinationMappings != null
            && sourceDestinationMappings.Count > 0)
        {
            var textOrderBy = "ORDER BY ";
            var textAsS = " AS S";
            int startIndex = q.IndexOf(textOrderBy);
            var qSegment = q[startIndex..q.IndexOf(textAsS)];
            var qSegmentUpdated = qSegment;
            foreach (var mapping in sourceDestinationMappings)
            {
                var propertySourceFormated = $"S.[{mapping.Value}]";
                var propertyFormated = $"[{mapping.Value}]";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $"[{sourceProperty}]");
                }
                if (q.Contains(propertySourceFormated))
                {
                    q = q.Replace(propertySourceFormated, $"S.[{sourceProperty}]");
                }
            }
            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        return (sql: q, parameters);
    }

    /// <summary>
    /// Generates SQL query to truncate a table
    /// </summary>
    /// <param name="tableName"></param>
    public static string TruncateTable(string tableName)
    {
        var q = $"TRUNCATE {tableName} RESTART IDENTITY;";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    #region PostgreSQL-specific methods

    /// <summary>
    /// Generates SQL query to create Output table for Stats
    /// </summary>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    /// <param name="unlogged"></param>
    public static string CreateOutputStatsTable(string newTableName, bool useTempDb, bool unlogged)
    {
        string keywordPrefix = "";
        if (useTempDb == true)
        {
            keywordPrefix = "TEMP "; // "TEMP " or "TEMPORARY "
        }
        else if(unlogged) // can not be combined with TEMP since Temporary tables are not logged by default.
        {
            keywordPrefix = "UNLOGGED ";
        }
        var q = @$"CREATE {keywordPrefix}TABLE IF NOT EXISTS {newTableName} (""xmaxNumber"" xid)"; // col name can't be just 'xmax' - conflicts with system column
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    /// <param name="unlogged"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb, bool unlogged)
    {
        string keywordPrefix = "";
        if (useTempDb == true)
        {
            keywordPrefix = "TEMP "; // "TEMP " or "TEMPORARY "
        }
        else if (unlogged) // can not be combined with TEMP since Temporary tables are not logged by default.
        {
            keywordPrefix = "UNLOGGED ";
        }
        var q = $"CREATE {keywordPrefix}TABLE {newTableName} " +
                $"AS TABLE {existingTableName} " +
                $"WITH NO DATA;";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL to copy table columns from STDIN 
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="tableName"></param>
    public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string? tableName = null)
    {
        tableName ??= tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
        tableName = tableName.Replace("[", @"""").Replace("]", @"""");

        var columnsList = GetColumnList(tableInfo, operationType);

        var commaSeparatedColumns = GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

        var q = $"COPY {tableName} " +
                $"({commaSeparatedColumns}) " +
                $"FROM STDIN (FORMAT BINARY)";

        return q + ";";
    }

    /// <summary>
    /// Generates SQL merge statement
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <exception cref="NotImplementedException"></exception>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        var columnsList = GetColumnList(tableInfo, operationType);

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"For Postgres method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string q;
        bool appendReturning = false;
        if (operationType == OperationType.Read)
        {
            var readByColumns = GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()); //, tableInfo.FullTableName, tableInfo.FullTempTableName

            q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                $"JOIN {tableInfo.FullTempTableName} " +
                $"USING ({readByColumns})"; //$"ON ({tableInfo.FullTableName}.readByColumns = {tableInfo.FullTempTableName}.readByColumns);";
        }
        else if (operationType == OperationType.Delete)
        {
            var deleteByColumns = GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(), tableInfo.FullTableName, tableInfo.FullTempTableName);
            deleteByColumns = deleteByColumns.Replace(",", " AND")
                                             .Replace("[", @"""").Replace("]", @"""");

            q = $"DELETE FROM {tableInfo.FullTableName} " +
                $"USING {tableInfo.FullTempTableName} " +
                $"WHERE {deleteByColumns}";
        }
        else if (operationType == OperationType.Update)
        {
            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(tableInfo.PropertyColumnNamesUpdateDict.ContainsValue).ToList();

            var updateByColumns = GetANDSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(),
                prefixTable: tableInfo.FullTableName, equalsTable: tableInfo.FullTempTableName).Replace("[", @"""").Replace("]", @"""");
            var equalsColumns = GetCommaSeparatedColumns(columnsToUpdate,
                equalsTable: tableInfo.FullTempTableName).Replace("[", @"""").Replace("]", @"""");

            q = $"UPDATE {tableInfo.FullTableName} SET {equalsColumns} " +
                $"FROM {tableInfo.FullTempTableName} " +
                $"WHERE {updateByColumns}";

            appendReturning = true;
        }
        else
        {
            var columnsListInsert = columnsList;
            var textValueFirstPK = tableInfo.TextValueFirstPK;
            if (textValueFirstPK != null && (textValueFirstPK == "0" || textValueFirstPK.ToString() == Guid.Empty.ToString() || textValueFirstPK.ToString() == ""))
            {
                //  PKs can be all set or all empty in which case DB generates it, can not have it combined in one list when using InsetOrUpdate  
                columnsListInsert = columnsList.Where(tableInfo.PropertyColumnNamesUpdateDict.ContainsValue).ToList();
            }
            var commaSeparatedColumns = GetCommaSeparatedColumns(columnsListInsert).Replace("[", @"""").Replace("]", @"""");

            var updateByColumns = GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()).Replace("[", @"""").Replace("]", @"""");

            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            var equalsColumns = GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", @"""").Replace("]", @"""");

            int subqueryLimit = tableInfo.BulkConfig.ApplySubqueryLimit;
            var subqueryText = subqueryLimit > 0 ? $"LIMIT {subqueryLimit} " : "";
            bool onUpdateDoNothing = columnsToUpdate.Count == 0 || string.IsNullOrWhiteSpace(equalsColumns);

            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " + subqueryText +
                $"ON CONFLICT ({updateByColumns}) " +
                (onUpdateDoNothing
                 ? $"DO NOTHING"
                 : $"DO UPDATE SET {equalsColumns}");

            if (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null)
            {
                q += $" WHERE {tableInfo.BulkConfig.OnConflictUpdateWhereSql(tableInfo.FullTableName.Replace("[", @"""").Replace("]", @""""), "EXCLUDED")}";
            }
            appendReturning = true;
        }

        if (appendReturning == true && tableInfo.CreateOutputTable)
        {
            var allColumnsList = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
            string commaSeparatedColumnsNames = GetCommaSeparatedColumns(allColumnsList, tableInfo.FullTableName).Replace("[", @"""").Replace("]", @"""");
            q += $" RETURNING {commaSeparatedColumnsNames}";

            if (tableInfo.BulkConfig.CalculateStats)
            {
                q += ", xmax";
            }
        }

        q = q.Replace("[", @"""").Replace("]", @"""");

        Dictionary<string, string>? sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns;
        if (tableInfo.BulkConfig.CustomSourceTableName != null && sourceDestinationMappings != null && sourceDestinationMappings.Count > 0)
        {
            var textSelect = "SELECT ";
            var textFrom = " FROM";
            int startIndex = q.IndexOf(textSelect);
            var qSegment = q[startIndex..q.IndexOf(textFrom)];
            var qSegmentUpdated = qSegment;
            foreach (var mapping in sourceDestinationMappings)
            {
                var propertyFormated = $@"""{mapping.Value}""";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $@"""{sourceProperty}""");
                }
            }
            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        if (tableInfo.BulkConfig.CalculateStats)
        {
            q = $"WITH upserted AS ({q}), " +
                $"NEW AS ( INSERT INTO {tableInfo.FullTempOutputTableName} SELECT xmax FROM upserted ) " +
                $"SELECT * FROM upserted";
        }

        q = q.Replace("[", @"""").Replace("]", @"""");
        q += ";";

        return q;
    }

    /// <summary>
    /// Returns a list of columns for the given table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
    {
        var tempDict = tableInfo.PropertyColumnNamesDict;
        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

        tableInfo.PropertyColumnNamesDict = tempDict;

        bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
        var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();
        if (!keepIdentity && tableInfo.HasIdentity && (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniquColumnName))
        {
            var identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
            columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
            propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
        }

        return columnsList;
    }

    /// <summary>
    /// Generates SQL query to drop a table
    /// </summary>
    /// <param name="tableName"></param>
    public static string DropTable(string tableName)
    {
        string q = $"DROP TABLE IF EXISTS {tableName}";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to count the unique constranints -  Not used, insted used only: CountUniqueIndex
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueConstrain(TableInfo tableInfo)
    {
        var primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string q;

        bool usePG_Catalog = true; // PG_Catalog used instead of Information_Schema
        if (usePG_Catalog)
        {
            q = @"SELECT COUNT(distinct c.conname)
                  FROM pg_catalog.pg_namespace nr,
                      pg_catalog.pg_class r,
                      pg_catalog.pg_attribute a,
                      pg_catalog.pg_namespace nc,
                      pg_catalog.pg_constraint c
                  WHERE nr.oid = r.relnamespace
                  AND r.oid = a.attrelid
                  AND nc.oid = c.connamespace
                  AND r.oid =
                      CASE c.contype
                          WHEN 'f'::""char"" THEN c.confrelid
                      ELSE c.conrelid
                          END
                      AND (a.attnum = ANY (
                          CASE c.contype
                      WHEN 'f'::""char"" THEN c.confkey
                          ELSE c.conkey
                          END))
                      AND NOT a.attisdropped
                      AND (c.contype = ANY (ARRAY ['p'::""char"", 'u'::""char""]))
                      AND (r.relkind = ANY (ARRAY ['r'::""char"", 'p'::""char""]))" +
                $" AND r.relname = '{tableInfo.TableName}'" + 
                $" AND nr.nspname = '{tableInfo.Schema}'" + 
                $" AND a.attname IN('{string.Join("','", primaryKeysColumns)}')";
        }
        else // Deprecated - Information_Schema no longer used (is available only in default database)
        {
            q = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ";
            foreach (var (pkColumn, index) in primaryKeysColumns.Select((value, i) => (value, i)))
            {
                q += $"INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu{index} " +
                     $"ON cu{index}.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND cu{index}.COLUMN_NAME = '{pkColumn}' ";
            }

            q += $"WHERE (tc.CONSTRAINT_TYPE = 'UNIQUE' OR tc.CONSTRAINT_TYPE = 'PRIMARY KEY') " +
                 $"AND tc.TABLE_NAME = '{tableInfo.TableName}' AND tc.TABLE_SCHEMA = '{tableInfo.Schema}'";
        }
        return q;
    }

    /// <summary>
    /// Generates SQL query to count the unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueIndex(TableInfo tableInfo)
    {
        var primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string q;
        q = @"SELECT COUNT(idx.relname)
              FROM pg_catalog.pg_index pgi
                JOIN pg_catalog.pg_class idx ON idx.oid = pgi.indexrelid
                JOIN pg_catalog.pg_namespace insp ON insp.oid = idx.relnamespace
                JOIN pg_catalog.pg_class tbl ON tbl.oid = pgi.indrelid
                JOIN pg_catalog.pg_namespace tnsp ON tnsp.oid = tbl.relnamespace
                JOIN pg_catalog.pg_attribute at ON at.attrelid = idx.oid
              WHERE pgi.indisunique
                AND not pgi.indisprimary" +
             $" AND tnsp.nspname = '{tableInfo.Schema}'" +
             $" AND tbl.relname = '{tableInfo.TableName}'" +
             $" AND at.attname IN('{string.Join("','", primaryKeysColumns)}')" +
            " GROUP BY idx.relname" +
            " HAVING COUNT(idx.relname) = " + primaryKeysColumns.Count + ";";
        return q;
    }

    /// <summary>
    /// Generate SQL query to create a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueIndex(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueIndexName = GetUniqueIndexName(tableInfo);

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesFormated = @"""" + string.Join(@""", """, uniqueColumnNames) + @"""";

        var q = $@"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ""{uniqueIndexName}"" " +
                $@"ON {fullTableNameFormated} ({uniqueColumnNamesFormated})";
        return q;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueConstrainName = GetUniqueIndexName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT ""{uniqueConstrainName}"" " +
                $@"UNIQUE USING INDEX ""{uniqueConstrainName}""";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueIndex(TableInfo tableInfo)
    {
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var uniqueIndexName = GetUniqueIndexName(tableInfo);
        var fullUniqueIndexNameFormated = $@"{schemaFormated}""{uniqueIndexName}""";

        var q = $@"DROP INDEX {fullUniqueIndexNameFormated};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueIndexName = GetUniqueIndexName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP CONSTRAINT ""{uniqueIndexName}"";";
        return q;
    }

    /// <summary>
    /// Creates UniqueConstrainName
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string GetUniqueIndexName(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueIndexName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";
        uniqueIndexName = uniqueIndexName.Length > 64 ? uniqueIndexName[..64] : uniqueIndexName;

        return uniqueIndexName;
    }

    #endregion


    /// <summary>
    /// Used for Sqlite, Truncate table 
    /// </summary>
    public static string DeleteTable(string tableName)
    {
        var q = $"DELETE FROM {tableName};" +
                $"VACUUM;";
        return q;
    }

    // propertColumnsNamesDict used with Sqlite for @parameter to be save from non valid charaters ('', '!', ...) that are allowed as column Names in Sqlite

    /// <summary>
    /// Generates SQL query to get comma seperated column
    /// </summary>
    /// <param name="columnsNames"></param>
    /// <param name="prefixTable"></param>
    /// <param name="equalsTable"></param>
    /// <param name="propertColumnsNamesDict"></param>
    /// <returns></returns>
    public static string GetCommaSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null,
                                                  Dictionary<string, string>? propertColumnsNamesDict = null)
    {
        prefixTable += (prefixTable != null && prefixTable != "@") ? "." : "";
        equalsTable += (equalsTable != null && equalsTable != "@") ? "." : "";

        string commaSeparatedColumns = "";
        foreach (var columnName in columnsNames)
        {
            var equalsParameter = propertColumnsNamesDict == null ? columnName : propertColumnsNamesDict.SingleOrDefault(a => a.Value == columnName).Key;
            commaSeparatedColumns += prefixTable != "" ? $"{prefixTable}[{columnName}]" : $"[{columnName}]";
            commaSeparatedColumns += equalsTable != "" ? $" = {equalsTable}[{equalsParameter}]" : "";
            commaSeparatedColumns += ", ";
        }
        if (commaSeparatedColumns != "")
        {
            commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
        }
        return commaSeparatedColumns;
    }


    /// <summary>
    /// Generates a comma seperated column list with its SQL data type
    /// </summary>
    /// <param name="columnsNamesAndTypes"></param>
    /// <returns></returns>
    public static string GetCommaSeparatedColumnsAndTypes(List<Tuple<string, string>> columnsNamesAndTypes)
    {
        string commaSeparatedColumns = "";
        foreach (var columnNameAndType in columnsNamesAndTypes)
        {
            commaSeparatedColumns += $"[{columnNameAndType.Item1}] {columnNameAndType.Item2}, ";
        }
        if (commaSeparatedColumns != "")
        {
            commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
        }
        return commaSeparatedColumns;
    }

    /// <summary>
    /// Generates SQL query to seperate columns
    /// </summary>
    /// <param name="columnsNames"></param>
    /// <param name="prefixTable"></param>
    /// <param name="equalsTable"></param>
    /// <param name="updateByPropertiesAreNullable"></param>
    /// <param name="propertColumnsNamesDict"></param>
    /// <returns></returns>
    public static string GetANDSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null, bool updateByPropertiesAreNullable = false,
                                                Dictionary<string, string>? propertColumnsNamesDict = null)
    {
        string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames, prefixTable, equalsTable, propertColumnsNamesDict);

        if (updateByPropertiesAreNullable)
        {
            string[] columns = commaSeparatedColumns.Split(',');
            string commaSeparatedColumnsNullable = String.Empty;
            foreach (var column in columns)
            {
                string[] columnTS = column.Split('=');
                string columnT = columnTS[0].Trim();
                string columnS = columnTS[1].Trim();
                string columnNullable = $"({column.Trim()} OR ({columnT} IS NULL AND {columnS} IS NULL))";
                commaSeparatedColumnsNullable += columnNullable + ", ";
            }
            if (commaSeparatedColumns != "")
            {
                commaSeparatedColumnsNullable = commaSeparatedColumnsNullable.Remove(commaSeparatedColumnsNullable.Length - 2, 2);
            }
            commaSeparatedColumns = commaSeparatedColumnsNullable;
        }

        string ANDSeparatedColumns = commaSeparatedColumns.Replace(",", " AND");
        return ANDSeparatedColumns;
    }

    /// <summary>
    /// Gets batch SQL reformat table alias and top statement for PostgreSQL
    /// </summary>
    /// <param name="sqlQuery"></param>
    /// <returns></returns>
    public static (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
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

    private static readonly int SelectStatementLength = "SELECT".Length;

    /// <summary>
    /// Gets batch SQL extract table alias from query for PostgreSQL
    /// </summary>
    /// <param name="fullQuery"></param>
    /// <param name="tableAlias"></param>
    /// <param name="tableAliasSuffixAs"></param>
    /// <returns></returns>
    public static ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs)
    {
        return new ExtractedTableAlias
        {
            TableAlias = tableAlias,
            TableAliasSuffixAs = tableAliasSuffixAs,
            Sql = fullQuery
        };
    }
}

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
