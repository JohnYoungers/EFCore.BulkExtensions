using Microsoft.EntityFrameworkCore;
using EFCore.BulkExtensions.SqlAdapters.PostgreSql;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using System;

namespace EFCore.BulkExtensions.SqlAdapters;



#pragma warning disable CS1591 // No XML comment required here
public static class SqlAdaptersMapping
{
    public static string? ProviderName { get; private set; }

    private static readonly PostgreSqlAdapter _adapter = new();
    private static readonly PostgreSqlDialect _dialect = new();
    private static readonly PostgreSqlQueryBuilder _queryBuilder = new();


    
    public static void UpdateProviderName(string? name)
    {
        var ignoreCase = StringComparison.InvariantCultureIgnoreCase;
        if (string.Equals(name, ProviderName, ignoreCase))
        {
            return;
        }

        ProviderName = name;

        if (!(ProviderName?.EndsWith("postgresql", ignoreCase) ?? false))
        {
            throw new NotSupportedException($"Database provider '{ProviderName}' is not supported. Only PostgreSQL is supported.");
        }
    }

    // Properties and methods from PostgreSqlDbServer consolidated here
#pragma warning disable EF1001
    public static string ValueGenerationStrategy => NpgsqlAnnotationNames.ValueGenerationStrategy;
#pragma warning restore EF1001
    
    public static bool PropertyHasIdentity(IAnnotation annotation) =>
        (Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy?)annotation.Value == 
        Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy.IdentityByDefaultColumn;

    /// <summary>
    /// Creates the bulk operations adapter
    /// </summary>
    /// <returns></returns>
    public static PostgreSqlAdapter CreateBulkOperationsAdapter(DbContext dbContext)
    {
        return _adapter;
    }

    /// <summary>
    /// Returns the Adapter dialect to be used
    /// </summary>
    /// <returns></returns>
    public static PostgreSqlDialect GetAdapterDialect(DbContext dbContext)
    {
        return _dialect;
    }



    /// <summary>
    /// Returns per provider QueryBuilder instance, containing a compilation of SQL queries used in EFCore.
    /// </summary>
    /// <returns></returns>
    public static PostgreSqlQueryBuilder GetQueryBuilder(DbContext dbContext)
    {
        return _queryBuilder;
    }
}

#pragma warning restore CS1591 // No XML comment required here
