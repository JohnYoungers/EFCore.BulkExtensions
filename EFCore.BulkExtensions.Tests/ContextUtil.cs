using System;
using System.Collections.Generic;
using System.Linq;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;

namespace EFCore.BulkExtensions.Tests;

public class ContextUtil
{
    public ContextUtil()
    {
    }

    public DbContextOptions GetOptions(IInterceptor dbInterceptor) => GetOptions([dbInterceptor]);
    public DbContextOptions GetOptions(IEnumerable<IInterceptor>? dbInterceptors = null) => GetOptions<TestContext>(dbInterceptors);

    public DbContextOptions GetOptions<TDbContext>(IEnumerable<IInterceptor>? dbInterceptors = null, 
        string databaseName = nameof(EFCoreBulkTest))
        where TDbContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();
        
        // PostgreSQL is the only supported database type
        string connectionString = GetPostgreSqlConnectionString(databaseName);
        var dataSource = new NpgsqlDataSourceBuilder(connectionString)
            .EnableDynamicJson()
            .UseNetTopologySuite()
            .Build();
        optionsBuilder.UseNpgsql(dataSource, opt => opt.UseNetTopologySuite());

        if (dbInterceptors?.Any() == true)
        {
            optionsBuilder.AddInterceptors(dbInterceptors);
        }

        return optionsBuilder.Options;
    }

    private static IConfiguration GetConfiguration()
    {
        var configBuilder = new ConfigurationBuilder()
            .AddJsonFile("testsettings.json", optional: false)
            .AddJsonFile("testsettings.local.json", optional: true);

        return configBuilder.Build();
    }

    public static string GetSqlServerConnectionString(string databaseName)
    {
        return GetConnectionString("SqlServer").Replace("{databaseName}", databaseName);
    }

    public static string GetSqliteConnectionString(string databaseName)
    {
        return GetConnectionString("Sqlite").Replace("{databaseName}", databaseName);
    }

    public static string GetPostgreSqlConnectionString(string databaseName)
    {
        return GetConnectionString("PostgreSql").Replace("{databaseName}", databaseName);
    }

    public static string GetMySqlConnectionString(string databaseName)
    {
        return GetConnectionString("MySql").Replace("{databaseName}", databaseName);
    }

    public static string GetOracleConnectionString(string databaseName)
    {
        return GetConnectionString("Oracle").Replace("{databaseName}", databaseName);
    }

    private static string GetConnectionString(string name)
    {
        return GetConfiguration().GetConnectionString(name) ?? throw new Exception($"Connection string '{name}' not found.");
    }
}
