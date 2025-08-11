using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransaction
{
    private static readonly ActivitySource ActivitySource = new("EFCore.BulkExtensions");

    public static async Task ExecuteAsync<T>(DbContext context, Type? type, IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken = default) where T : class
    {
        ValidatePostgreSqlProvider(context);

        type ??= typeof(T);

        var activity = ActivitySource.StartActivity("EFCore.BulkExtensions.BulkExecute");
        if (activity != null)
        {
            activity.AddTag("operationType", operationType.ToString("G"));
            activity.AddTag("entitiesCount", entities.Count().ToString(CultureInfo.InvariantCulture));
        }

        using (activity)
        {
            if (!IsValidTransaction(entities, operationType, bulkConfig)) return;

            if (operationType == OperationType.SaveChanges)
            {
                await DbContextBulkTransactionSaveChanges.SaveChangesAsync(context, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                return;
            }

            var tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

            switch (operationType)
            {
                case OperationType.Insert:
                    await SqlBulkOperation.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                    break;

                case OperationType.Read:
                    await SqlBulkOperation.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                    break;

                case OperationType.Truncate:
                    await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
                    break;

                default:
                    await SqlBulkOperation.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
    }

    #region SqlAdapters Settings
    private static void ValidatePostgreSqlProvider(DbContext context)
    {
        var providerName = context.Database.ProviderName;
        var ignoreCase = StringComparison.InvariantCultureIgnoreCase;
        if (!(providerName?.EndsWith("postgresql", ignoreCase) ?? false))
        {
            throw new NotSupportedException($"Database provider '{providerName}' is not supported. Only PostgreSQL is supported.");
        }
    }
    #endregion

    #region Transaction Validators
    private static bool IsValidTransaction<T>(IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig)
    {
        return entities.Any() ||
               operationType == OperationType.Truncate ||
               operationType == OperationType.SaveChanges ||
               operationType == OperationType.InsertOrUpdateOrDelete ||
               bulkConfig is { CustomSourceTableName: not null } ||
               bulkConfig is { DataReader: not null };
    }
    #endregion
}
