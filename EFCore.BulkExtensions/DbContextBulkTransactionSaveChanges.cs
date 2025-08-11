using Medallion.Collections; // uses StrongNamer nuget to sign ref. with Strong Name
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransactionSaveChanges
{
    #region SaveChanges

    public static async Task SaveChangesAsync(DbContext context, BulkConfig? bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await SaveChangesAsync(context, bulkConfig, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    private static async Task SaveChangesAsync(DbContext context, BulkConfig? bulkConfig, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        // 2 ways:
        // OPTION 1) iteration with Dic and Fast member
        // OPTION 2) using Node model (here setting FK still not implemented)
        int option = 1;

        if (bulkConfig == null)
        {
            bulkConfig = new BulkConfig { };
        }
        // IncludeGraph and FK propagation functionality removed

        var entries = context.ChangeTracker.Entries().Where(x => x.State != EntityState.Unchanged);
        var entriesGroupedByEntity = entries.GroupBy(a => new { EntityType = GetNonProxyType(a.Entity.GetType()), a.State },
            (entry, group) => new
            {
                entry.State,
                Entities = group.Select(a => a.Entity).ToList(),
                EntryType = entry.EntityType,
                EntityType = context.Model.FindEntityType(entry.EntityType)!,
            })
        .ToList();

        // Function to get FKs of an entity type, except self-referencies
        Func<IEntityType, IEnumerable<IEntityType>> getFks = e => e.GetForeignKeys()
            .Where(x => x.PrincipalEntityType != e)
            .Select(x => x.PrincipalEntityType);

        // Topologically sort insert operations by FK
        var added = entriesGroupedByEntity.Where(x => x.State == EntityState.Added);
        var addedLookup = added.ToLookup(x => x.EntityType);
        var sortedAdded = added.OrderTopologicallyBy(g => getFks(g.EntityType).SelectMany(x => addedLookup[x]));

        // Topologically sort delete operations by reverse FK
        var deleted = entriesGroupedByEntity.Where(x => x.State == EntityState.Deleted);
        var deletedLookup = deleted.ToLookup(x => x.EntityType);
        var sortedDeleted = deleted.OrderTopologicallyBy(g => getFks(g.EntityType).SelectMany(x => deletedLookup[x])).Reverse();

        var sortedGroups = sortedAdded
            .Concat(entriesGroupedByEntity.Where(x => x.State == EntityState.Modified))
            .Concat(sortedDeleted)
            .ToList();

        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }
        var connection = context.GetUnderlyingConnection(bulkConfig);

        var hasExistingTransaction = context.Database.CurrentTransaction != null || Transaction.Current != null;

        try
        {

            var transaction = hasExistingTransaction ? null : context.Database.CurrentTransaction ?? context.Database.BeginTransaction();

            if (option == 1)
            {
                Dictionary<string, Dictionary<string, FastProperty>> fastPropertyDicts = new();
                foreach (var entryGroup in sortedGroups)
                {
                    Type entityType = entryGroup.EntryType;
                    entityType = (entityType.Namespace == "Castle.Proxies") ? entityType.BaseType! : entityType;
                    var entityModelType = context.Model.FindEntityType(entityType) ??
                                            throw new ArgumentNullException($"Unable to determine EntityType from given type with name {entityType.Name}");

                    var entityPropertyDict = new Dictionary<string, FastProperty>();
                    if (!fastPropertyDicts.ContainsKey(entityType.Name))
                    {
                        var properties = entityModelType.GetProperties();
                        var navigationPropertiesInfo = entityModelType.GetNavigations().Select(x => x.PropertyInfo);

                        foreach (var property in properties)
                        {
                            if (property.PropertyInfo != null) // skip Shadow Property
                            {
                                entityPropertyDict.Add(property.Name, FastProperty.GetOrCreate(property.PropertyInfo));
                            }
                        }
                        foreach (var navigationPropertyInfo in navigationPropertiesInfo)
                        {
                            if (navigationPropertyInfo != null)
                            {
                                entityPropertyDict.Add(navigationPropertyInfo.Name, FastProperty.GetOrCreate(navigationPropertyInfo));
                            }
                        }
                        fastPropertyDicts.Add(entityType.Name, entityPropertyDict);
                    }
                    else
                    {
                        entityPropertyDict = fastPropertyDicts[entityType.Name];
                    }

                    string methodName = EntityStateBulkMethodDict[entryGroup.State].Key;
                    await InvokeBulkMethod(context, entryGroup.Entities, entityType, methodName, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                }
            }
            else if (option == 2)
            {
                List<BulkMethodEntries> bulkMethodEntriesList = GetBulkMethodEntries(entries);
                foreach (var bulkMethod in bulkMethodEntriesList)
                {
                    if (isAsync)
                    {
                        await InvokeBulkMethod(context, bulkMethod.Entries, bulkMethod.Type, bulkMethod.MethodName, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await InvokeBulkMethod(context, bulkMethod.Entries, bulkMethod.Type, bulkMethod.MethodName, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            if (!hasExistingTransaction)
            {
                transaction!.Commit();
                context.ChangeTracker.AcceptAllChanges();
            }
        }
        finally
        {
            if (!hasExistingTransaction)
            {
                if (isAsync)
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
            }
        }
    }

    private static async Task InvokeBulkMethod(DbContext context, List<object> entities, Type entityType, string methodName, BulkConfig bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        methodName += "Async"; // Always use async methods
        MethodInfo? bulkMethod = typeof(DbContextBulkExtensions)
            .GetMethods()
            .Where(a => a.Name == methodName)
            .FirstOrDefault();

        bulkMethod = bulkMethod?.MakeGenericMethod(typeof(object));

        var arguments = new List<object?> { context, entities, bulkConfig, progress, entityType, cancellationToken };
        var methodArguments = arguments.ToArray();
        if (bulkMethod is not null)
        {
            var task = (Task?)bulkMethod.Invoke(null, methodArguments);
            if (task != null)
            {
                await task.ConfigureAwait(false);
            }
        }
    }

    private static Dictionary<EntityState, KeyValuePair<string, int>> EntityStateBulkMethodDict => new()
    {
        { EntityState.Deleted, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkDeleteAsync), 1) },
        { EntityState.Modified, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkUpdateAsync), 2) },
        { EntityState.Added, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkInsertAsync), 3)},
    };
#endregion

    private static List<BulkMethodEntries> GetBulkMethodEntries(IEnumerable<EntityEntry> entries)
    {
        EntityEntry[] entryList = entries.ToArray();
        var tree = new Dictionary<Type, DbNode>();

        for (int i = 0; i < entryList.Length; i++)
        {
            EntityEntry entry = entryList[i];

            Type type = GetNonProxyType(entry.Entity.GetType());

            if (!tree.TryGetValue(type, out DbNode? node))
            {
                node = new DbNode() { Type = type };
                tree.TryAdd(type, node);
            }

            node.AddEntry(entry);

            var navigations = entry.Navigations.Where(a => a.IsLoaded);

            foreach (var n in navigations.Where(a => a.Metadata.IsCollection))
            {
                Type navType = GetNonProxyType(n.Metadata.ClrType.GenericTypeArguments.Single());
                if (!tree.TryGetValue(navType, out DbNode? childNode))
                {
                    childNode = new DbNode() { Type = navType };

                    tree.TryAdd(navType, childNode);
                };

                if (!childNode.Parents.Any(a => a.Type == node.Type))
                {
                    childNode.Parents.Add(node);
                }

                if (!node.Children.Any(a => a.Type == navType))
                {
                    node.Children.Add(childNode);
                }
            }

            foreach (var n in navigations.Where(a => !a.Metadata.IsCollection))
            {
                Type navType = GetNonProxyType(n.Metadata.ClrType);
                if (!tree.TryGetValue(navType, out DbNode? parentNode))
                {
                    parentNode = new DbNode() { Type = navType };
                    tree.TryAdd(navType, parentNode);
                };

                if (!parentNode.Children.Any(a => a.Type == node.Type))
                {
                    parentNode.Children.Add(node);
                }

                if (!node.Parents.Any(a => a.Type == parentNode.Type))
                {
                    node.Parents.Add(parentNode);
                }
            }
        }

        var rootNodes = tree.Where(a => a.Value.Parents.Count == 0);
        var handledTypes = new Dictionary<Type, bool>();
        var bulkMehodEntriesList = new List<BulkMethodEntries>();

        bool TryAddNode(DbNode node)
        {
            if (node.Parents.All(a => handledTypes.TryGetValue(a.Type, out bool exists)))
            {
                if (!handledTypes.TryGetValue(node.Type, out bool exists))
                {
                    handledTypes.Add(node.Type, true);

                    foreach (var me in node.MethodEntries)
                    {
                        if (me.Value != null && me.Value.Count > 0)
                        {
                            bulkMehodEntriesList.Add(new BulkMethodEntries()
                            {
                                Type = node.Type,
                                MethodName = me.Key,
                                Entries = me.Value,
                            });
                        }
                    }
                }

                foreach (var p in node.Children)
                {
                    TryAddNode(p);
                }

                return exists;
            }

            return false;
        }

        foreach (var r in rootNodes)
        {
            TryAddNode(r.Value);
        }

        return bulkMehodEntriesList;
    }

    private static Type GetNonProxyType(Type type) => type.Namespace == "Castle.Proxies" ? type.BaseType! : type;


    internal class BulkMethodEntries
    {
        public BulkMethodEntries()
        {
            Entries = new List<object>();
        }

        public string MethodName { get; set; } = null!;

        public Type Type { get; set; } = null!;

        public List<object> Entries { get; set; }
    }

    internal class DbNode
    {
        public DbNode()
        {
            Parents = new List<DbNode>();
            Children = new List<DbNode>();
            MethodEntries = new KeyValuePair<string, List<object>>[EntityStateBulkMethodDict.Count];
        }

        public Type Type { get; set; } = null!;

        public List<DbNode> Parents { get; set; }

        public List<DbNode> Children { get; set; }

        public KeyValuePair<string, List<object>>[] MethodEntries { get; private set; }

        public void AddEntry(EntityEntry entry)
        {
            if (EntityStateBulkMethodDict.TryGetValue(entry.State, out KeyValuePair<string, int> method))
            {
                var methodEntry = MethodEntries.FirstOrDefault(a => a.Key == method.Key);
                if (methodEntry.Key == null)
                {
                    methodEntry = new KeyValuePair<string, List<object>>(method.Key, new List<object>());
                    MethodEntries[method.Value - 1] = methodEntry;
                }

                methodEntry.Value.Add(entry.Entity);
            }
        }
    }
}
