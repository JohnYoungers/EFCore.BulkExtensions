using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTestSaveChanges
{
    [Theory]
    public void SaveChangesTest()
    {
        var util = new ContextUtil(dbServer);
        new EFCoreBatchTest().RunDeleteAll();
        using (var context = new TestContext())
        {
#pragma warning disable
            context.ItemHistories.BatchDelete();
        }

        RunSaveChangesOnInsert(dbServer);
        RunSaveChangesOnInsertAndUpdate(dbServer);
    }

    [Theory]
    public async Task SaveChangesTestAsync()
    {
        await new EFCoreBatchTestAsync().RunDeleteAllAsync(dbServer);

        await RunSaveChangesOnInsertAsync(dbServer);
        await RunSaveChangesOnInsertAndUpdateAsync(dbServer);
    }

    private static List<Item> GetNewEntities(int count, string nameSuffix)
    {
        var newEntities = new List<Item>();
        var dateTimeNow = DateTime.Now;

        for (int i = 1; i <= count; i += 1) // Insert 4000 new ones
        {
            newEntities.Add(new Item
            {
                //ItemId = i,
                Name = "Name " + nameSuffix + i,
                Description = "info",
                Quantity = i + 100,
                Price = i / (i % 5 + 1),
                TimeUpdated = dateTimeNow,
                ItemHistories = new List<ItemHistory>()
                    {
                        new ItemHistory
                        {
                            //ItemId = i,
                            ItemHistoryId = SeqGuid.Create(),
                            Remark = $"some more info {i}.1"
                        },
                        new ItemHistory
                        {
                            //ItemId = i,
                            ItemHistoryId = SeqGuid.Create(),
                            Remark = $"some more info {i}.2"
                        }
                    }
            });
        }
        return newEntities;
    }

    private static void RunSaveChangesOnInsert()
    {
        using var context = new TestContext();

        var newEntities = GetNewEntities(dbServer, 5000, "");

        using var transaction = context.Database.BeginTransaction();

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        transaction.Commit();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item? firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity?.Name);
    }

    private static async Task RunSaveChangesOnInsertAsync()
    {
        using var context = new TestContext();

        var newEntities = GetNewEntities(dbServer, 5000, "");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item? firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity?.Name);
    }

    private static void RunSaveChangesOnInsertAndUpdate()
    {
        using var context = new TestContext();

        var loadedEntites = context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToList(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.First().Remark += " UPD";
        }

        var newEntities = GetNewEntities(dbServer, 4000, "NEW ");

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item? firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity?.Description);
    }

    private static async Task RunSaveChangesOnInsertAndUpdateAsync()
    {
        using var context = new TestContext();

        var loadedEntites = await context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToListAsync(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.First().Remark += " UPD";
        }

        var newEntities = GetNewEntities(dbServer, 4000, "NEW ");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item? firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity?.Description);
    }
}
