using System.Collections.Generic;
using EFCore.BulkExtensions.SqlAdapters;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class TableInfoTests
{
    [Theory]
    private void CreateTableInfo_For_Hidden_PrimaryKey_Does_Not_Throw()
    {
        using var context = new TestContext();

        var entities = new List<TrayType> { new TrayType() };
        var info = TableInfo.CreateInstance(context, null, entities, OperationType.Insert, new BulkConfig
        {
            SetOutputIdentity = true
        });

        Assert.NotNull(info);
    }
}
