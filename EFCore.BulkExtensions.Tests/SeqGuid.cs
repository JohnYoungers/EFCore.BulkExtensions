using EFCore.BulkExtensions.SqlAdapters;
using RT.Comb;
using System;

namespace EFCore.BulkExtensions.Tests;

public static class SeqGuid
{
    private static readonly ICombProvider SqlNoRepeatCombs = new SqlCombProvider(new SqlDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider UnixCombs = new SqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider PGCombs = new PostgreSqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    public static Guid Create(SqlType sqlType = SqlType.PostgreSql)
    {
        if(sqlType == SqlType.PostgreSql)
            return SqlNoRepeatCombs.Create();
        else if (sqlType == SqlType.PostgreSql)
            return PGCombs.Create();
        else //if (sqlType == SqlType.PostgreSql || sqlType == SqlType.PostgreSql)
            return UnixCombs.Create();
    }
}
