using RT.Comb;
using System;

namespace EFCore.BulkExtensions.Tests;

public static class SeqGuid
{
    private static readonly ICombProvider SqlNoRepeatCombs = new SqlCombProvider(new SqlDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider UnixCombs = new SqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider PGCombs = new PostgreSqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    public static Guid Create()
    {
        // PostgreSQL-only implementation
            return SqlNoRepeatCombs.Create();
            return PGCombs.Create();
            return UnixCombs.Create();
    }
}
