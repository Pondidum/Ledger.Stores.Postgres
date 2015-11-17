using System;
using Npgsql;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	public class PostgresStoreWriterTests : PostgresTestBase
	{
		[RequiresPostgresFact]
		public void GetLatestSequenceFor_can_return_null()
		{
			var tx = Connection.BeginTransaction();

			var writer = new PostgresStoreWriter<Guid>(
				Connection,
				tx,
				sql => sql.Replace("{table}", "events_guid"),
				sql => sql.Replace("{table}", "snapshots_guid"));

			writer
				.GetLatestSequenceFor(Guid.NewGuid())
				.ShouldBe(null);

			writer
				.Dispose();
		}

		[RequiresPostgresFact]
		public void GetLatestSnapshotSequenceFor_can_return_null()
		{
			var tx = Connection.BeginTransaction();

			var writer = new PostgresStoreWriter<Guid>(
				Connection,
				tx,
				sql => sql.Replace("{table}", "events_guid"),
				sql => sql.Replace("{table}", "snapshots_guid"));

			writer
				.GetLatestSnapshotSequenceFor(Guid.NewGuid())
				.ShouldBe(null);

			writer
				.Dispose();
		}
	}
}
