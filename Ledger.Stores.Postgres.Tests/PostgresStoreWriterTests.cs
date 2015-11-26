using System;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using Npgsql;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class PostgresStoreWriterTests
	{
		private readonly PostgresEventStore _store;
		private readonly StoreConventions _conventions;

		public PostgresStoreWriterTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof (Guid), typeof (TestAggregate));
		}

		[RequiresPostgresFact]
		public void GetLatestSequenceFor_can_return_null()
		{
			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer
					.GetLatestSequenceFor(Guid.NewGuid())
					.ShouldBe(null);
			}
		}

		[RequiresPostgresFact]
		public void GetLatestSnapshotSequenceFor_can_return_null()
		{
			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer
					.GetLatestSnapshotSequenceFor(Guid.NewGuid())
					.ShouldBe(null);
			}
		}
	}
}
