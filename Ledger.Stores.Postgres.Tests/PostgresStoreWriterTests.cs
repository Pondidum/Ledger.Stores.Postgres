﻿using System;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class PostgresStoreWriterTests
	{
		private readonly PostgresEventStore _store;

		public PostgresStoreWriterTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);
		}

		[RequiresPostgresFact]
		public void GetLatestSequenceFor_can_return_null()
		{
			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer
					.GetLatestStampFor(Guid.NewGuid())
					.ShouldBe(null);
			}
		}

		[RequiresPostgresFact]
		public void GetLatestSnapshotSequenceFor_can_return_null()
		{
			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer
					.GetNumberOfEventsSinceSnapshotFor(Guid.NewGuid())
					.ShouldBe(0);
			}
		}
	}
}
