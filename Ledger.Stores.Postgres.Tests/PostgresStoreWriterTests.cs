using System;
using Ledger.Acceptance.TestObjects;
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
					.GetLatestSequenceFor(Guid.NewGuid())
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

		[RequiresPostgresFact]
		public void When_getting_event_count()
		{
			var aggregate = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer.SaveEvents(new[]
				{
					new TestEvent { AggregateID = aggregate, Sequence = 0 },
					new TestEvent { AggregateID = aggregate, Sequence = 1 },
					new TestEvent { AggregateID = aggregate, Sequence = 2 },
					new TestEvent { AggregateID = aggregate, Sequence = 3 },
					new TestEvent { AggregateID = aggregate, Sequence = 4 },
				});

				writer.SaveSnapshot(new TestSnapshot { AggregateID = aggregate, Sequence = 2 });
			}

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer
					.GetNumberOfEventsSinceSnapshotFor(aggregate)
					.ShouldBe(2);
			}
		}
	}
}
