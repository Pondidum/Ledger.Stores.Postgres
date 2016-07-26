using System;
using Ledger.Acceptance.TestObjects;
using Ledger.Infrastructure;
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
					new TestEvent { AggregateID = aggregate, Sequence = 0.AsSequence() },
					new TestEvent { AggregateID = aggregate, Sequence = 1.AsSequence() },
					new TestEvent { AggregateID = aggregate, Sequence = 2.AsSequence() },
					new TestEvent { AggregateID = aggregate, Sequence = 3.AsSequence() },
					new TestEvent { AggregateID = aggregate, Sequence = 4.AsSequence() },
				});

				writer.SaveSnapshot(new TestSnapshot { AggregateID = aggregate, Sequence = 2.AsSequence() });
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
