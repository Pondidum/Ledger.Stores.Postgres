using System;
using System.Linq;
using Ledger.Acceptance;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Infrastructure;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class EventSaveLoadTests
	{
		private readonly PostgresEventStore _store;

		public EventSaveLoadTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);
		}

		[RequiresPostgresFact]
		public void Events_should_keep_types_and_be_ordered()
		{
			var id = Guid.NewGuid();
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { AggregateID = id, NewName = "Deed"},
				new FixNameSpelling {AggregateID = id, NewName = "Fix"},
			};

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer.SaveEvents(toSave);
			}

			var loaded = _store.CreateReader<Guid>(PostgresFixture.TestContext).LoadEvents(id);

			loaded.First().ShouldBeOfType<NameChangedByDeedPoll>();
			loaded.Last().ShouldBeOfType<FixNameSpelling>();
		}

		[RequiresPostgresFact]
		public void Only_events_for_the_correct_aggregate_are_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, NewName = "Fix" } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll { AggregateID = second, NewName = "Deed" } });
			}

			var loaded = _store.CreateReader<Guid>(PostgresFixture.TestContext).LoadEvents(first);

			loaded.Single().ShouldBeOfType<FixNameSpelling>();
		}

		[RequiresPostgresFact]
		public void Only_the_latest_sequence_is_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 4.AsSequence() } });
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 5.AsSequence() } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll { AggregateID = second, Sequence = 6.AsSequence() } });

				writer
					.GetLatestSequenceFor(first)
					.ShouldBe(5.AsSequence());
			}
		}

		[RequiresPostgresFact]
		public void Loading_events_since_only_gets_events_after_the_sequence()
		{
			var id = Guid.NewGuid();
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { AggregateID = id, Sequence = 3.AsSequence() },
				new FixNameSpelling { AggregateID = id, Sequence = 4.AsSequence() },
				new FixNameSpelling { AggregateID = id, Sequence = 5.AsSequence() },
				new FixNameSpelling { AggregateID = id, Sequence = 6.AsSequence() },
			};

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.TestContext))
			{
				writer.SaveEvents(toSave);
			}

			var loaded = _store.CreateReader<Guid>(PostgresFixture.TestContext).LoadEventsSince(id, 4.AsSequence());

			loaded.Select(x => x.Sequence).ShouldBe(new[] { 5.AsSequence(), 6.AsSequence() });
		}

		[RequiresPostgresFact]
		public void When_there_are_no_events_and_load_is_called()
		{
			var id = Guid.NewGuid();
			var loaded = _store.CreateReader<Guid>(PostgresFixture.TestContext).LoadEventsSince(id, 4.AsSequence());

			loaded.ShouldBeEmpty();
		}
	}
}
