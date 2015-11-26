using System;
using System.Linq;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class EventSaveLoadTests
	{
		private readonly PostgresEventStore _store;
		private readonly StoreConventions _conventions;

		public EventSaveLoadTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof(Guid), typeof(TestAggregate));
		}

		[RequiresPostgresFact]
		public void Events_should_keep_types_and_be_ordered()
		{
			var id = Guid.NewGuid();
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { AggregateID = id, Sequence = 0, NewName = "Deed"},
				new FixNameSpelling {AggregateID = id, Sequence = 1, NewName = "Fix"},
			};

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(toSave);
			}

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEvents(id);

			loaded.First().ShouldBeOfType<NameChangedByDeedPoll>();
			loaded.Last().ShouldBeOfType<FixNameSpelling>();
		}

		[RequiresPostgresFact]
		public void Only_events_for_the_correct_aggregate_are_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, NewName = "Fix" } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll { AggregateID = second, NewName = "Deed" } });
			}

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEvents(first);

			loaded.Single().ShouldBeOfType<FixNameSpelling>();
		}

		[RequiresPostgresFact]
		public void Only_the_latest_sequence_is_returned()
		{
			var first = Guid.NewGuid();
			var second = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 4 } });
				writer.SaveEvents(new[] { new FixNameSpelling { AggregateID = first, Sequence = 5 } });
				writer.SaveEvents(new[] { new NameChangedByDeedPoll { AggregateID = second, Sequence = 6 } });

				writer
					.GetLatestSequenceFor(first)
					.ShouldBe(5);
			}
		}

		[RequiresPostgresFact]
		public void Loading_events_since_only_gets_events_after_the_sequence()
		{
			var id = Guid.NewGuid();
			var toSave = new DomainEvent<Guid>[]
			{
				new NameChangedByDeedPoll { AggregateID = id, Sequence = 3 },
				new FixNameSpelling { AggregateID = id, Sequence = 4 },
				new FixNameSpelling { AggregateID = id, Sequence = 5 },
				new FixNameSpelling { AggregateID = id, Sequence = 6 },
			};

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(toSave);
			}

			var loaded = _store.CreateReader<Guid>(_conventions).LoadEventsSince(id, 4);

			loaded.Select(x => x.Sequence).ShouldBe(new[] { 5, 6 });
		}

		[RequiresPostgresFact]
		public void When_there_are_no_events_and_load_is_called()
		{
			var id = Guid.NewGuid();
			var loaded = _store.CreateReader<Guid>(_conventions).LoadEventsSince(id, 4);

			loaded.ShouldBeEmpty();
		}
	}
}
