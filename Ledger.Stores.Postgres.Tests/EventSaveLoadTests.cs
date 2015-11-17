using System;
using System.Linq;
using Ledger.Acceptance.TestDomain.Events;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	public class EventSaveLoadTests : PostgresTestBase
	{
		private readonly PostgresEventStore _store;
		private readonly StoreConventions _conventions;

		public EventSaveLoadTests()
		{
			_store = new PostgresEventStore(Connection);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof(Guid), typeof(TestAggregate));
		}

		[RequiresPostgresFact]
		public void Events_should_keep_types_and_be_ordered()
		{
			var toSave = new DomainEvent[]
			{
				new NameChangedByDeedPoll {Sequence = 0, NewName = "Deed"},
				new FixNameSpelling {Sequence = 1, NewName = "Fix"},
			};

			var id = Guid.NewGuid();
			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(id, toSave);
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
				writer.SaveEvents(first, new[] { new FixNameSpelling { NewName = "Fix" } });
				writer.SaveEvents(second, new[] { new NameChangedByDeedPoll { NewName = "Deed" } });
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
				writer.SaveEvents(first, new[] { new FixNameSpelling { Sequence = 4 } });
				writer.SaveEvents(first, new[] { new FixNameSpelling { Sequence = 5 } });
				writer.SaveEvents(second, new[] { new NameChangedByDeedPoll { Sequence = 6 } });

				writer
					.GetLatestSequenceFor(first)
					.ShouldBe(5);
			}
		}

		[RequiresPostgresFact]
		public void Loading_events_since_only_gets_events_after_the_sequence()
		{
			var toSave = new DomainEvent[]
			{
				new NameChangedByDeedPoll { Sequence = 3 },
				new FixNameSpelling { Sequence = 4 },
				new FixNameSpelling { Sequence = 5 },
				new FixNameSpelling { Sequence = 6 },
			};

			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveEvents(id, toSave);
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
