using System;
using System.IO;
using Ledger.Acceptance.TestDomain;
using Ledger.Acceptance.TestObjects;
using Ledger.Conventions;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	public class SnapshotSaveLoadTests : PostgresTestBase
	{
		private readonly PostgresEventStore _store;
		private readonly StoreConventions _conventions;

		public SnapshotSaveLoadTests()
		{
			_store = new PostgresEventStore(Connection);
			_conventions = new StoreConventions(new KeyTypeNamingConvention(), typeof(Guid), typeof(TestAggregate));
		}

		[RequiresPostgresFact]
		public void A_snapshot_should_maintain_type()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 0});
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 1});
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 2});
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 3});
			}

			var loaded = _store.CreateReader<Guid>(_conventions).LoadLatestSnapshotFor(id);

			loaded.ShouldBeOfType<CandidateMemento>();
		}

		[RequiresPostgresFact]
		public void Only_the_latest_snapshot_should_be_loaded()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 4});
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 5});
			}
			_store
				.CreateReader<Guid>(_conventions)
				.LoadLatestSnapshotFor(id)
				.Sequence
				.ShouldBe(5);
		}

		[RequiresPostgresFact]
		public void The_most_recent_snapshot_id_should_be_found()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(_conventions))
			{
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 4});
				writer.SaveSnapshot(id, new CandidateMemento {Sequence = 5});

				writer
					.GetLatestSnapshotSequenceFor(id)
					.ShouldBe(5);
			}
		}


		[RequiresPostgresFact]
		public void When_there_is_no_snapshot_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.CreateReader<Guid>(_conventions).LoadLatestSnapshotFor(id);

			loaded.ShouldBe(null);
		}
	}
}
