using System;
using Ledger.Acceptance;
using Ledger.Acceptance.TestDomain;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class SnapshotSaveLoadTests
	{
		private readonly PostgresEventStore _store;
		private readonly IncrementingStamper _stamper;

		public SnapshotSaveLoadTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);
			_stamper = new IncrementingStamper();
		}

		[RequiresPostgresFact]
		public void A_snapshot_should_maintain_type()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.StreamName))
			{
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(0) });
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(1) });
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(2) });
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(3) });
			}

			var loaded = _store.CreateReader<Guid>(PostgresFixture.StreamName).LoadLatestSnapshotFor(id);

			loaded.ShouldBeOfType<CandidateMemento>();
		}

		[RequiresPostgresFact]
		public void Only_the_latest_snapshot_should_be_loaded()
		{
			var id = Guid.NewGuid();

			using (var writer = _store.CreateWriter<Guid>(PostgresFixture.StreamName))
			{
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(4) });
				writer.SaveSnapshot(new CandidateMemento { AggregateID = id, Stamp = _stamper.Offset(5) });
			}
			_store
				.CreateReader<Guid>(PostgresFixture.StreamName)
				.LoadLatestSnapshotFor(id)
				.Stamp
				.ShouldMatch(_stamper.Offset(5));
		}

		[RequiresPostgresFact]
		public void When_there_is_no_snapshot_file_and_load_is_called()
		{
			var id = Guid.NewGuid();

			var loaded = _store.CreateReader<Guid>(PostgresFixture.StreamName).LoadLatestSnapshotFor(id);

			loaded.ShouldBe(null);
		}
	}
}
