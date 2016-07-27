using System;
using System.Linq;
using Ledger.Acceptance.TestObjects;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using Npgsql;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class PostgresStoreReaderTests
	{
		private const int TotalRecords = 1000000;
		public const string StreamName = "ImportStream";
		private readonly PostgresEventStore _store;

		public PostgresStoreReaderTests(PostgresFixture fixture)
		{
			_store = new PostgresEventStore(fixture.Connection);

			fixture.DropOnDispose(StreamName);
			CreateAndFillEvents();
		}

		private void CreateAndFillEvents()
		{
			using (var connection = new NpgsqlConnection(PostgresFixture.ConnectionString))
			{
				connection.Open();

				var creator = new TableBuilder(connection);
				creator.CreateTable(typeof(Guid), StreamName);

				var id = Guid.NewGuid();

				var events = Enumerable
					.Range(0, TotalRecords)
					.Select(i => i.AsSequence())
					.Select(seq => new TestEvent { AggregateID = id, Sequence = seq, Stamp = DefaultStamper.Now() });

				using (var writer = connection.BeginBinaryImport("COPY importstream_events(aggregateid, sequence, eventtype, event) from STDIN (format binary)"))
				{
					foreach (var @event in events)
					{
						writer.StartRow();

						writer.Write(@event.AggregateID.ToString());
						writer.Write((int)@event.Sequence);
						writer.Write(@event.GetType().AssemblyQualifiedName);
						writer.Write(Serializer.Serialize(@event));
					}
				}
			}
		}

		[RequiresPostgresFact]
		public void When_loading_all_events()
		{
			var context = new EventStoreContext("ImportStream", new DefaultTypeResolver());
			using (var reader = _store.CreateReader<Guid>(context))
			{
				//15 -> 20 seconds
				Should.CompleteIn(() => reader.LoadAllEvents().Count().ShouldBe(TotalRecords), TimeSpan.FromSeconds(20));
			}
		}
	}
}
