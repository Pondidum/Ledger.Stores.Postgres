using System;
using System.Linq;
using Ledger.Acceptance.TestObjects;
using Newtonsoft.Json;
using Npgsql;
using Shouldly;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class PostgresStoreReaderTests
	{
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
					.Range(0, 1000000)
					.Select(i => new TestEvent { AggregateID = id, Sequence = i, Stamp = DefaultStamper.Now() });

				using (var transaciton = connection.BeginTransaction())
				using (var command = connection.CreateCommand())
				{
					command.CommandText = "COPY importstream_events(aggregateid, stamp, sequence, eventtype, event) from STDIN;";
					command.CommandTimeout = 600000;
					command.Transaction = transaciton;

					var serializer = new NpgsqlCopySerializer(connection);
					var copy = new NpgsqlCopyIn(command, connection, serializer.ToStream);

					copy.Start();

					foreach (var @event in events)
					{
						serializer.AddString(@event.AggregateID.ToString());
						serializer.AddDateTime(@event.Stamp);
						serializer.AddInt32(@event.Sequence);
						serializer.AddString(@event.GetType().AssemblyQualifiedName);
						serializer.AddString(JsonConvert.SerializeObject(@event));

						serializer.EndRow();
						serializer.Flush();
					}

					copy.End();
					serializer.Close();

					transaciton.Commit();
				}
			}
		}

		[RequiresPostgresFact]
		public void When_loading_all_events()
		{
			var context = new EventStoreContext("ImportStream", Default.SerializerSettings);
			using (var reader = _store.CreateReader<Guid>(context))
			{
				//15 -> 20 seconds
				Should.CompleteIn(() => reader.LoadAllEvents().Count().ShouldBe(1000000), TimeSpan.FromSeconds(20));
			}
		}
	}
}
