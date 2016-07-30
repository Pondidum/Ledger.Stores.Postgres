using System;
using System.Collections.Generic;
using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres.Tests
{
	public class PostgresFixture : IDisposable
	{
		public const string ConnectionString = "PORT=5432;TIMEOUT=60;POOLING=True;MINPOOLSIZE=1;MAXPOOLSIZE=20;COMMANDTIMEOUT=20;HOST=10.0.75.2;USER ID=postgres;PASSWORD=postgres;DATABASE=postgres";

		public static readonly EventStoreContext TestContext = new EventStoreContext("TestStream", new DefaultTypeResolver());

		private readonly List<string> _streams;

		public PostgresFixture()
		{
			_streams = new List<string>();

			DropOnDispose(TestContext.StreamName);
			DropOnDispose("testaggregatestream");
			DropOnDispose("snapshotaggregatestream");
			DropOnDispose("importstream");

			Cleanup();

			var create = new CreateGuidAggregateTablesCommand(ConnectionString);
			create.Execute(TestContext.StreamName);
		}

		public void DropOnDispose(string streamName)
		{
			_streams.Add(streamName);
		}

		public void Dispose()
		{
			Cleanup();
		}

		private void Cleanup()
		{
			using (var connection = new NpgsqlConnection(ConnectionString))
			{
				connection.Open();

				foreach (var stream in _streams)
				{
					Catch(() => connection.Execute($"drop table if exists {TableBuilder.EventsName(stream)};"));
					Catch(() => connection.Execute($"drop table if exists {TableBuilder.SnapshotsName(stream)};"));
				}
			}
		}

		private void Catch(Action action)
		{
			try
			{
				action();
			}
			catch (Exception)
			{
			}
		}
	}
}
