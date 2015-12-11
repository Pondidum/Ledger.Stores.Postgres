using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres.Tests
{
	public class PostgresFixture : IDisposable
	{
		public const string ConnectionString = "PORT=5432;TIMEOUT=60;POOLING=True;MINPOOLSIZE=1;MAXPOOLSIZE=20;COMMANDTIMEOUT=20;COMPATIBLE=2.1.3.0;HOST=192.168.99.100;USER ID=postgres;PASSWORD=postgres;DATABASE=postgres";
		public const string StreamName = "TestStream";

		public NpgsqlConnection Connection { get; set; }

		private readonly List<string> _streams;

		public PostgresFixture()
		{
			_streams = new List<string>();

			Connection = new NpgsqlConnection(ConnectionString);
			Connection.Open();

			var create = new CreateGuidAggregateTablesCommand(Connection);
			create.Execute(StreamName);

			DropOnDispose(StreamName);
		}

		public void DropOnDispose(string streamName)
		{
			_streams.Add(streamName);
		}

		public void Dispose()
		{
			try
			{
				foreach (var stream in _streams)
				{
					Connection.Execute($"drop table if exists {TableBuilder.EventsName(stream)};");
					Connection.Execute($"drop table if exists {TableBuilder.SnapshotsName(stream)};");
				}
			}
			catch (Exception)
			{
				//omg
			}

			if (Connection.State != ConnectionState.Closed)
			{
				Connection.Close();
			}
		}
	}
}
