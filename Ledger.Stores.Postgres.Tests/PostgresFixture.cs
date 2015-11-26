using System;
using System.Data;
using Npgsql;

namespace Ledger.Stores.Postgres.Tests
{
	public class PostgresFixture : IDisposable
	{
		public const string ConnectionString = "PORT=5432;TIMEOUT=15;POOLING=True;MINPOOLSIZE=1;MAXPOOLSIZE=20;COMMANDTIMEOUT=20;COMPATIBLE=2.1.3.0;HOST=192.168.99.100;USER ID=postgres;PASSWORD=postgres;DATABASE=postgres";
		public const string StreamName = "TestStream";

		public NpgsqlConnection Connection { get; set; }

		public PostgresFixture()
		{
			Connection = new NpgsqlConnection(ConnectionString);

			Connection.Open();
			
			var create = new CreateGuidAggregateTablesCommand(Connection);
			create.Execute(StreamName);
		}

		public void Dispose()
		{
			if (Connection.State != ConnectionState.Closed)
			{
				Connection.Close();
			}
		}
	}
}
