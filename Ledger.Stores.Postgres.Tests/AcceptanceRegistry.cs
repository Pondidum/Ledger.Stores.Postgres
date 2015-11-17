using System;
using Npgsql;
using StructureMap.Configuration.DSL;

namespace Ledger.Stores.Postgres.Tests
{
	public class AcceptanceRegistry : Registry
	{
		public AcceptanceRegistry()
		{
			For<IEventStore>().Use(() => new PostgresEventStore(new NpgsqlConnection(PostgresTestBase.ConnectionString)));
		}
	}
}
