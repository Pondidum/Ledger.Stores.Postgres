using Ledger.Acceptance;
using Npgsql;

namespace Ledger.Stores.Postgres.Tests
{
	public class Acceptance : AcceptanceTests
	{
		public Acceptance()
			: base(new PostgresEventStore(new NpgsqlConnection(PostgresTestBase.ConnectionString)))
		{
		}
	}
}
