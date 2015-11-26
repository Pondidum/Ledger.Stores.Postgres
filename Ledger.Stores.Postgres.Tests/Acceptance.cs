using Ledger.Acceptance;
using Npgsql;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class Acceptance : AcceptanceTests
	{
		public Acceptance(PostgresFixture fixture)
			: base(new PostgresEventStore(fixture.Connection))
		{
		}
	}
}
