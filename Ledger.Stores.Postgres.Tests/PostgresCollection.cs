using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[CollectionDefinition("Postgres Collection")]
	public class PostgresCollection : ICollectionFixture<PostgresFixture>
	{
		// This class has no code, and is never created. Its purpose is simply
		// to be the place to apply [CollectionDefinition] and all the
		// ICollectionFixture<> interfaces.
	}
}
