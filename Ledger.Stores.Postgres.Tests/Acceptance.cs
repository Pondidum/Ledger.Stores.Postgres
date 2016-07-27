using Ledger.Acceptance;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class Acceptance : AcceptanceTests
	{
		public Acceptance(PostgresFixture fixture)
			: base(new PostgresEventStore(PostgresFixture.ConnectionString))
		{

			var snapshotted = new CreateGuidAggregateTablesCommand(PostgresFixture.ConnectionString);
			snapshotted.Execute(SnapshotStream.StreamName);

			var normal = new CreateGuidAggregateTablesCommand(PostgresFixture.ConnectionString);
			normal.Execute(DefaultStream.StreamName);

			fixture.DropOnDispose(SnapshotStream.StreamName);
			fixture.DropOnDispose(DefaultStream.StreamName);

		}
	}
}
