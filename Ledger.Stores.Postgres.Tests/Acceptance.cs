using Ledger.Acceptance;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class Acceptance : AcceptanceTests
	{
		public Acceptance(PostgresFixture fixture)
			: base(new PostgresEventStore(fixture.Connection))
		{

			var snapshotted = new CreateGuidAggregateTablesCommand(fixture.Connection);
			snapshotted.Execute(SnapshotStream);

			var normal = new CreateGuidAggregateTablesCommand(fixture.Connection);
			normal.Execute(DefaultStream);

			fixture.DropOnDispose(SnapshotStream);
			fixture.DropOnDispose(DefaultStream);

		}
	}
}
