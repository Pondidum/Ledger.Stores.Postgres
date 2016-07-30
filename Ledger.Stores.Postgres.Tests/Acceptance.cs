using System;
using Dapper;
using Ledger.Acceptance;
using Npgsql;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	[Collection("Postgres Collection")]
	public class Acceptance : AcceptanceTests, IDisposable
	{
		private readonly PostgresFixture _fixture;

		public Acceptance(PostgresFixture fixture)
			: base(new PostgresEventStore(PostgresFixture.ConnectionString))
		{
			_fixture = fixture;
			var snapshotted = new CreateGuidAggregateTablesCommand(PostgresFixture.ConnectionString);
			snapshotted.Execute(SnapshotStream.StreamName);

			var normal = new CreateGuidAggregateTablesCommand(PostgresFixture.ConnectionString);
			normal.Execute(DefaultStream.StreamName);

			fixture.DropOnDispose(SnapshotStream.StreamName);
			fixture.DropOnDispose(DefaultStream.StreamName);
		}

		public void Dispose()
		{
			using (var connection = new NpgsqlConnection(PostgresFixture.ConnectionString))
			{
				connection.Open();

				connection.Execute($"drop table if exists {TableBuilder.EventsName(DefaultStream.StreamName)};");
				connection.Execute($"drop table if exists {TableBuilder.SnapshotsName(DefaultStream.StreamName)};");

				connection.Execute($"drop table if exists {TableBuilder.EventsName(SnapshotStream.StreamName)};");
				connection.Execute($"drop table if exists {TableBuilder.SnapshotsName(SnapshotStream.StreamName)};");
			}
		}
	}
}
