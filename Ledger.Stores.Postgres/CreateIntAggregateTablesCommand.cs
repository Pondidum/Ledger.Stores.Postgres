using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class CreateIntAggregateTablesCommand
	{
		private const string Sql = @"
create table if not exists {events-table} (
	streamSequence serial primary key,
	aggregateID int not null,
	sequence int not null,
	eventType varchar(255) not null,
	event json not null
);

create table if not exists {snapshots-table} (
	streamSequence serial primary key,
	aggregateID int not null,
	sequence int not null,
	snapshotType varchar(255) not null,
	snapshot json not null
);
";

		private readonly string _connectionString;

		public CreateIntAggregateTablesCommand(string connectionString)
		{
			_connectionString = connectionString;
		}

		public void Execute(string stream)
		{
			var sql = Sql
				.Replace("{events-table}", stream + "_events")
				.Replace("{snapshots-table}", stream + "_snapshots");

			using (var connection = new NpgsqlConnection(_connectionString))
			{
				connection.Open();
				connection.Execute(sql);
			}
		}
	}
}
