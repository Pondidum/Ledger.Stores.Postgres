﻿using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class CreateIntAggregateTablesCommand
	{
		private const string Sql = @"
create table if not exists {events-table} (
	id serial primary key,
	timestamp timestamptz not null,
	aggregateID int not null,
	sequence integer not null,
	eventType varchar(255) not null,
	event json not null
);

create table if not exists {snapshots-table} (
	id serial primary key,
	timestamp timestamptz not null,
	aggregateID int not null,
	sequence integer not null,
	snapshotType varchar(255) not null,
	snapshot json not null
);
";

		private readonly NpgsqlConnection _connection;

		public CreateIntAggregateTablesCommand(NpgsqlConnection connection)
		{
			_connection = connection;
		}

		public void Execute(string stream)
		{
			var sql = Sql
				.Replace("{events-table}", stream + "_events")
				.Replace("{snapshots-table}", stream + "_snapshots");

			_connection.Execute(sql);
		}
	}
}
