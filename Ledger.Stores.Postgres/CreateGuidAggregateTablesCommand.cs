﻿using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class CreateGuidAggregateTablesCommand
	{
		private const string Sql = @"
create extension if not exists ""uuid-ossp"";

create table if not exists {events-table} (
	streamSequence serial primary key,
	aggregateID uuid not null,
	sequence int not null,
	eventType varchar(255) not null,
	event json not null
);

create table if not exists {snapshots-table} (
	streamSequence serial primary key,
	aggregateID uuid not null,
	sequence int not null,
	snapshotType varchar(255) not null,
	snapshot json not null
);
";
		private readonly NpgsqlConnection _connection;

		public CreateGuidAggregateTablesCommand(NpgsqlConnection connection)
		{
			_connection = connection;
		}

		public void Execute(string stream)
		{
			var sql = Sql
				.Replace("{events-table}", TableBuilder.EventsName(stream))
				.Replace("{snapshots-table}", TableBuilder.SnapshotsName(stream));

			_connection.Execute(sql);
		}
	}
}
