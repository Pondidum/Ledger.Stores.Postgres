﻿using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class CreateGuidAggregateTablesCommand
	{
		private const string Sql = @"
create extension if not exists ""uuid-ossp"";

create table if not exists {events-table} (
	id uuid primary key default uuid_generate_v4(),
	timestamp timestamptz not null,
	aggregateID uuid not null,
	sequence integer not null,
	eventType varchar(255) not null,
	event json not null
);

create table if not exists {snapshots-table} (
	id uuid primary key default uuid_generate_v4(),
	timestamp timestamptz not null,
	aggregateID uuid not null,
	sequence integer not null,
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
