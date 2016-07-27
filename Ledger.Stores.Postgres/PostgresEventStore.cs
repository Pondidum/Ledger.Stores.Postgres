using System.Collections.Generic;
using System.Data;
using Dapper;
using Newtonsoft.Json;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresEventStore : IEventStore
	{
		static PostgresEventStore()
		{
			SqlMapper.AddTypeHandler(new SequenceTypeHandler());
		}

		private readonly NpgsqlConnection _connection;

		public PostgresEventStore(NpgsqlConnection connection)
		{
			_connection = connection;
		}

		public IStoreReader<TKey> CreateReader<TKey>(EventStoreContext context)
		{
			var connection = _connection.CloneWith(_connection.ConnectionString);

			if (connection.State != ConnectionState.Open)
				connection.Open();

			var transaction = connection.BeginTransaction();

			return new PostgresStoreReader<TKey>(
				connection,
				transaction,
				sql => Events(context.StreamName, sql),
				sql => Snapshots(context.StreamName, sql),
				context.TypeResolver
			);
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(EventStoreContext context)
		{
			var connection = _connection.CloneWith(_connection.ConnectionString);

			if (connection.State != ConnectionState.Open)
				connection.Open();

			var transaction = connection.BeginTransaction();

			return new PostgresStoreWriter<TKey>(
				connection,
				transaction,
				sql => Events(context.StreamName, sql),
				sql => Snapshots(context.StreamName, sql)
			);
		}

		private string Events(string stream, string sql)
		{
			return sql
				.Replace("{table}", TableBuilder.EventsName(stream))
				.Replace("{events_table}", TableBuilder.EventsName(stream));
		}

		private string Snapshots(string stream, string sql)
		{
			return sql
				.Replace("{table}", TableBuilder.SnapshotsName(stream))
				.Replace("{snapshots_table}", TableBuilder.SnapshotsName(stream));
		}
	}
}
