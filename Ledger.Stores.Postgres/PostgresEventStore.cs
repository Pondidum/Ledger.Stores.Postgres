using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using Newtonsoft.Json;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresEventStore : IEventStore
	{
		private readonly NpgsqlConnection _connection;

		public PostgresEventStore(NpgsqlConnection connection)
		{
			_connection = connection;
		}

		public IStoreReader<TKey> CreateReader<TKey>(IStoreConventions storeConventions)
		{
			if (_connection.State != ConnectionState.Open)
				_connection.Open();

			var transaction = _connection.BeginTransaction();

			return new PostgresStoreReader<TKey>(
				_connection,
				transaction,
				sql => Events(storeConventions, sql),
				sql => Snapshots(storeConventions, sql)
			);
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(IStoreConventions storeConventions)
		{
			if (_connection.State != ConnectionState.Open)
				_connection.Open();

			var transaction = _connection.BeginTransaction();

			return new PostgresStoreWriter<TKey>(
				_connection,
				transaction,
				sql => Events(storeConventions, sql),
				sql => Snapshots(storeConventions, sql)
			);
		}

		private string Events(IStoreConventions conventions, string sql)
		{
			return sql.Replace("{table}", conventions.EventStoreName());
		}

		private string Snapshots(IStoreConventions conventions, string sql)
		{
			return sql.Replace("{table}", conventions.SnapshotStoreName());
		}
	}
}
