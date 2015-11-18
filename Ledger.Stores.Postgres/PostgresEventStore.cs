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
			var connection = _connection.Clone();
			connection.Open();

			var transaction = connection.BeginTransaction();

			return new PostgresStoreReader<TKey>(
				connection,
				transaction,
				sql => Events(storeConventions, sql),
				sql => Snapshots(storeConventions, sql)
			);
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(IStoreConventions storeConventions)
		{
			var connection = _connection.Clone();
			connection.Open();

			var transaction = connection.BeginTransaction();

			return new PostgresStoreWriter<TKey>(
				connection,
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
