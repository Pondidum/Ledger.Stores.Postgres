using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using Newtonsoft.Json;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresEventStore<TKey> : IEventStore<TKey>
	{
		private readonly NpgsqlConnection _connection;
		private readonly JsonSerializerSettings _jsonSettings;
		private readonly NpgsqlTransaction _transaction;

		public PostgresEventStore(NpgsqlConnection connection)
			: this(connection, null)
		{
		}

		private PostgresEventStore(NpgsqlConnection connection, NpgsqlTransaction transaction)
		{
			_connection = connection;
			_transaction = transaction;
			_jsonSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Objects };
		}

		/// <summary>
		///   
		/// </summary>
		/// <param name="conventions">_aggregateStore.Conventions&lt;TAggregate&gt;);</param>
		public void CreateTable(IStoreConventions conventions)
		{
			var builder = new TableBuilder(_connection);
			builder.CreateTable(conventions);
		}

		private string Events(IStoreConventions conventions, string sql)
		{
			return sql.Replace("{table}", conventions.EventStoreName());
		}

		private string Snapshots(IStoreConventions conventions, string sql)
		{
			return sql.Replace("{table}", conventions.SnapshotStoreName());
		}

		public int? GetLatestSequenceFor(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Events(conventions, "select max(sequence) from {table} where aggregateID = @id");

			return _connection.ExecuteScalar<int>(sql, new { ID = aggregateID });
		}

		public int? GetLatestSnapshotSequenceFor(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Snapshots(conventions, "select max(sequence) from {table} where aggregateID = @id");

			return _connection.ExecuteScalar<int>(sql, new { ID = aggregateID });
		}

		public void SaveEvents(IStoreConventions conventions, TKey aggregateID, IEnumerable<IDomainEvent> changes)
		{
			var sql = Events(conventions, "insert into {table} (aggregateID, sequence, event) values (@id, @sequence, @event::json);");

			foreach (var change in changes)
			{
				_connection.Execute(sql, new
				{
					ID = aggregateID,
					Sequence = change.Sequence,
					Event = JsonConvert.SerializeObject(change, _jsonSettings)
				});
			}
		}

		public IEnumerable<IDomainEvent> LoadEvents(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Events(conventions, "select event from {table} where aggregateID = @id order by sequence asc");

			return _connection
				.Query<string>(sql, new { ID = aggregateID })
				.Select(json => JsonConvert.DeserializeObject<IDomainEvent>(json, _jsonSettings))
				.ToList();
		}

		public IEnumerable<IDomainEvent> LoadEventsSince(IStoreConventions conventions, TKey aggregateID, int sequenceID)
		{
			var sql = Events(conventions, "select event from {table} where aggregateID = @id and sequence > @last order by sequence asc");

			return _connection
				.Query<string>(sql, new { ID = aggregateID, Last = sequenceID })
				.Select(json => JsonConvert.DeserializeObject<IDomainEvent>(json, _jsonSettings))
				.ToList();
		}

		public ISequenced LoadLatestSnapshotFor(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Snapshots(conventions, "select snapshot from {table} where aggregateID = @id order by sequence desc limit 1");

			return _connection
				.Query<string>(sql, new { ID = aggregateID })
				.Select(json => JsonConvert.DeserializeObject<ISequenced>(json, _jsonSettings))
				.FirstOrDefault();
		}

		public void SaveSnapshot(IStoreConventions conventions, TKey aggregateID, ISequenced snapshot)
		{
			var sql = Snapshots(conventions, "insert into {table} (aggregateID, sequence, snapshot) values (@id, @sequence, @snapshot::json);");

			_connection.Execute(sql, new
			{
				ID = aggregateID,
				Sequence = snapshot.Sequence,
				Snapshot = JsonConvert.SerializeObject(snapshot, _jsonSettings)
			});
		}

		public IEventStore<TKey> BeginTransaction()
		{
			if (_connection.State != ConnectionState.Open)
			{
				_connection.Open();
			}

			return new PostgresEventStore<TKey>(_connection, _connection.BeginTransaction());
		}

		public void Dispose()
		{
			if (_transaction != null)
			{
				_transaction.Commit();
			}

			_connection.Close();
		}
	}
}
