using System;
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
		private readonly NpgsqlTransaction _transaction;

		public PostgresEventStore(NpgsqlConnection connection)
			: this(connection, null)
		{
		}

		private PostgresEventStore(NpgsqlConnection connection, NpgsqlTransaction transaction)
		{
			_connection = connection;
			_transaction = transaction;
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
			var sql = Events(conventions, "insert into {table} (aggregateID, sequence, eventType, event) values (@id, @sequence, @eventType, @event::json);");

			foreach (var change in changes)
			{
				_connection.Execute(sql, new
				{
					ID = aggregateID,
					Sequence = change.Sequence,
					EventType = change.GetType().FullName,
					Event = JsonConvert.SerializeObject(change)
				});
			}
		}

		public IEnumerable<IDomainEvent> LoadEvents(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Events(conventions, "select eventType, event from {table} where aggregateID = @id order by sequence asc");

			return _connection
				.Query<EventDto>(sql, new { ID = aggregateID })
				.Select(e => e.Process());
		}

		public IEnumerable<IDomainEvent> LoadEventsSince(IStoreConventions conventions, TKey aggregateID, int sequenceID)
		{
			var sql = Events(conventions, "select eventType, event from {table} where aggregateID = @id and sequence > @last order by sequence asc");

			return _connection
				.Query<EventDto>(sql, new {ID = aggregateID, Last = sequenceID})
				.Select(e => e.Process());
		}

		public ISequenced LoadLatestSnapshotFor(IStoreConventions conventions, TKey aggregateID)
		{
			var sql = Snapshots(conventions, "select snapshotType, snapshot from {table} where aggregateID = @id order by sequence desc limit 1");

			return _connection
				.Query<SnapshotDto>(sql, new { ID = aggregateID })
				.Select(s => s.Process())
				.FirstOrDefault();
		}

		public void SaveSnapshot(IStoreConventions conventions, TKey aggregateID, ISequenced snapshot)
		{
			var sql = Snapshots(conventions, "insert into {table} (aggregateID, sequence, snapshotType, snapshot) values (@id, @sequence, @snapshotType, @snapshot::json);");

			_connection.Execute(sql, new
			{
				ID = aggregateID,
				Sequence = snapshot.Sequence,
				SnapshotType = snapshot.GetType().FullName,
				Snapshot = JsonConvert.SerializeObject(snapshot)
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

		private class EventDto
		{
			public string EventType { get; set; }
			public string Event { get; set; }

			public IDomainEvent Process()
			{
				return (IDomainEvent) JsonConvert.DeserializeObject(Event, Type.GetType(EventType));
			}
		}

		private class SnapshotDto
		{
			public string SnapshotType { get; set; }
			public string Snapshot { get; set; }

			public ISequenced Process()
			{
				return (ISequenced)JsonConvert.DeserializeObject(Snapshot, Type.GetType(SnapshotType));
			}
		}
	}
}
