using System;
using System.Collections.Generic;
using Dapper;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresStoreWriter<TKey> : IStoreWriter<TKey>
	{
		private readonly NpgsqlConnection _connection;
		private readonly NpgsqlTransaction _transaction;
		private readonly Func<string, string> _getEvents;
		private readonly Func<string, string> _getSnapshots;
		private readonly JsonSerializerSettings _jsonSettings;

		public PostgresStoreWriter(NpgsqlConnection connection, NpgsqlTransaction transaction, Func<string, string> getEvents, Func<string, string> getSnapshots, JsonSerializerSettings jsonSettings)
		{
			_connection = connection;
			_transaction = transaction;
			_getEvents = getEvents;
			_getSnapshots = getSnapshots;
			_jsonSettings = jsonSettings;
		}

		public Sequence? GetLatestSequenceFor(TKey aggregateID)
		{
			var sql = _getEvents("select max(sequence) from {table} where aggregateID = @id");
			var value = _connection.ExecuteScalar<int?>(sql, new { ID = aggregateID }, _transaction);

			return value.HasValue
				? new Sequence(value.Value) 
				: (Sequence?)null;
		}

		public int GetNumberOfEventsSinceSnapshotFor(TKey aggregateID)
		{
			var sql = 
				"select count(*) " +
				"from {events_table} e " +
				"join {snapshots_table} s on s.aggregateid = e.aggregateid " +
				"where e.sequence > s.sequence " +
				"  and e.aggregateid = @id";

			sql = _getEvents(_getSnapshots(sql));

			return _connection.ExecuteScalar<int>(sql, new { ID = aggregateID }, _transaction);
		}

		public void SaveEvents(IEnumerable<DomainEvent<TKey>> changes)
		{
			var sql = _getEvents(
				"insert into {table} (aggregateID, sequence, eventType, event) " +
				"values (@id, @sequence, @eventType, @event::json)" +
				"returning streamSequence;");

			foreach (var change in changes)
			{
				var dto = new
				{
					ID = change.AggregateID,
					Sequence = (int)change.Sequence,
					EventType = change.GetType().AssemblyQualifiedName,
					Event = Serializer.Serialize(change)
				};

				var sequence = _connection.ExecuteScalar<int>(sql, dto, _transaction);
				change.StreamSequence = new StreamSequence(sequence);
			}
		}

		public void SaveSnapshot(Snapshot<TKey> snapshot)
		{
			var sql = _getSnapshots(
				"insert into {table} (aggregateID, sequence, snapshotType, snapshot) " +
				"values (@id, @sequence, @snapshotType, @snapshot::json);");

			var dto = new
			{
				ID = snapshot.AggregateID,
				Sequence = (int)snapshot.Sequence,
				SnapshotType = snapshot.GetType().AssemblyQualifiedName,
				Snapshot = Serializer.Serialize(snapshot)
			};

			_connection.Execute(sql, dto, _transaction);
		}

		public void Dispose()
		{
			_transaction.Commit();
			_connection.Close();
		}
	}
}
