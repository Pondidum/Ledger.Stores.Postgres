using System;
using System.Collections.Generic;
using Dapper;
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

		public PostgresStoreWriter(NpgsqlConnection connection, NpgsqlTransaction transaction, Func<string, string> getEvents, Func<string, string> getSnapshots)
		{
			_connection = connection;
			_transaction = transaction;
			_getEvents = getEvents;
			_getSnapshots = getSnapshots;
		}

		public int? GetLatestSequenceFor(TKey aggregateID)
		{
			var sql = _getEvents("select max(sequence) from {table} where aggregateID = @id");

			return _connection.ExecuteScalar<int?>(sql, new { ID = aggregateID }, _transaction);
		}

		public int GetNumberOfEventsSinceSnapshotFor(TKey aggregateID)
		{
			var sql = @"
select count(*)
from {events_table} e
join {snapshots_table} s on s.aggregateid = e.aggregateid
where e.sequence > s.sequence
  and e.aggregateid = @id";

			sql = _getEvents(_getSnapshots(sql));

			return _connection.ExecuteScalar<int>(sql, new { ID = aggregateID }, _transaction);
		}

		public void SaveEvents(IEnumerable<DomainEvent<TKey>> changes)
		{
			var sql = _getEvents("insert into {table} (aggregateID, stamp, sequence, eventType, event) values (@id, @stamp, @sequence, @eventType, @event::json);");

			foreach (var change in changes)
			{
				var dto = new
				{
					ID = change.AggregateID,
					Stamp = change.Stamp,
					Sequence = change.Sequence,
					EventType = change.GetType().AssemblyQualifiedName,
					Event = JsonConvert.SerializeObject(change)
				};

				_connection.Execute(sql, dto, _transaction);
			}
		}

		public void SaveSnapshot(Snapshot<TKey> snapshot)
		{
			var sql = _getSnapshots("insert into {table} (aggregateID, stamp, sequence, snapshotType, snapshot) values (@id, @stamp, @sequence, @snapshotType, @snapshot::json);");

			var dto = new
			{
				ID = snapshot.AggregateID,
				Stamp = snapshot.Stamp,
				Sequence = snapshot.Sequence,
				SnapshotType = snapshot.GetType().AssemblyQualifiedName,
				Snapshot = JsonConvert.SerializeObject(snapshot)
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
