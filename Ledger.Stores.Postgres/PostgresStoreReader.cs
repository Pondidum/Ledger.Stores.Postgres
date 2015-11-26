﻿using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresStoreReader<TKey> : IStoreReader<TKey>
	{
		private readonly NpgsqlConnection _connection;
		private readonly NpgsqlTransaction _transaction;
		private readonly Func<string, string> _getEvents;
		private readonly Func<string, string> _getSnapshots;

		public PostgresStoreReader(NpgsqlConnection connection, NpgsqlTransaction transaction, Func<string, string> getEvents, Func<string, string> getSnapshots)
		{
			_connection = connection;
			_transaction = transaction;
			_getEvents = getEvents;
			_getSnapshots = getSnapshots;
		}

		public IEnumerable<IDomainEvent<TKey>> LoadEvents(TKey aggregateID)
		{
			var sql = _getEvents("select eventType, event from {table} where aggregateID = @id order by sequence asc");

			return _connection
				.Query<EventDto<TKey>>(sql, new { ID = aggregateID }, _transaction)
				.Select(e => e.Process());
		}

		public IEnumerable<IDomainEvent<TKey>> LoadEventsSince(TKey aggregateID, int sequenceID)
		{
			var sql = _getEvents("select eventType, event from {table} where aggregateID = @id and sequence > @last order by sequence asc");

			return _connection
				.Query<EventDto<TKey>>(sql, new { ID = aggregateID, Last = sequenceID }, _transaction)
				.Select(e => e.Process());
		}

		public ISnapshot<TKey> LoadLatestSnapshotFor(TKey aggregateID)
		{
			var sql = _getSnapshots("select snapshotType, snapshot from {table} where aggregateID = @id order by sequence desc limit 1");

			return _connection
				.Query<SnapshotDto<TKey>>(sql, new { ID = aggregateID }, _transaction)
				.Select(s => s.Process())
				.FirstOrDefault();
		}

		public IEnumerable<TKey> LoadAllKeys()
		{
			var sql = _getEvents("select distinct aggregateID from {table} order by timestamp");

			return _connection
				.Query<TKey>(sql);
		}

		public void Dispose()
		{
			_transaction.Commit();
			_connection.Close();
		}
	}
}
