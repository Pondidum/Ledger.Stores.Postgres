using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class PostgresStoreReader<TKey> : IStoreReader<TKey>
	{
		private readonly NpgsqlConnection _connection;
		private readonly NpgsqlTransaction _transaction;
		private readonly Func<string, string> _getEvents;
		private readonly Func<string, string> _getSnapshots;
		private readonly JsonSerializerSettings _jsonSettings;
		private readonly ITypeResolver _typeResolver;

		public PostgresStoreReader(NpgsqlConnection connection, NpgsqlTransaction transaction, Func<string, string> getEvents, Func<string, string> getSnapshots, JsonSerializerSettings jsonSettings, ITypeResolver typeResolver)
		{
			_connection = connection;
			_transaction = transaction;
			_getEvents = getEvents;
			_getSnapshots = getSnapshots;
			_jsonSettings = jsonSettings;
			_typeResolver = typeResolver;
		}

		public IEnumerable<DomainEvent<TKey>> LoadEvents(TKey aggregateID)
		{
			var sql = _getEvents("select streamSequence, eventType, event from {table} where aggregateID = @id order by sequence asc");

			return _connection
				.Query<EventDto<TKey>>(sql, new { ID = aggregateID }, _transaction)
				.Select(e => e.Process(_typeResolver, _jsonSettings));
		}

		public IEnumerable<DomainEvent<TKey>> LoadEventsSince(TKey aggregateID, Sequence? sequence)
		{
			var sql = _getEvents("select streamSequence, eventType, event from {table} where aggregateID = @id and sequence > @last order by sequence asc");

			return _connection
				.Query<EventDto<TKey>>(sql, new { ID = aggregateID, Last = (int)(sequence ?? Sequence.Start) }, _transaction)
				.Select(e => e.Process(_typeResolver, _jsonSettings));
		}

		public Snapshot<TKey> LoadLatestSnapshotFor(TKey aggregateID)
		{
			var sql = _getSnapshots("select snapshotType, snapshot from {table} where aggregateID = @id order by sequence desc limit 1");

			return _connection
				.Query<SnapshotDto<TKey>>(sql, new { ID = aggregateID }, _transaction)
				.Select(s => s.Process(_typeResolver, _jsonSettings))
				.FirstOrDefault();
		}

		public IEnumerable<TKey> LoadAllKeys()
		{
			var sql = _getEvents("select distinct aggregateID from {table} order by sequence");

			return _connection
				.Query<TKey>(sql);
		}

		public IEnumerable<DomainEvent<TKey>> LoadAllEvents()
		{
			var sql = _getEvents("select streamSequence, eventType, event from {table} order by sequence asc");

			return _connection
				.Query<EventDto<TKey>>(sql, _transaction, buffered: false)
				.Select(e => e.Process(_typeResolver, _jsonSettings));
		}

		public void Dispose()
		{
			_transaction.Commit();
			_connection.Close();
		}
	}
}
