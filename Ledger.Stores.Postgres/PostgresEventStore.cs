using System.Collections.Generic;
using System.Data;
using Dapper;
using Ledger.Stores.Postgres.Infrastructure;
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

		private readonly string _connectionString;

		public PostgresEventStore(string connection)
		{
			_connectionString = connection;
		}

		public IStoreReader<TKey> CreateReader<TKey>(EventStoreContext context)
		{
			return new PostgresStoreReader<TKey>(
				_connectionString,
				sql => Events(context.StreamName, sql),
				sql => Snapshots(context.StreamName, sql),
				context.TypeResolver
			);
		}

		public IStoreWriter<TKey> CreateWriter<TKey>(EventStoreContext context)
		{
			return new PostgresStoreWriter<TKey>(
				_connectionString,
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
