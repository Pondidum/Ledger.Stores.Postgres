using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class TableBuilder
	{
		private readonly Dictionary<Type, Action<string>> _creators;

		public TableBuilder(NpgsqlConnection connection)
		{
			_creators = new Dictionary<Type, Action<string>>
			{
				{typeof (Guid), stream => new CreateGuidAggregateTablesCommand(connection).Execute(stream)},
				{typeof (int), stream => new CreateIntAggregateTablesCommand(connection).Execute(stream)}
			};
		}

		public void CreateTable(Type keyType, string stream)
		{
			Action<string> create;

			if (_creators.TryGetValue(keyType, out create))
			{
				create(stream);
			}

			throw new NotSupportedException(string.Format(
				"Cannot create a '{0}' aggregate keyed table, only '{1}' are supported.", 
				keyType.Name,
				string.Join(", ", _creators.Keys.Select(k => k.Name))));
		}

		public static string EventsName(string stream)
		{
			return stream + "_events";
		}

		public static string SnapshotsName(string stream)
		{
			return stream + "_snapshots";
		}
	}
}
