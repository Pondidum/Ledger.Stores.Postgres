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

			if (_creators.TryGetValue(keyType, out create) == false)
			{
				var supported = string.Join(", ", _creators.Keys.Select(k => k.Name));
				throw new NotSupportedException($"Cannot create a '{keyType.Name}' aggregate keyed table, only '{supported}' are supported.");
			}

			create(stream);
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
