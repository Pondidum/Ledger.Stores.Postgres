using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;

namespace Ledger.Stores.Postgres
{
	public class TableBuilder
	{
		private readonly Dictionary<Type, Action<IStoreConventions>> _creators;

		public TableBuilder(NpgsqlConnection connection)
		{
			_creators = new Dictionary<Type, Action<IStoreConventions>>
			{
				{typeof (Guid), conventions => new CreateGuidAggregateTablesCommand(connection).Execute(conventions)},
				{typeof (int), conventions => new CreateIntAggregateTablesCommand(connection).Execute(conventions)}
			};
		}

		public void CreateTable(IStoreConventions conventions)
		{
			Action<IStoreConventions> create;

			if (_creators.TryGetValue(conventions.KeyType, out create))
			{
				create(conventions);
			}

			throw new NotSupportedException(string.Format(
				"Cannot create a '{0}' aggregate keyed table, only '{1}' are supported.", 
				conventions.KeyType.Name,
				string.Join(", ", _creators.Keys.Select(k => k.Name))));
		}
	}
}
