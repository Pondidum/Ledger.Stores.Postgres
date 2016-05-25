using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class SnapshotDto<TKey>
	{
		public string SnapshotType { get; set; }
		public string Snapshot { get; set; }

		public Snapshot<TKey> Process(ITypeResolver typeResolver, JsonSerializerSettings jsonSettings)
		{
			return (Snapshot<TKey>)JsonConvert.DeserializeObject(Snapshot, typeResolver.GetType(SnapshotType), jsonSettings);
		}
	}
}
