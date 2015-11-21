using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class SnapshotDto<TKey>
	{
		public string SnapshotType { get; set; }
		public string Snapshot { get; set; }

		public ISnapshot<TKey> Process()
		{
			return (ISnapshot<TKey>)JsonConvert.DeserializeObject(Snapshot, Type.GetType(SnapshotType));
		}
	}
}
