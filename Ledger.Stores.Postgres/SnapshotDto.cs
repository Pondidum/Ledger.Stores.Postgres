using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class SnapshotDto
	{
		public string SnapshotType { get; set; }
		public string Snapshot { get; set; }

		public ISequenced Process()
		{
			return (ISequenced)JsonConvert.DeserializeObject(Snapshot, Type.GetType(SnapshotType));
		}
	}
}
