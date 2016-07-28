using Ledger.Stores.Postgres.Infrastructure;

namespace Ledger.Stores.Postgres
{
	internal class SnapshotDto<TKey>
	{
		public string SnapshotType { get; set; }
		public string Snapshot { get; set; }

		public Snapshot<TKey> Process(ITypeResolver typeResolver)
		{
			return (Snapshot<TKey>)Serializer.Deserialize(Snapshot, typeResolver.GetType(SnapshotType));
		}
	}
}
