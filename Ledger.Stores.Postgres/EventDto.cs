using Ledger.Infrastructure;
using Ledger.Stores.Postgres.Infrastructure;

namespace Ledger.Stores.Postgres
{
	internal class EventDto<TKey>
	{
		public int StreamSequence { get; set; }
		public string EventType { get; set; }
		public string Event { get; set; }

		public DomainEvent<TKey> Process(ITypeResolver typeResolver)
		{
			var type = typeResolver.GetType(EventType);
			var domainEvent = (DomainEvent<TKey>)Serializer.Deserialize(Event, type);

			domainEvent.StreamSequence = new StreamSequence(StreamSequence);

			return domainEvent;
		}
	}
}
