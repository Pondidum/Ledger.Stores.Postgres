using Ledger.Infrastructure;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class EventDto<TKey>
	{
		public int StreamSequence { get; set; }
		public string EventType { get; set; }
		public string Event { get; set; }

		public DomainEvent<TKey> Process(ITypeResolver typeResolver, JsonSerializerSettings jsonSettings)
		{
			var type = typeResolver.GetType(EventType);
			var domainEvent = (DomainEvent<TKey>)JsonConvert.DeserializeObject(Event, type, jsonSettings);

			domainEvent.StreamSequence = new StreamSequence(StreamSequence);

			return domainEvent;
		}
	}
}
