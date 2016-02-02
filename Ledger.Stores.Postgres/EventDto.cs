using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class EventDto<TKey>
	{
		public string EventType { get; set; }
		public string Event { get; set; }

		public DomainEvent<TKey> Process()
		{
			return (DomainEvent<TKey>)JsonConvert.DeserializeObject(Event, Type.GetType(EventType));
		}
	}
}
