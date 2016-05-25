using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class EventDto<TKey>
	{
		public string EventType { get; set; }
		public string Event { get; set; }

		public DomainEvent<TKey> Process(ITypeResolver typeResolver)
		{
			return (DomainEvent<TKey>)JsonConvert.DeserializeObject(Event, typeResolver.GetType(EventType));
		}
	}
}
