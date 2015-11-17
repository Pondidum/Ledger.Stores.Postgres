using System;
using Newtonsoft.Json;

namespace Ledger.Stores.Postgres
{
	internal class EventDto
	{
		public string EventType { get; set; }
		public string Event { get; set; }

		public IDomainEvent Process()
		{
			return (IDomainEvent)JsonConvert.DeserializeObject(Event, Type.GetType(EventType));
		}
	}
}
