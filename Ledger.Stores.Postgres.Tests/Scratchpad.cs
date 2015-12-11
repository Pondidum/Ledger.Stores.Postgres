using System;
using System.Linq;
using Ledger.Acceptance.TestObjects;
using Ledger.Infrastructure;
using Newtonsoft.Json;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Ledger.Stores.Postgres.Tests
{
	public class Scratchpad
	{
		public const string StreamName = "ImportStream";

		private readonly ITestOutputHelper _output;

		public Scratchpad(ITestOutputHelper output)
		{
			_output = output;
		}

		[Fact]
		public void When_testing_something()
		{
			

		}
	}
}
