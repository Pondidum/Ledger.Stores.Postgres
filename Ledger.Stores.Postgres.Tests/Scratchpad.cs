using System;
using Npgsql;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace Ledger.Stores.Postgres.Tests
{
	public class Scratchpad
	{
		private readonly ITestOutputHelper _output;

		public Scratchpad(ITestOutputHelper output)
		{
			_output = output;
		}

		[Fact]
		public void Run()
		{
			using (var connection = new NpgsqlConnection(PostgresTestBase.ConnectionString))
			{
				connection.Open();
				var tx = connection.BeginTransaction();

				var writer = new PostgresStoreWriter<Guid>(
					connection, 
					tx, 
					sql => sql.Replace("{table}", "events_guid"),
					sql => sql.Replace("{table}", "snapshots_guid"));

				var latest = writer.GetLatestSequenceFor(Guid.NewGuid());

				_output.WriteLine(Convert.ToString(latest));

				latest.ShouldBe(null);
			}
				
        }
	}
}
