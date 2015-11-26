using System;
using System.Data;
using Npgsql;
using Xunit;

namespace Ledger.Stores.Postgres.Tests
{
	public class RequiresPostgresFactAttribute : FactAttribute
	{
		public RequiresPostgresFactAttribute()
		{
			if (IsPostgresAvailable.Value == false)
			{
				Skip = "Postgres is not available";
			}
		}

		private static readonly Lazy<bool> IsPostgresAvailable;

		static RequiresPostgresFactAttribute()
		{
			IsPostgresAvailable = new Lazy<bool>(() =>
			{
				try
				{
					var builder = new NpgsqlConnectionStringBuilder(PostgresFixture.ConnectionString);
					builder.Timeout = 1000;

					using (var connection = new NpgsqlConnection(builder.ToString()))
					{
						connection.Open();
						return connection.State == ConnectionState.Open;
					}
				}
				catch (Exception)
				{
					return false;
				}
			});
		}
	}
}