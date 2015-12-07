using System;
using Shouldly;

namespace Ledger.Stores.Postgres.Tests
{
	public static class Extensions
	{
		public static void ShouldMatch(this DateTime actual, DateTime expected)
		{
			actual = actual.AddTicks(-(actual.Ticks % TimeSpan.TicksPerMillisecond));
			expected = expected.AddTicks(-(expected.Ticks % TimeSpan.TicksPerMillisecond));

			ShouldBeTestExtensions.ShouldBe(actual, expected);
		}

		public static void ShouldMatch(this DateTime? actualDate, DateTime? expectedDate)
		{
			var actual = actualDate.Value;
			var expected = expectedDate.Value;

			actual = actual.AddTicks(-(actual.Ticks % TimeSpan.TicksPerMillisecond));
			expected = expected.AddTicks(-(expected.Ticks % TimeSpan.TicksPerMillisecond));

			ShouldBeTestExtensions.ShouldBe(actual, expected);
		}
	}
}
