using System.Data;
using Dapper;
using Ledger.Infrastructure;

namespace Ledger.Stores.Postgres.Infrastructure
{
	public class SequenceTypeHandler : SqlMapper.TypeHandler<Sequence>
	{
		public override void SetValue(IDbDataParameter parameter, Sequence value)
		{
			parameter.Value = (int)value;
		}

		public override Sequence Parse(object value)
		{
			return new Sequence((int)value);
		}
	}
}
