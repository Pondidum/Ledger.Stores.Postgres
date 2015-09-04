# Ledger.Stores.Postgres

Provides a [PostgreSQL][postgres] connector for [Ledger][github-ledger].

## Usage

```csharp
//or wherever your connection string is stored
var cs = ConfigurationManager.ConnectionStrings["Postgres"].ConnectionString;

//PostgresEventStore will open and close this connection object as it needs.
//it will *NOT* dispose it.
var eventStore = new PostgresEventStore<Guid>(new NpgsqlConnection(cs)));
var aggregateStore = new AggregateStore<Guid>(eventStore);

//optional: will create an events table and a snapshots table if
//they don't already exist.
aggregateStore.CreateTable();

var person = aggregateStore.Load(id, () => new Person());
```

## Customisation

By default, `PostgresEventStore` will use a table naming convention of `events_{keytype}` and `snapshots_{keytype}`, which is provided by the `KeyTypeTableName` class.

To change the convention just create a new implementation of `ITableName` and pass it to the `PostgresEventStore`:

```csharp
public class PrefixedTableName : ITableName
{
  private readonly string _prefix;

  public PrefixedTableName(string prefix)
  {
    _prefix = prefix;
  }

	public string ForEvents<TKey>()
	{
		return _prefix + "_events_" + typeof(TKey).Name.ToLower();
	}

	public string ForSnapshots<TKey>()
	{
		return _prefix + "_snapshots_" + typeof(TKey).Name.ToLower();
	}
}

//usage
var eventStore = new PostgresEventStore<Guid>(connection, new PrefixedTableName("test"));
```

[postgres]: http://www.postgresql.org
[github-ledger]: https://github.com/Pondidum/Ledger
