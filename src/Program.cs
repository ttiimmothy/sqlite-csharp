using codecrafters_sqlite.Databases;
using codecrafters_sqlite.Query;

var (path, command) = args.Length switch
{
  0 => throw new InvalidOperationException("Missing <database path> and <command>"),
  1 => throw new InvalidOperationException("Missing <command>"),
  _ => (args[0], args[1])
};

var database = new Database(path);
var queryParser = new QueryParser();
var queryOptimizer = new QueryOptimizer(database);
var queryExecutor = new QueryExecutor(database, queryOptimizer, queryParser);

if (command == ".dbinfo")
{
  Console.WriteLine($"number of tables: {database.GetTablesCount()}");
}
else if (command == ".tables")
{
  Console.WriteLine(string.Join(' ', database.Schemata.Select(s => s.TableName)));
}
else
{
  var query = command.Replace("\"", "");
  queryExecutor.Execute(query);
}