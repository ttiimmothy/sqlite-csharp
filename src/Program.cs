using static System.Buffers.Binary.BinaryPrimitives;

var (path, command) = args.Length switch
{
  0 => throw new InvalidOperationException("Missing <database path> and <command>"),
  1 => throw new InvalidOperationException("Missing <command>"),
  _ => (args[0], args[1])
};

var databaseFile = File.OpenRead(path);

if (command == ".dbinfo")
{
  databaseFile.Seek(16, SeekOrigin.Begin);
  byte[] pageSizeBytes = new byte[2];
  databaseFile.Read(pageSizeBytes, 0, 2);
  var pageSize = ReadUInt16BigEndian(pageSizeBytes);
  Console.WriteLine($"database page size: {pageSize}");
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}
