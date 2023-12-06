using codecrafters_sqlite.Utils;
using static System.Text.Encoding;
using static System.Buffers.Binary.BinaryPrimitives;

namespace codecrafters_sqlite.Databases;

public record Record(string[] Columns, long RowId)
{
  public static Record Parse(ReadOnlyMemory<byte> stream, long rowId) => new(ParseColumns(stream).ToArray(), rowId);

  private static IEnumerable<string> ParseColumns(ReadOnlyMemory<byte> stream)
  {
    var (headerSize, headerOffset) = VarInt.Parse(stream);
    var contentOffset = (int)headerSize;

    while (headerOffset < headerSize)
    {
      var (serialType, bytesRead) = Varint.Parse(stream[headerOffset..]);
      var column = ParseColumnValue((int)serialType, stream[contentOffset..]);
      yield return column.Item1;
      headerOffset += bytesRead;
      contentOffset += column.Item2;
    }
  }

  private static Tuple<string, int> ParseColumnValue(int serialType, ReadOnlyMemory<byte> stream) => serialType switch
  {
    0 => Tuple.Create<string, int>("", 0),
    1 => Tuple.Create(stream[0..1].Span[0].ToString(), 1),
    2 => Tuple.Create(ReadUInt16BigEndian(stream[0..2].Span).ToString(), 2),
    3 => Tuple.Create(Get24BitInteger(stream[0..3].Span.ToArray()), 3),
    8 => Tuple.Create<string, int>("0", 0),
    9 => Tuple.Create<string, int>("1", 0),
    var t when IsText(t) => Tuple.Create(UTF8.GetString(stream[..GetTextLen(t)].Span), GetTextLen(t)),
    var t => throw new NotSupportedException($"Invalid serial type: {t}")
  };

  private static bool IsText(int serialType) => serialType >= 13 && serialType % 2 == 1;

  private static int GetTextLen(int serialType) => (serialType - 13) / 2;

  private static string Get24BitInteger(byte[] data)
  {
    return ReadUInt32BigEndian(new byte[1] { 0 }.Concat(data).ToArray()).ToString();
  }
}