using codecrafters_sqlite.Utils;
namespace codecrafters_sqlite.Databases;
public class PageHelper
{
  public static byte[] ReadPage(string path, int pageSize, long pageNumber)
  {
    return FileHelper.ReadBytes(path, (pageNumber - 1) * pageSize, pageSize);
  }
}