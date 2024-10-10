import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
// cd /mnt/c/Users/surya/IdeaProjects/codecrafters-sqlite-java
public class Main {
  public static void main(String[] args){
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }

    String databaseFilePath = args[0];
    String command = args[1];

    switch (command) {
      case ".dbinfo" -> {
        try {
          FileInputStream databaseFile = new FileInputStream(new File(databaseFilePath));
          
          databaseFile.skip(16); // Skip the first 16 bytes of the header
          byte[] pageSizeBytes = new byte[2]; // The following 2 bytes are the page size
          databaseFile.read(pageSizeBytes);
          short pageSizeSigned = ByteBuffer.wrap(pageSizeBytes).getShort();
          int pageSize = Short.toUnsignedInt(pageSizeSigned);

          // Read the number of cells from the sqlite_schema page header
          databaseFile.skip(100 - 18);// Skip to the start of the sqlite_schema page header
          databaseFile.skip(3);
          byte[] cellCountBytes = new byte[2]; // The cell count is a 2-byte big-endian value
          databaseFile.read(cellCountBytes);
          int cellCount = ByteBuffer.wrap(cellCountBytes).getShort();
          // You can use print statements as follows for debugging, they'll be visible when running tests.
          System.out.println("Logs from your program will appear here!");

          // Uncomment this block to pass the first stage
           System.out.println("database page size: " + pageSize);
           System.out.println("number of tables: " + cellCount);
        } catch (IOException e) {
          System.out.println("Error reading file: " + e.getMessage());
        }
      }
      default -> System.out.println("Missing or invalid command passed: " + command);
    }
  }
}
