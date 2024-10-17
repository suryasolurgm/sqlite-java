import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
      case ".tables" -> {
        try (FileInputStream databaseFile = new FileInputStream(new File(databaseFilePath))) {
          int pageSize = readPageSize(databaseFile);
         // System.out.println("Page size: " + pageSize);
          List<String> tableNames = readTableNames(databaseFile, pageSize);

          for (String tableName : tableNames) {
            System.out.print(tableName + " ");
          }
          //System.out.println();
        } catch (IOException e) {
          System.out.println("Error reading file: " + e.getMessage());
        }
      }
      default -> System.out.println("Missing or invalid command passed: " + command);
    }
  }
  private static int readPageSize(FileInputStream file) throws IOException {
    file.skip(16);
    byte[] pageSizeBytes = new byte[2];
    file.read(pageSizeBytes);
    return Short.toUnsignedInt(ByteBuffer.wrap(pageSizeBytes).getShort());
  }

  private static int readCellCount(FileInputStream file) throws IOException {
    byte[] cellCountBytes = new byte[2];
    file.read(cellCountBytes);
    return ByteBuffer.wrap(cellCountBytes).getShort();
  }

  private static List<String> readTableNames(FileInputStream file, int pageSize) throws IOException {
    file.skip(100 - 18);
    file.skip(3);
    int cellCount = readCellCount(file);
    System.out.println("cellCount: " + cellCount);
    file.skip(3);

    List<Integer> cellOffsets = readCellOffsets(file, cellCount);
    List<String> tableNames = new ArrayList<>();

    for (int offset : cellOffsets) {
      //file.skip(offset - (100 + 8 + cellCount * 2));
      file.getChannel().position(0);
      file.skip(offset);
      tableNames.add(readTableNameFromCell(file));
    }

    return tableNames;
  }

  private static List<Integer> readCellOffsets(FileInputStream file, int cellCount) throws IOException {
    List<Integer> offsets = new ArrayList<>();
    for (int i = 0; i < cellCount; i++) {
      byte[] offsetBytes = new byte[2];
      file.read(offsetBytes);
      offsets.add(Short.toUnsignedInt(ByteBuffer.wrap(offsetBytes).getShort()));
      System.out.println(offsets.get(i));
    }
    return offsets;
  }

  private static String readTableNameFromCell(FileInputStream file) throws IOException {
    int payloadSize = readVarint(file);
    readVarint(file); // Skip rowid

    int headerSize = readVarint(file);
    List<Integer> serialTypes = new ArrayList<>();
    int bytesRead = 0;
    while (bytesRead < headerSize - 1) {
      int serialType = readVarint(file);
      serialTypes.add(serialType);
      bytesRead += getVarintSize(serialType);
    }

    // Skip to the third column (tbl_name)
    for (int i = 0; i < 2; i++) {
      int size = getSerialTypeSize(serialTypes.get(i));
      file.skip(size);
    }

    // Read tbl_name
    int tableNameSize = getSerialTypeSize(serialTypes.get(2));
    byte[] tableNameBytes = new byte[tableNameSize];
    file.read(tableNameBytes);
    return new String(tableNameBytes);
  }

  private static int readVarint(FileInputStream file) throws IOException {
    int value = 0;
    int shift = 0;
    while (true) {
      int b = file.read();
      value |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return value;
  }

  private static int getVarintSize(int value) {
    int size = 1;
    while ((value >>= 7) != 0) size++;
    return size;
  }

  private static int getSerialTypeSize(int serialType) {
    if (serialType >= 1 && serialType <= 4) {
      return serialType;
    } else if (serialType >= 13) {
      return (serialType - 13) / 2;
    }
    return 0; // Other types not needed for this implementation
  }
}
