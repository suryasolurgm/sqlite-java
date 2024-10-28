import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SchemaReader {
    private static final int SCHEMA_PAGE_HEADER_OFFSET = 100;
    private final DatabaseFile databaseFile;

    public SchemaReader(DatabaseFile databaseFile) {
        this.databaseFile = databaseFile;
    }

    public int getTableCount() throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position(SCHEMA_PAGE_HEADER_OFFSET);
        file.skip(3);
        return SqliteUtils.readShort(file);
    }
    public String getCreateTableSql(String tableName) throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position(SCHEMA_PAGE_HEADER_OFFSET);
        file.skip(3);

        int cellCount = SqliteUtils.readShort(file);
        file.skip(3);

        List<Integer> cellOffsets = readCellOffsets(file, cellCount);

        for (int offset : cellOffsets) {
            file.getChannel().position(offset);
            SchemaEntry entry = readSchemaEntry(file);
            if (entry.name().equals(tableName)) {
                return entry.sql();
            }
        }

        throw new IOException("Table not found: " + tableName);
    }
    public List<String> getTableNames() throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position(SCHEMA_PAGE_HEADER_OFFSET);
        file.skip(3);

        int cellCount = SqliteUtils.readShort(file);
        file.skip(3);

        List<Integer> cellOffsets = readCellOffsets(file, cellCount);
        List<String> tableNames = new ArrayList<>();

        for (int offset : cellOffsets) {
            file.getChannel().position(offset);
            tableNames.add(readTableNameFromCell(file));
        }

        return tableNames;
    }

    public int findRootPage(String tableName) throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position(SCHEMA_PAGE_HEADER_OFFSET);
        file.skip(3);

        int cellCount = SqliteUtils.readShort(file);
        file.skip(3);

        List<Integer> cellOffsets = readCellOffsets(file, cellCount);

        for (int offset : cellOffsets) {
            file.getChannel().position(offset);
            SchemaEntry entry = readSchemaEntry(file);
            if (entry.name().equals(tableName)) {
                return entry.rootPage();
            }
        }

        throw new IOException("Table not found: " + tableName);
    }

    private List<Integer> readCellOffsets(FileInputStream file, int cellCount) throws IOException {
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i < cellCount; i++) {
            offsets.add(SqliteUtils.readShort(file));
        }
        return offsets;
    }

    private String readTableNameFromCell(FileInputStream file) throws IOException {
        Record record = RecordReader.readRecord(file);
        return record.getColumnData(2).toString();
    }

    private SchemaEntry readSchemaEntry(FileInputStream file) throws IOException {
        Record record = RecordReader.readRecord(file);
        return new SchemaEntry(
                record.getColumnData(1).toString(),  // name
                record.getColumnData(2).toString(),  // tbl_name
                record.getColumnData(3).asInt(),      // rootpage
                record.getColumnData(4).toString()   // sql
        );
    }
}
