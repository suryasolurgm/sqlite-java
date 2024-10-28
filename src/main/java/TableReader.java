import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReader {
    private final DatabaseFile databaseFile;

    public TableReader(DatabaseFile databaseFile) {
        this.databaseFile = databaseFile;
    }

    public int countRows(int rootPage) throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position((rootPage - 1) * databaseFile.getPageSize());
        file.skip(3);
        return SqliteUtils.readShort(file);
    }
    public List<Record> readTableRows(int rootPage) throws IOException {
        FileInputStream file = databaseFile.getInputStream();
        file.getChannel().position((rootPage - 1) * databaseFile.getPageSize());

        // Read page header
        file.skip(3);
        int cellCount = SqliteUtils.readShort(file);
        file.skip(3);

        // Read cell offsets
        List<Integer> cellOffsets = new ArrayList<>();
        for (int i = 0; i < cellCount; i++) {
            cellOffsets.add(SqliteUtils.readShort(file));
        }

        // Read records
        List<Record> records = new ArrayList<>();
        for (int offset : cellOffsets) {
            file.getChannel().position((rootPage - 1) * databaseFile.getPageSize() + offset);
            records.add(RecordReader.readRecord(file));
        }

        return records;
    }

}
