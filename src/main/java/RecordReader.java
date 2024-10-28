import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordReader {
    public static Record readRecord(FileInputStream file) throws IOException {
        int payloadSize = SqliteUtils.readVarint(file);
        SqliteUtils.readVarint(file); // Skip rowid

        int headerSize = SqliteUtils.readVarint(file);
        List<Integer> serialTypes = readSerialTypes(file, headerSize);
        List<ColumnData> columns = readColumns(file, serialTypes);

        return new Record(columns);
    }

    private static List<Integer> readSerialTypes(FileInputStream file, int headerSize) throws IOException {
        List<Integer> serialTypes = new ArrayList<>();
        int bytesRead = 0;
        while (bytesRead < headerSize - 1) {
            int serialType = SqliteUtils.readVarint(file);
            serialTypes.add(serialType);
            bytesRead += SqliteUtils.getVarintSize(serialType);
        }
        return serialTypes;
    }

    private static List<ColumnData> readColumns(FileInputStream file, List<Integer> serialTypes) throws IOException {
        List<ColumnData> columns = new ArrayList<>();
        for (int serialType : serialTypes) {
            ColumnData data;
            switch (serialType) {
                case 0 -> data = new ColumnData(new byte[0], serialType); // NULL
                case 1 -> data = readInteger(file, 1); // 8-bit signed int
                case 2 -> data = readInteger(file, 2); // 16-bit signed int
                case 3 -> data = readInteger(file, 3); // 24-bit signed int
                case 4 -> data = readInteger(file, 4); // 32-bit signed int
                case 5 -> data = readInteger(file, 6); // 48-bit signed int
                case 6 -> data = readInteger(file, 8); // 64-bit signed int
                case 7 -> data = readFloat(file); // 64-bit IEEE float
                case 8 -> data = new ColumnData(new byte[0], serialType); // 0
                case 9 -> data = new ColumnData(new byte[0], serialType); // 1
                default -> {
                    if (serialType >= 13 && serialType % 2 == 1) { // Text
                        int size = (serialType - 13) / 2;
                        byte[] bytes = new byte[size];
                        file.read(bytes);
                        data = new ColumnData(bytes, serialType);
                    } else if (serialType >= 12 && serialType % 2 == 0) { // BLOB
                        int size = (serialType - 12) / 2;
                        byte[] bytes = new byte[size];
                        file.read(bytes);
                        data = new ColumnData(bytes, serialType);
                    } else {
                        throw new IOException("Unsupported serial type: " + serialType);
                    }
                }
            }
            columns.add(data);
        }
        return columns;
    }

    private static ColumnData readInteger(FileInputStream file, int bytes) throws IOException {
        byte[] data = new byte[bytes];
        file.read(data);
        return new ColumnData(data, bytes);
    }

    private static ColumnData readFloat(FileInputStream file) throws IOException {
        byte[] data = new byte[8];
        file.read(data);
        return new ColumnData(data, 7);
    }
}
