import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SqliteUtils {
    public static int readShort(FileInputStream file) throws IOException {
        byte[] bytes = new byte[2];
        file.read(bytes);
        return Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort());
    }

    public static int readVarint(FileInputStream file) throws IOException {
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

    public static int getVarintSize(int value) {
        int size = 1;
        while ((value >>= 7) != 0) size++;
        return size;
    }

    public static int getSerialTypeSize(int serialType) {
        if (serialType >= 1 && serialType <= 4) {
            return serialType;
        } else if (serialType >= 13) {
            return (serialType - 13) / 2;
        }
        return 0;
    }
}