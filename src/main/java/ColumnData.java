import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ColumnData {
    private final byte[] data;
    private final int serialType;

    public ColumnData(byte[] data, int serialType) {
        this.data = data;
        this.serialType = serialType;
    }

    @Override
    public String toString() {
        if (serialType == 0) {
            return "NULL";
        }
        // Text type (odd numbers >= 13)
        if (serialType >= 13 && serialType % 2 == 1) {
            return new String(data);
        }
        // Integer types
        if (serialType >= 1 && serialType <= 6) {
            return String.valueOf(asInt());
        }
        // Float type
        if (serialType == 7) {
            return String.valueOf(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getDouble());
        }
        // Boolean types
        if (serialType == 8) {
            return "0";
        }
        if (serialType == 9) {
            return "1";
        }
        // BLOB type (even numbers >= 12)
        if (serialType >= 12 && serialType % 2 == 0) {
            StringBuilder hex = new StringBuilder();
            for (byte b : data) {
                hex.append(String.format("%02x", b));
            }
            return "x'" + hex + "'";
        }
        return "";
    }

    public int asInt() {
        if (serialType == 0 || data.length == 0) {
            return 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);

        switch (serialType) {
            case 1: // 8-bit signed int
                return (int) data[0];
            case 2: // 16-bit signed int
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put(data);
                buffer.flip();
                return (int) buffer.getShort();
            case 3: // 24-bit signed int
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put(data);
                buffer.flip();
                return buffer.getInt();
            case 4: // 32-bit signed int
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put(data);
                buffer.flip();
                return buffer.getInt();
            case 5: // 48-bit signed int
                buffer.put((byte) 0);
                buffer.put((byte) 0);
                buffer.put(data);
                buffer.flip();
                return (int) buffer.getLong();
            case 6: // 64-bit signed int
                buffer.put(data);
                buffer.flip();
                return (int) buffer.getLong();
            case 8: // Boolean false
                return 0;
            case 9: // Boolean true
                return 1;
            default:
                throw new IllegalStateException("Cannot convert serialType " + serialType + " to int");
        }
    }

    public boolean isNull() {
        return serialType == 0;
    }

    public boolean isText() {
        return serialType >= 13 && serialType % 2 == 1;
    }

    public boolean isInteger() {
        return serialType >= 1 && serialType <= 6;
    }

    public boolean isFloat() {
        return serialType == 7;
    }

    public boolean isBlob() {
        return serialType >= 12 && serialType % 2 == 0;
    }

    public boolean isBoolean() {
        return serialType == 8 || serialType == 9;
    }

    public byte[] getRawData() {
        return data;
    }

    public int getSerialType() {
        return serialType;
    }
}