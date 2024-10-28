import java.io.*;
import java.nio.ByteBuffer;

public class DatabaseFile implements AutoCloseable {
    private final FileInputStream fileInputStream;
    private final int pageSize;

    public DatabaseFile(String path) throws IOException {
        this.fileInputStream = new FileInputStream(new File(path));
        this.pageSize = readPageSize();
    }

    public int getPageSize() {
        return pageSize;
    }

    public FileInputStream getInputStream() {
        return fileInputStream;
    }

    private int readPageSize() throws IOException {
        fileInputStream.skip(16);
        byte[] pageSizeBytes = new byte[2];
        fileInputStream.read(pageSizeBytes);
        return Short.toUnsignedInt(ByteBuffer.wrap(pageSizeBytes).getShort());
    }

    @Override
    public void close() throws IOException {
        fileInputStream.close();
    }
}
