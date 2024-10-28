import java.util.List;

public class Record {
    private final List<ColumnData> columns;

    public Record(List<ColumnData> columns) {
        this.columns = columns;
    }

    public ColumnData getColumnData(int index) {
        return columns.get(index);
    }
}
