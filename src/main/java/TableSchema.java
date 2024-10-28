import java.util.ArrayList;
import java.util.List;

public class TableSchema {
    private final String tableName;
    private final List<ColumnDefinition> columns;

    public TableSchema(String tableName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    public static TableSchema parse(String createTableSql) {
        // Remove CREATE TABLE and table name
        int startBracket = createTableSql.indexOf('(');
        int endBracket = createTableSql.lastIndexOf(')');
        String columnDefinitions = createTableSql.substring(startBracket + 1, endBracket).trim();

        String tableName = createTableSql.substring(
                "CREATE TABLE ".length(),
                createTableSql.indexOf('(')
        ).trim();

        List<ColumnDefinition> columns = new ArrayList<>();
        String[] columnDefs = columnDefinitions.split(",");

        for (String columnDef : columnDefs) {
            columnDef = columnDef.trim();
            String[] parts = columnDef.split("\\s+");
            String columnName = parts[0];
            String columnType = parts.length > 1 ? parts[1] : "TEXT";
            columns.add(new ColumnDefinition(columnName, columnType));
        }

        return new TableSchema(tableName, columns);
    }
}
