import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommandExecutor {
    private final DatabaseFile databaseFile;
    private final SchemaReader schemaReader;
    private final TableReader tableReader;

    public CommandExecutor(DatabaseFile databaseFile) {
        this.databaseFile = databaseFile;
        this.schemaReader = new SchemaReader(databaseFile);
        this.tableReader = new TableReader(databaseFile);
    }

    public void execute(String command) throws IOException {
        if (command.startsWith(".")) {
            switch (command) {
                case ".dbinfo" -> executeDbInfo();
                case ".tables" -> executeTables();
                default -> System.out.println("Unknown command: " + command);
            }
        } else {
            executeQuery(command);
        }
    }
    private void executeQuery(String sql) throws IOException {
        SqlQuery query = SqlQuery.parse(sql);
        if (query.type().equals("SELECT")) {
            executeSelect(query);
        } else {
            System.out.println("Unsupported query type: " + query.type());
        }
    }
    private void executeSelect(SqlQuery query) throws IOException {
        if (query.columns().size() == 1 && query.columns().get(0).equals("count(*)")) {
            // Handle COUNT(*) differently
            int rootPage = schemaReader.findRootPage(query.tableName());
            int rowCount = tableReader.countRows(rootPage);
            System.out.println(rowCount);
            return;
        }
        // Get table schema
        String createTableSql = schemaReader.getCreateTableSql(query.tableName());
        TableSchema schema = TableSchema.parse(createTableSql);

        // Get table data
        int rootPage = schemaReader.findRootPage(query.tableName());
        List<Record> records = tableReader.readTableRows(rootPage);
        //System.out.println("size of record"+records.size());
        // Output requested columns
        for (Record record : records) {
            List<String> values = new ArrayList<>();
            for (String columnName : query.columns()) {
                int columnIndex = schema.getColumnIndex(columnName);
                //System.out.println(record.getColumnData(columnIndex));
                values.add(record.getColumnData(columnIndex).toString());
            }
            System.out.println(String.join("|", values));
        }
    }
    private void executeDbInfo() throws IOException {
        int tableCount = schemaReader.getTableCount();
        System.out.println("database page size: " + databaseFile.getPageSize());
        System.out.println("number of tables: " + tableCount);
    }

    private void executeTables() throws IOException {
        List<String> tableNames = schemaReader.getTableNames();
        System.out.print(String.join(" ", tableNames));
    }

    private void executeSelect(String command) throws IOException {
        if (command.startsWith("select count(*) from")) {
            String tableName = command.split(" ")[3];
            int rootPage = schemaReader.findRootPage(tableName);
            int rowCount = tableReader.countRows(rootPage);
            System.out.println(rowCount);
        } else {
            System.out.println("Invalid select command: " + command);
        }
    }
}
