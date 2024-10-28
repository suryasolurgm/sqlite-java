import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record SqlQuery(
        String type,
        String tableName,
        List<String> columns,
        List<String> conditions
) {
    public static SqlQuery parse(String sql) {
        String[] parts = sql.split("\\s+");
        String type = parts[0].toUpperCase();
        List<String> columns = new ArrayList<>();

        if (type.equals("SELECT")) {
            int fromIndex = -1;
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].equalsIgnoreCase("FROM")) {
                    fromIndex = i;
                    break;
                }
            }

            String columnsStr = String.join(" ", Arrays.copyOfRange(parts, 1, fromIndex));
            if (columnsStr.toLowerCase().equals("count(*)")) {
                columns.add("count(*)");
            } else {
                columns = Arrays.asList(columnsStr.split(",\\s*"));
            }
            String tableName = parts[fromIndex + 1];
            return new SqlQuery(type, tableName, columns, new ArrayList<>());
        }

        throw new IllegalArgumentException("Unsupported query type: " + type);
    }
}
