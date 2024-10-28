import java.io.IOException;
import java.util.List;

public class DatabaseReader {
    private final CommandExecutor commandExecutor;
    private final DatabaseFile databaseFile;

    public DatabaseReader(String databasePath) throws IOException {
        this.databaseFile = new DatabaseFile(databasePath);
        this.commandExecutor = new CommandExecutor(databaseFile);
    }

    public void executeCommand(String[] args) {
        if (args.length < 2) {
            System.out.println("Missing <database path> and <command>");
            return;
        }

        String command = args[1];
        try {
            commandExecutor.execute(command);
        } catch (IOException e) {
            System.err.println("Error executing command: " + e.getMessage());
        }
    }
}
