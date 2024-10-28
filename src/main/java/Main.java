import java.io.IOException;

// cd /mnt/c/Users/surya/IdeaProjects/codecrafters-sqlite-java
public class Main {
  public static void main(String[] args){
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      return;
    }
    try {
      DatabaseReader reader = new DatabaseReader(args[0]);
      reader.executeCommand(args);
    } catch (IOException e) {
      System.err.println("Error: " + e.getMessage());
    }

  }
}
