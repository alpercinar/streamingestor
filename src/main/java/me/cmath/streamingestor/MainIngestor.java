package me.cmath.streamingestor;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;


public class MainIngestor {

  public static final String APP_NAME = "StreamIngestor";
  public static int serverPort = 18000;
  public static int throughPut = 10000;
  public static int duration = 3000;
  public static String dataSourceFile =
      "/Users/christian/Documents/Workspace/s-store/tpcdidata/withdebug/Batch1/Trade.txt";

  public static void startServer() {
    try {
      ServerSocket listenSocket = new ServerSocket(serverPort);

      System.out.println("Server started on port " + serverPort + "..");

      while (true) {
        Socket clientSocket = listenSocket.accept();
        StreamServer c = new StreamServer(clientSocket, throughPut, duration, dataSourceFile);
      }
    } catch (IOException e) {
      System.out.println("Error: " + e.getMessage());
    }
  }

  public static void main(String[] args) {
    Options options = new Options();

    options.addOption("h", "help", false, "Show help");
    options.addOption("p", "port", true, "The port that the server runs on");
    options.addOption("t", "throughput", true, "The amount of tuples per second that will be streamed");
    options.addOption("d", "duration", true, "The amount of time in ms that tuples will be streamed");
    options.addOption("f", "file", true, "The file that tuples will be pulled from");

    CommandLineParser parser = new BasicParser();
    HelpFormatter formater = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        formater.printHelp(APP_NAME, options);
        System.exit(0);
      }

      if (cmd.hasOption("p")) {
        serverPort = Integer.parseInt(cmd.getOptionValue("p"));
      }
      if (cmd.hasOption("t")) {
        throughPut = Integer.parseInt(cmd.getOptionValue("t"));
      }
      if (cmd.hasOption("d")) {
        duration = Integer.parseInt(cmd.getOptionValue("d"));
      }
      if (cmd.hasOption("f")) {
        dataSourceFile = cmd.getOptionValue("f");
      }


    } catch (Exception e) {
      formater.printHelp(APP_NAME, options);
      System.exit(0);
    }

    startServer();
  }
}
