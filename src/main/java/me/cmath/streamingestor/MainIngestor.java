package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;


public class MainIngestor {

  public static final String APP_NAME = "StreamIngestor";
  private int _serverPort = 18000;
  private int _throughPut = 10000;
  private int _duration = 3000;
  private String _dataSourceFile = "";
  private ServerSocket _listenSocket;
  private AtomicInteger _consumedTuples;

  public MainIngestor(int serverPort, int throughPut, int duration, String dataSourceFile) {
    _serverPort = (serverPort == 0) ? _serverPort : serverPort;
    _throughPut = (throughPut == 0) ? _throughPut : throughPut;
    _duration = (duration == 0) ? _duration : duration;
    _dataSourceFile = dataSourceFile;
    _consumedTuples = new AtomicInteger(0);

  }

  public void startServer() {
    try {
      _listenSocket = new ServerSocket(_serverPort);
      BufferedReader dataSource = new BufferedReader(new FileReader(new File(_dataSourceFile)));
      RateLimiter rateLimiter = RateLimiter.create(_throughPut);
      final long startTime = System.currentTimeMillis();
      System.out.println("Server started on port " + _serverPort + "..");
      while (true) {
        if (System.currentTimeMillis() - startTime > _duration+3000) {
          printStats();
        }
        if (_listenSocket.isClosed()) {
          printStats();
          break;
        }
        Socket clientSocket = _listenSocket.accept();
        StreamServer c = new StreamServer(clientSocket, rateLimiter, startTime, _duration, dataSource, _consumedTuples);

      }
    } catch (IOException e) {
      System.out.println("Error: " + e.getMessage() + " (using localhost:" + _serverPort + ")");
    } catch (Exception e) {
      System.out.println("Shutting down due to interruption..");
    }
  }

  private void printStats() {
    double actualTP = (double) _consumedTuples.get() / (_duration/1000.0);
    System.out.println("Duration has passed. Shutting down server..");
    System.out.println("Consumed tuples: " + _consumedTuples.get());
    System.out.println("Actual throughput: " + actualTP + " tuples/sec");
  }

  public void stopServer() {
    try {
      printStats();
      _listenSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    int serverPort = 0, throughPut = 0, duration = 0;
    String dataSourceFile = null;

    Options options = new Options();

    options.addOption("h", "help", false, "Show this dialog");
    options.addOption("p", "port", true, "The port that the server runs on");
    options.addOption("t", "throughput", true, "The amount of tuples per second that will be streamed");
    options.addOption("d", "duration", true, "The amount of time in ms that tuples will be streamed");
    options.addOption("f", "file", true, "The input file with tuples separated by line breaks");

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
      } else {
        System.err.println("Please specifiy an input file");
        System.exit(1);
      }
    } catch (Exception e) {
      formater.printHelp(APP_NAME, options);
      System.exit(0);
    }

    MainIngestor mainIngestor = new MainIngestor(serverPort, throughPut, duration, dataSourceFile);
    mainIngestor.startServer();
  }
}
