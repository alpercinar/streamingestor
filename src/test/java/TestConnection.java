import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import me.cmath.streamingestor.MainIngestor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestConnection {

  private final String IP = "localhost";
  private final String TEST_DATA_FILE = "testdata.txt";
  private final String SAMPLE_TUPLE = "44|2012-07-07 03:16:55|CMPT|TMB|0|AAAAAAAAAAAADRC|3045|2.82|28|5663|2.89|34.70|36.13|3284.36\n";
  private final int NUM_SAMPLE_TUPLES = 50000;

  public class MainRunner implements Runnable {

    public void run() {
      MainIngestor.dataSourceFile = TEST_DATA_FILE;
      MainIngestor.startServer();
    }
  }

  /**
   * Set up server in separate thread
   */
  @Before
  public void setup() {
    // Create sample file with plenty of tuples
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(TEST_DATA_FILE), "utf-8"))) {
      for (int i = 0; i < NUM_SAMPLE_TUPLES; i++) {
        writer.write(SAMPLE_TUPLE);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    MainRunner myRunnable = new MainRunner();
    Thread t = new Thread(myRunnable);
    t.start();
  }

  @After
  public void cleanUp() {
    // Delete sample file
    try {
      Files.delete(new File(TEST_DATA_FILE).toPath());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test that the server is able to start and has consistent throughput
   */
  @Test
  public void testConnection() {

    Socket clientSocket = null;
    try {
      int serverPort = MainIngestor.serverPort;

      clientSocket = new Socket(IP, serverPort);
      clientSocket.setSoTimeout(5000);

      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
      String messageString = "";
      int totalTuples = 0;

      long start = System.currentTimeMillis();
      while (true) {
        int length = in.read();
        if (length == -1) {
          break;
        }
        byte[] messageByte = new byte[length];
        in.read(messageByte);
        totalTuples++;
        messageString = new String(messageByte);
      }
      long end = System.currentTimeMillis();

      double duration = (end - start);
      double tuplesPerSec = totalTuples / (duration / 1000);

      System.out.println("** Emitted " + totalTuples + " tuples in " + duration + " ms");
      System.out.println("** Txn rate: " + tuplesPerSec + " tuples/sec");

      // Test
      // Check that the duration was within 4 seconds of the desired duration
      assertTrue(duration >= MainIngestor.duration - 2000 && duration <= MainIngestor.duration + 2000);

      // Test
      // Check that the tp was within 200 tuples per second of the desired duration
      assertTrue(tuplesPerSec >= MainIngestor.throughPut - 100 && tuplesPerSec <= MainIngestor.throughPut + 100);
    } catch (InterruptedIOException e) {
      System.out.println("Timed out!");
      fail();
    } catch (UnknownHostException e) {
      System.out.println("Sock:" + e.getMessage());
      fail();
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
      fail();
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
      fail();
    }
  }
}