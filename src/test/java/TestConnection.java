

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.UnknownHostException;
import me.cmath.streamingestor.MainIngestor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class TestConnection {

  private final String IP = "localhost";

  public class MainRunner implements Runnable {

    public void run() {
      MainIngestor.main(null);
    }
  }

  @Before
  public void setup() {
    // Start server with default settings
    MainRunner myRunnable = new MainRunner();
    Thread t = new Thread(myRunnable);
    t.start();
  }

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
      double tuplesPerSec = totalTuples / (duration/1000);

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
    } catch (UnknownHostException e) {
      System.out.println("Sock:" + e.getMessage());
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
    }
  }
}
