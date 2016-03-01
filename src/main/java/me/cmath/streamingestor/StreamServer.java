package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;


public class StreamServer extends Thread {
  private int _duration;
  private BufferedReader _sourceBuffer;
  private BufferedOutputStream _output;
  private Socket _clientSocket;
  private RateLimiter _rateLimiter;
  private final int END_OF_STREAM_SIG = -1;

  public StreamServer(Socket aClientSocket, int throughPut, int duration, String dataSourceFile) {

    try {
      _duration = duration;
      _sourceBuffer = new BufferedReader(new FileReader(new File(dataSourceFile)));
      _rateLimiter = RateLimiter.create(throughPut);
      _clientSocket = aClientSocket;

      _output = new BufferedOutputStream(_clientSocket.getOutputStream());
      this.start();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  public void run() {
    try {

      System.out.println("Client " +
          _clientSocket.getInetAddress() + ":" +
          _clientSocket.getPort() + " connected. Starting stream..");

      long start = System.currentTimeMillis();
      String tuple;
      while ((tuple = _sourceBuffer.readLine()) != null) {
        _rateLimiter.acquire();

        _output.write(tuple.length());
        _output.write(tuple.getBytes());

        if (System.currentTimeMillis() - start > _duration) {
          break;
        }
      }
      // Send kill signal
      _output.write(END_OF_STREAM_SIG);
      System.out.println("Stream ended.");
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
    } finally {
      try {
        _clientSocket.close();
      } catch (IOException e) {/*close failed*/}
    }
  }
}

