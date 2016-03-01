package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;


public class StreamServer extends Thread {
  private int _duration;
  private BufferedReader _sourceBuffer;
  private BufferedOutputStream _output;
  private Socket _clientSocket;
  private RateLimiter _rateLimiter;
  private final int END_OF_STREAM_SIG = -1;
  private long _startTime;

  public StreamServer(Socket aClientSocket, RateLimiter rateLimiter, long startTime, int duration, BufferedReader dataSource) {

    try {
      _duration = duration;
      _sourceBuffer = dataSource;
      _rateLimiter = rateLimiter;
      _clientSocket = aClientSocket;
      _startTime = startTime;

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

      String tuple;
      while ((tuple = _sourceBuffer.readLine()) != null) {
        _rateLimiter.acquire();

        _output.write(tuple.length());
        _output.write(tuple.getBytes());

        if (System.currentTimeMillis() - _startTime > _duration) {
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

