package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;


public class StreamServer extends Thread {
  private int _maxTuples;
  private int _duration;
  private BufferedReader _sourceBuffer;
  private OutputStreamWriter _output;
  private Socket _clientSocket;
  private RateLimiter _rateLimiter;
  private final int END_OF_STREAM_SIG = 0;
  private long _startTime;
  private AtomicInteger _cosumedTuples;
  private final Object _fileLock;

  public StreamServer(Socket aClientSocket, RateLimiter rateLimiter, long startTime, int duration,
      BufferedReader dataSource, AtomicInteger consumedTuples, int maxTupels, Object lock) {

    _fileLock = lock;

    try {
      _duration = duration;
      _sourceBuffer = dataSource;
      _rateLimiter = rateLimiter;
      _clientSocket = aClientSocket;
      _startTime = startTime;
      _cosumedTuples = consumedTuples;
      _maxTuples = maxTupels;
      _output = new OutputStreamWriter(_clientSocket.getOutputStream(), Charset.forName("UTF-8").newEncoder());
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
      int localTuples = 0;

      do {
        synchronized (_fileLock) {
          tuple = _sourceBuffer.readLine();
        }

        if (tuple == null) {
          break;
        }

        _rateLimiter.acquire();
        _cosumedTuples.incrementAndGet();
        if (_maxTuples != -1 && _cosumedTuples.get() > _maxTuples) {
          break;
        }

//        _output.write(tuple.length());
        _output.write(tuple);
        _output.write(System.lineSeparator());

        localTuples++;
        if (_maxTuples == -1 && System.currentTimeMillis() - _startTime > _duration) {
          break;
        }
      } while (true);

      // Send kill signal
      _output.write(END_OF_STREAM_SIG);
      _output.close();
      System.out.println("Stream ended. Streamed " + localTuples + " tuples.");
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

