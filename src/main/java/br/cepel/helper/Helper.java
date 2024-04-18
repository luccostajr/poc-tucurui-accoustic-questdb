package br.cepel.helper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Helper {
  private static Logger logger = LogManager.getLogger(Helper.class);

  /**
   * Timer class to measure elapsed time
   */
  public static class Timer {
    private Long start = null;
    private Long end = null;

    public Timer() {
      start = System.currentTimeMillis();
    }

    public Long getStart() {
      return start;
    }

    public Long getElapsedTime() {
      end = System.currentTimeMillis() - start;
      return end;
    }

    public Long getEnd() {
      return end;
    }

    public void reset() {
      start = System.currentTimeMillis();
    }
  }

  public static Map<String, Timer> timers = new HashMap<>();
  private static final String DIR = "logs/";

  /**
   * Sleep for a defined amount of time
   * 
   * @param millis the time to sleep in milliseconds
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  /**
   * Create a timer with a defined name
   * 
   * @param name the name of the timer
   */
  public static void createTimer(String name) {
    removeTimer(name);
    timers.put(name, new Timer());
  }

  /**
   * Get the elapsed time of a defined timer
   * 
   * @param name the name of the timer
   * @return the elapsed time
   */
  public static Long getElapsedTime(String name) {
    if (timers.containsKey(name)) {
      return timers.get(name).getElapsedTime();
    }
    return null;
  }

  /**
   * Reset a defined timer
   * 
   * @param name the name of the timer
   */
  public static void resetTimer(String name) {
    if (timers.containsKey(name)) {
      timers.get(name).reset();
    }
  }

  /**
   * Remove a defined timer
   * 
   * @param name the name of the timer
   */
  public static void removeTimer(String name) {
    if (timers.containsKey(name)) {
      timers.remove(name);
    }
  }

  /**
   * Get the start time of a defined timer
   * 
   * @param name the name of the timer
   * @return the start time
   */
  public static Long getStartTime(String name) {
    if (timers.containsKey(name)) {
      return timers.get(name).getStart();
    }
    return null;
  }

  /**
   * Get the end time of a defined timer
   * 
   * @param name the name of the timer
   * @return the end time
   */
  public static Long getEndTime(String name) {
    if (timers.containsKey(name)) {
      return timers.get(name).getEnd();
    }
    return null;
  }

  /**
   * Create a file in the logs directory
   * 
   * @param shortFileName the name of the file
   * @return the file created
   */
  public static BufferedWriter createFile(String shortFileName) {
    String longFileName = DIR + shortFileName + ".log";
    BufferedWriter outFile = null;
    try {
      outFile = new BufferedWriter(new FileWriter(longFileName, false));
    } catch (IOException e) {
      logger.error("Error creating file " + longFileName + ".log");
      outFile = null;
    }
    return outFile;
  }

  /**
   * Write a line to the defined file
   * 
   * @param outFile the file to write to
   * @param line    the line to be written
   */
  public static void writeToFile(BufferedWriter outFile, String line) {
    try {
      String newLine = "[" + getFormattedDateTime(System.currentTimeMillis()) + "]\n" + line + "\n";
      outFile.write(newLine);
      outFile.flush();
    } catch (IOException e) {
      logger.error("Error writing to file. Line: " + line);
    }
  }

  /**
   * Close the defined file
   * 
   * @param outFile the file to be closed
   */
  public static void closeFile(BufferedWriter outFile) {
    try {
      outFile.close();
    } catch (IOException e) {
      logger.error("Error closing file");
    }
  }

  /**
   * This method is responsible for formatting the date time.
   * 
   * @param ts the timestamp in milliseconds
   * @return the formatted date time
   */
  public synchronized static String getFormattedDateTime(double ts) {
    long timestamp = ((Double) ts).longValue();
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", timestamp);
  }

  /**
   * This method is responsible for formatting the date time.
   * 
   * @param ts the timestamp in milliseconds
   * @return the formatted date time
   */
  public synchronized static String getFormattedDateTime(long ts) {
    return String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS", ts);
  }
}
