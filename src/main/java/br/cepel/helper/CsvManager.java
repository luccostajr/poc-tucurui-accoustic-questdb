package br.cepel.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.opencsv.CSVReader;

public class CsvManager {
  private static final String SEPARATOR = ";";
  private Integer repetition = 300;

  public CsvManager(Integer repetition) {
    super();
    if (repetition != null) {
      this.repetition = repetition;
    }
  }

  public CsvManager() {
    this(null);
  }

  public List<String[]> readAllLines(Path filePath) throws Exception {
    try (Reader reader = Files.newBufferedReader(filePath)) {
      try (CSVReader csvReader = new CSVReader(reader)) {
        return csvReader.readAll();
      }
    }
  }

  public List<String[]> readLineByLine(Path filePath) throws Exception {
    List<String[]> list = new ArrayList<>();
    try (Reader reader = Files.newBufferedReader(filePath)) {
      try (CSVReader csvReader = new CSVReader(reader)) {
        String[] line;
        while ((line = csvReader.readNext()) != null) {
          list.add(line);
        }
      }
    }
    return list;
  }

  public List<String[]> readCsv(String fileName) throws FileNotFoundException {
    File csvFile = new File(fileName);
    List<String[]> resultLines = new ArrayList<>();
    List<String[]> csvLines = new ArrayList<>();

    Scanner scanner = new Scanner(csvFile);

    // Skip the header rows
    scanner.nextLine();
    scanner.nextLine();
    scanner.nextLine();

    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      try {
        String[] lineSplited = line.split(SEPARATOR);
        for (int i = 0; i < lineSplited.length; i++) {
          lineSplited[i] = lineSplited[i].replace(",", ".");
        }
        csvLines.add(lineSplited);
      } catch (Exception e) {
        System.out.println("Error: " + e.getMessage());
      }
    }

    scanner.close();

    for (int repeatTimes = 0; repeatTimes < repetition; repeatTimes++) {
      for (String[] line : csvLines) {
        resultLines.add(line);
      }
    }
    return resultLines;
  }
}
