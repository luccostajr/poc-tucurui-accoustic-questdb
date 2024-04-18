package br.cepel;

import java.io.BufferedWriter;
import java.util.List;
import java.util.UUID;

import br.cepel.helper.CsvManager;
import br.cepel.helper.Helper;
import br.cepel.questdb.business.AccousticQdbBO;

public class App {
    private static final String FILE_PATH = "csv/acustico.csv";
    private static CsvManager csvManager = null;
    private static AccousticQdbBO accousticQdbBO = null;

    public static void main(String[] args) {
        try {
            BufferedWriter writer = Helper.createFile("App");
            Helper.createTimer("App");
            Helper.writeToFile(writer, "Start of Process");
            csvManager = new CsvManager();
            accousticQdbBO = new AccousticQdbBO();

            Helper.createTimer("readCsv");
            Helper.writeToFile(writer, "Start of data creation");
            List<String[]> lines = csvManager.readCsv(FILE_PATH);
            Integer elapsedTimeReadCsv = Helper.getElapsedTime("readCsv").intValue();
            Helper.writeToFile(writer, "Created " + lines.size() + " lines in " + elapsedTimeReadCsv + " ms");

            Helper.createTimer("insert");
            Helper.writeToFile(writer, "Start of insertion");
            accousticQdbBO.insert("DataSource_" + UUID.randomUUID().toString(), lines);
            Helper.writeToFile(writer, "Inserted " + lines.size() + " lines into QuestDB in "
                    + Helper.getElapsedTime("insert").intValue() + " ms");

            Integer elapsedTime = Helper.getElapsedTime("App").intValue();
            Helper.writeToFile(writer, "End of Process in " + elapsedTime + " ms");
            Helper.closeFile(writer);
            System.out.println("End of Process. Inserted " + lines.size() + " lines in " + elapsedTime + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
