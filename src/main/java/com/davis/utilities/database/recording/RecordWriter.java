package com.davis.utilities.database.recording;

import com.csvreader.CsvWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class RecordWriter {
  private static final Logger log = LoggerFactory.getLogger(RecordWriter.class.getName());
  private static Path location;
  private static CsvWriter csvWriter;
  private static boolean intialized;

  public RecordWriter() {}

  public static void main(String[] args) {

    String outputFile = "users.csv";

    // before we open the file check to see if it already exists
    boolean alreadyExists = new File(outputFile).exists();

    try {
      // use FileWriter constructor that specifies open for appending
      CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

      // if the file didn't already exist then we need to write out the header line
      if (!alreadyExists) {
        csvOutput.write("id");
        csvOutput.write("name");
        csvOutput.endRecord();
      }
      // else assume that the file already has the correct header line

      // write out a few records
      csvOutput.write("1");
      csvOutput.write("Bruce");
      csvOutput.endRecord();

      csvOutput.write("2");
      csvOutput.write("John");
      csvOutput.endRecord();

      csvOutput.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean initialize(Path locationAndNameOfRecordToWrite, List<String> headerFields) {
    RecordWriter.location = locationAndNameOfRecordToWrite;
    RecordWriter.intialized = false;
    boolean alreadyExists = RecordWriter.location.toFile().exists();
    try {
      RecordWriter.csvWriter =
          new CsvWriter(new FileWriter(RecordWriter.location.toFile(), true), ',');
      if (!alreadyExists) {
        writeHeaderFields(headerFields);
      }
      RecordWriter.intialized = true;
    } catch (IOException e) {
      RecordWriter.intialized = false;
      log.error("Exception encountered attempting to create the record {}", e);
    }
    return RecordWriter.intialized;
  }

  public void writeHeaderFields(List<String> headerFields) throws IOException {
    for (String headerField : headerFields) {
      RecordWriter.csvWriter.write(headerField);
    }
    RecordWriter.csvWriter.endRecord();
  }

  public void writeSingleRecord(Integer number, String caption) throws IOException {
    RecordWriter.csvWriter.write(String.valueOf(number));
    RecordWriter.csvWriter.write(caption);
    RecordWriter.csvWriter.endRecord();
  }

  public void writeSingleRecord(Integer id, String split, String filePath) throws IOException {
    RecordWriter.csvWriter.write(String.valueOf(id));
    RecordWriter.csvWriter.write(split);
    RecordWriter.csvWriter.write(filePath);
    RecordWriter.csvWriter.endRecord();
  }

  public void closeCsv() {
    RecordWriter.csvWriter.close();
  }

  public void writeRecord(List<String> stuffToWrite) throws IOException {
    for (String entry : stuffToWrite) {
      RecordWriter.csvWriter.write(entry);
    }
    RecordWriter.csvWriter.endRecord();
  }
}
