package com.davis.utilities.database;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The type Main. */
public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class.getName());
  private static final String SQL_EXCEPTION = "SQL Exception {}";

  public static void main(String[] args) {
    String dbFilePath = null;
    String srcPath = null;
    String destPath = null;
    String pathOtRegexOut = null;
    /*if(args != null){
      if (args.length == 0 && args[0] != null) {
        dbFilePath = args[0];
      } else {
        dbFilePath = "/home/sam/projects/dev-personal/ml-uilities/dataset.db";
      }
      if (args.length >= 1 && args[1] != null) {
        srcPath = args[1];
      } else {
        srcPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data/";
      }
      if (args.length >= 2 && args[2] != null) {
        destPath = args[2];
      } else {
        destPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data-new/";
      }
      if (args.length >= 3 && args[3] != null) {
        pathOtRegexOut = args[3];
      } else {
        pathOtRegexOut = "/mnt/storage/label-images/nasic-data/";
      }
    }else{
      pathOtRegexOut = "/mnt/storage/label-images/nasic-data/";
      destPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data-new/";
      srcPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data/";
      dbFilePath = "/home/sam/projects/dev-personal/ml-uilities/dataset.db";
    }*/

    pathOtRegexOut = "/mnt/storage/label-images/nasic-data/";
    destPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data-new/";
    srcPath = "/mnt/linux-storage-large/nasic-tf/data-sets/nasic/nasic-data/";
    dbFilePath = "/home/sam/projects/dev-personal/ml-uilities/dataset.db";

    File databaseFile = new File(dbFilePath);
    Connection connection = null;
    try {
      connection = createConnection(databaseFile.getPath());
      // create a database connection
      setProcessedTo0ForBadLabels(connection);
      setUserToGuestForNoUsers(connection);
      List<String> badLabels = getIdsWithBadLabels(connection);
      setBadLabelEntitiesToNothingAndProcessedTo0(connection, badLabels);
      Map<String, String> dbFilePaths = getFilePaths(connection);
      writeClassRenameToCsv(dbFilePaths);
      boolean justCsv = true;
      if(!justCsv){
        copyFilesToNewDirectory(dbFilePaths, srcPath, destPath, pathOtRegexOut);
      }
    } catch (SQLException e) {
      log.error("Error occurred {}", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        // connection close failed.
        log.error("SQLException {}", e);
      }
    }
  }

  private static Connection createConnection(String databasePath) throws SQLException {
    return DriverManager.getConnection("jdbc:sqlite:" + databasePath);
  }

  private static Map<String, String> getFilePaths(Connection connection) {
    Map<String, String> filePath = new HashMap<>();
    String allStatement =
        "SELECT il.hashId, processed, location, classification, user_name, annotations "
            + "FROM image_locations il "
            + "INNER JOIN image_classes ic ON il.hashId=ic.hashId "
            + "INNER JOIN image_annotations ia ON il.hashId=ia.hashId "
            + "INNER JOIN user_annotated ua ON il.hashId=ua.hashId;";
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = connection.createStatement();
      statement.setQueryTimeout(30000); // set timeout to 30 sec.
      rs = statement.executeQuery(allStatement);
      while (rs.next()) {
        filePath.put(rs.getString("location"), rs.getString("classification"));
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }

    return filePath;
  }

  private static void copyFilesToNewDirectory(
      Map<String, String> filePathsFromDb,
      String pathToCopyFrom,
      String pathToCopyTo,
      String regexRename) {
    for (Map.Entry entry : filePathsFromDb.entrySet()) {
      String dbPath = (String) entry.getKey();
      String localPath = getLocalPathFromDbPath(dbPath, regexRename, pathToCopyFrom);
      String classification = (String) entry.getValue();
      String targetPath =
          createTargetPathFromDbPathAndClass(dbPath, classification, pathToCopyTo, regexRename);
      copyFile(localPath, targetPath);
    }
  }

  private static String createTargetPathFromDbPathAndClass(
      String dbPath, String className, String targetPath, String regex) {
    if (className == null) {
      className = "unknown";
    }
    String result = null;
    try {
      String target = dbPath.replace(regex, "");
      int firstSlash = target.indexOf('/');
      if (firstSlash > -1) {
        result = targetPath + className + target.substring(firstSlash, target.length());
      } else {
        result = targetPath + className + target;
      }

    } catch (StringIndexOutOfBoundsException e) {
      log.error("String index error {}", e);
    }

    return result;
  }

  private static String getLocalPathFromDbPath(String dbPath, String regex, String srcPath) {
    return dbPath.replace(regex, srcPath);
  }

  private static boolean copyFile(String srcPath, String targetPath) {
    boolean result = true;
    File srcFile = new File(srcPath);
    File targetFile = new File(targetPath);
    targetFile.getParentFile().mkdirs();
    if (!targetFile.exists()) {
      try {
        FileUtils.copyFile(srcFile, targetFile);
      } catch (IOException e) {
        result = false;
        log.error("Exception copying file {}", e);
      }
    } else {
      log.trace("File already exists in destination {}", targetFile.getPath());
      String ext = StringUtils.substringAfterLast(targetFile.getPath(), ".");
      String everythingBefore = StringUtils.substringBeforeLast(targetFile.getPath(), ".");
      String target2 = everythingBefore + "2" + "." + ext;
      copyFile(srcPath, target2);
    }
    if (!targetFile.exists()) {
      result = false;
    } else {
      log.info("Successful copy of {} to {}", srcFile.getPath(), targetFile.getPath());
    }

    return result;
  }

  private static List<String> getIdsWithBadLabels(Connection connection) {
    List<String> badLabelList = new ArrayList<>();
    String searchAll =
        "SELECT il.hashId, processed, location, classification, user_name, annotations "
            + "FROM image_locations il "
            + "LEFT JOIN image_classes ic ON il.hashId=ic.hashId "
            + "LEFT JOIN image_annotations ia ON il.hashId=ia.hashId "
            + "LEFT JOIN user_annotated ua ON il.hashId=ua.hashId;";
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = connection.createStatement();
      statement.setQueryTimeout(30000); // set timeout to 30 sec.
      rs = statement.executeQuery(searchAll);
      while (rs.next()) {
        String label = rs.getString("classification");
        String hash = rs.getString("hashId");
        String user = rs.getString("user_name");
        if (isBadLabel(label)) {
          log.info("Bad label for ID of {} Label was {} User that did it = {}", hash, label, user);
          badLabelList.add(hash);
        }
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }

    log.info("A total of {} images were labelled with bad labels. ", badLabelList.size());

    return badLabelList;
  }

  private static boolean isBadLabel(String label) {
    boolean result = true;
    if (label == null) {
      result = false;
    } else if (label.equalsIgnoreCase("person")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-vehicle")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-vehicle-weapon")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-vehicle-weapon-building")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-weapon")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-weapon-building")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-building")) {
      result = false;
    } else if (label.equalsIgnoreCase("person-building-vehicle")) {
      result = false;
    } else if (label.equalsIgnoreCase("building")) {
      result = false;
    } else if (label.equalsIgnoreCase("building-weapon")) {
      result = false;
    } else if (label.equalsIgnoreCase("building-vehicle")) {
      result = false;
    } else if (label.equalsIgnoreCase("weapon")) {
      result = false;
    } else if (label.equalsIgnoreCase("vehicle")) {
      result = false;
    } else if (label.equalsIgnoreCase("vehicle-weapon")) {
      result = false;
    } else if (label.equalsIgnoreCase("vehicle-weapon-building")) {
      result = false;
    } else if (label.equalsIgnoreCase("unknown")) {
      result = false;
    }
    return result;
  }

  private static void setProcessedTo0ForBadLabels(Connection connection) {
    List<String> removeIds = null;
    String getEntiteisLabeledAsEmptyString =
        "SELECT il.hashId, processed, location, classification, user_name, annotations "
            + "FROM image_locations il "
            + "INNER JOIN image_classes ic ON il.hashId=ic.hashId "
            + "INNER JOIN image_annotations ia ON il.hashId=ia.hashId "
            + "LEFT JOIN user_annotated ua ON il.hashId=ua.hashId "
            + "WHERE ic.classification='' "
            + "AND il.processed='1';";

    PreparedStatement statement = null;
    ResultSet rs = null;
    try {
      statement = connection.prepareStatement(getEntiteisLabeledAsEmptyString);
      statement.setQueryTimeout(30000); // set timeout to 30 sec.
      rs = statement.executeQuery();
      removeIds = new ArrayList<>();
      while (rs.next()) {
        removeIds.add(rs.getString("hashId"));
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }
    String updateString = "UPDATE image_locations SET processed=0 WHERE hashId=?";
    log.info("Remove Id Size {}", removeIds.size());
    try {
      statement = connection.prepareStatement(updateString);
      for (String badOne : removeIds) {
        statement.setString(1, badOne);
        statement.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }
  }

  private static void setUserToGuestForNoUsers(Connection connection) {
    String getEntitiesWithNoUserAssociated =
        "SELECT il.hashId, processed, location, classification, "
            + "user_name, annotations FROM image_locations il "
            + "INNER JOIN image_classes ic ON il.hashId=ic.hashId "
            + "INNER JOIN image_annotations ia ON il.hashId=ia.hashId "
            + "LEFT JOIN user_annotated ua ON il.hashId=ua.hashId WHERE user_name IS NULL;";
    List<String> noUserList = new ArrayList<>();
    PreparedStatement statement = null;
    ResultSet rs2 = null;
    try {
      statement = connection.prepareStatement(getEntitiesWithNoUserAssociated);
      statement.setQueryTimeout(30000);
      rs2 = statement.executeQuery();
      while (rs2.next()) {
        noUserList.add(rs2.getString("hashId"));
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (rs2 != null) {
        try {
          rs2.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }

    log.info("No User Id Size {}", noUserList.size());
    if (!noUserList.isEmpty()) {
      String sqlUpdate = "INSERT INTO user_annotated (hashId, user_name) VALUES(?,'guest');";
      try {
        statement = connection.prepareStatement(sqlUpdate);
        for (String badOne : noUserList) {
          statement.setString(1, badOne);
          statement.executeUpdate();
        }
      } catch (SQLException e) {
        log.error(SQL_EXCEPTION, e);
      } finally {
        if (statement != null) {
          try {
            statement.close();
          } catch (SQLException e) {
            log.error(SQL_EXCEPTION, e);
          }
        }
      }
    }
  }

  private static void setBadLabelEntitiesToNothingAndProcessedTo0(
      Connection connection, List<String> badLabels) {
    String updateLocation = "UPDATE image_locations SET processed='0' WHERE hashId=?";

    PreparedStatement updateLocationStatement = null;
    String updateClass = "UPDATE image_classes SET classification=NULL WHERE hashId=?";
    PreparedStatement updateClassStatement = null;
    try {
      updateLocationStatement = connection.prepareStatement(updateLocation);
      for (String bad : badLabels) {
        updateLocationStatement.setString(1, bad);
        updateLocationStatement.executeUpdate();
      }
      updateLocationStatement.close();

    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (updateLocationStatement != null) {
        try {
          updateLocationStatement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }
    try {
      updateClassStatement = connection.prepareStatement(updateClass);
      for (String bad : badLabels) {
        updateClassStatement.setString(1, bad);
        updateClassStatement.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(SQL_EXCEPTION, e);
    } finally {
      if (updateClassStatement != null) {
        try {
          updateClassStatement.close();
        } catch (SQLException e) {
          log.error(SQL_EXCEPTION, e);
        }
      }
    }
  }

  private static void writeClassRenameToCsv(Map<String, String> dbFilePaths) {
    try (BufferedWriter bw =
        new BufferedWriter(
            new FileWriter(
                "classChange" + new Date().toString().replaceAll(" ", "-") + ".csv", true))) {

      for(Map.Entry<String, String> entry : dbFilePaths.entrySet()){
        bw.write(entry.getKey() +","+entry.getValue());
        bw.newLine();
      }
      bw.close();
    } catch (IOException e) {
      log.error("Unable to write results to CSV  {}", e);
    }
  }
}
