package com.davis.utilities.database.utils;


import com.davis.utilities.database.api.FileMoveInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * * This software was created for me rights to this software belong to me and appropriate licenses
 * and * restrictions apply.
 *
 * @author Samuel Davis created on 7/21/17.
 */
public class PathUtils {

  private static final Logger log = LoggerFactory.getLogger(PathUtils.class.getName());

  private PathUtils() {}

  /**
   * Gets file paths.
   *
   * @param dirToRecurseIn the dir to recurse in
   * @return the file paths
   */
  public static List<Path> getFilePaths(String dirToRecurseIn) {
    Path p0 = Paths.get(dirToRecurseIn);
    //Doing things like this makes everything in the list a absolute Path object
    Path path = Paths.get(p0.toAbsolutePath().toString());
    List<Path> files = new ArrayList<>();
    try {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              if (!attrs.isDirectory()) {
                files.add(file);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      e.printStackTrace();
    }

    return files;
  }

  public static List<String> getFilePathsString(String dirToRecurseIn) {
    Path p0 = Paths.get(dirToRecurseIn);
    //Doing things like this makes everything in the list a absolute Path object
    Path path = Paths.get(p0.toAbsolutePath().toString());
    List<Path> paths = new ArrayList<>();
    try {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              if (!attrs.isDirectory()) {
                paths.add(file);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      e.printStackTrace();
    }
    List<String> stringPaths =
        paths.stream().map(i -> i.toAbsolutePath().toString()).collect(Collectors.toList());
    return stringPaths;
  }

  /**
   * Gets dir paths.
   *
   * @param dirToRecurseIn the dir to recurse in
   * @return the dir paths
   */
  public static List<Path> getDirPaths(String dirToRecurseIn) {
    Path p0 = Paths.get(dirToRecurseIn);
    //Doing things like this makes everything in the list a absolute Path object
    Path path = Paths.get(p0.toAbsolutePath().toString());
    List<Path> files = new ArrayList<>();
    DirectoryStream<Path> stream = null;
    try {
      stream = Files.newDirectoryStream(path);
      Iterator<Path> iter = stream.iterator();
      while (iter.hasNext()) {
        if(iter.next().toFile().isDirectory()){
          files.add(iter.next());
        }
      }
      stream.close();
    } catch (IOException e) {
      try {
        stream.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      e.printStackTrace();
    }


    return files;
  }

  /**
   * Is directory empty boolean.
   *
   * @param directory the directory
   * @return the boolean
   * @throws IOException the io exception
   */
  public static boolean isDirectoryEmpty(Path directory) throws IOException {

    DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory);
    if (directoryStream.iterator().hasNext()) {
      directoryStream.close();
      return true;
    } else {
      directoryStream.close();
      return false;
    }
  }

  /**
   * Delete everything in path.
   *
   * @param path the path
   */
  public static void deleteEverythingInPath(String path) {
    try {
      Path rootPath = Paths.get(path);
      Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .peek(System.out::println)
          .forEach(File::delete);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Returns Path to new File * @param fullPathOfFiletoMove the full path of fileto move
   *
   * @param fileMoveInfo the file move info
   * @return the boolean
   */
  public static Path moveFileIntoDir(FileMoveInfo fileMoveInfo) {
    try {

      String destDir = fileMoveInfo.getTargetPath().getParent().toString();

      if (fileMoveInfo.getTargetPathWithOriginalName().toFile().exists()) {
        log.trace(
            "File Already exists in target location of {}, moving is skipped. ",
            fileMoveInfo.getTargetPathWithOriginalName().toAbsolutePath().toString());
      } else {
        FileUtils.moveFileToDirectory(
            FileUtils.getFile(fileMoveInfo.getOriginalPath().toFile()),
            FileUtils.getFile(destDir),
            true);
      }

      return Paths.get(destDir + "/" + fileMoveInfo.getOriginalFilename());
    } catch (IOException e) {
      log.error("Move File Failed : Reason given {}", e.getMessage());
      return null;
    }
  }

  public static String getFilenameFromPath(String path) {
    String result = "";
    if (path.endsWith("/")) {
      result = path.substring(0, path.length() - 2);
      result = StringUtils.substringAfterLast(result, "/");
    } else {
      result = StringUtils.substringAfterLast(path, "/");
    }
    return result;
  }

  public static String getFilenameFromPath(Path path) {
    return path.subpath(path.getNameCount() - 1, path.getNameCount()).toString();
  }

  /**
   * Returns Path to new File * @param oldFile the old file
   *
   * @param fileMoveInfo the move info
   * @return the boolean
   */
  public static Path renameFile(FileMoveInfo fileMoveInfo) {
    try {
      if (fileMoveInfo.getTargetPath().toFile().exists()) {
        log.trace(
            "A file in the target directory already exists with the name of {} , nothing will be renamed. ",
            fileMoveInfo.getTargetPath().toAbsolutePath().toString());
        return null;
      } else {
        FileUtils.moveFile(
            fileMoveInfo.getTargetPathWithOriginalName().toAbsolutePath().toFile(),
            fileMoveInfo.getTargetPath().toAbsolutePath().toFile());
        return Paths.get(fileMoveInfo.getTargetPath().toFile().toString());
      }

    } catch (IOException e) {
      log.error("Rename File Failed : Reason given {}", e.getMessage());
      return null;
    }
  }
}
