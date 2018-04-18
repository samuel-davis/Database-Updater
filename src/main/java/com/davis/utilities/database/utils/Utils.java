package com.davis.utilities.database.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This software was created for rights to this software belong to appropriate licenses and
 * restrictions apply.
 *
 * @author Samuel Davis created on 10/3/17.
 */
public class Utils {
  private Utils() {};

  public static List<Integer> getIndexesOf(String path, char indexChar) {
    List<Integer> list = new ArrayList<>();

    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == indexChar) {
        list.add(i);
      }
    }
    return list;
  }

  public static String getFilename(String filePath) {
    //Remove trailing slash
    char lastChar = filePath.charAt(filePath.length() - 1);
    if (lastChar == '/') {
      filePath = filePath.substring(0, filePath.length() - 2);
    }
    return StringUtils.substringAfterLast(filePath, "/");
  }

  public static String getClassName(String filePath) {
    //Remove trailing slash
    char lastChar = filePath.charAt(filePath.length() - 1);
    if (lastChar == '/') {
      filePath = filePath.substring(0, filePath.length() - 2);
    }

    List<Integer> slashIndexes = getIndexesOf(filePath, '/');
    int lastIndex = -1;
    int secondToLast = -1;
    String className = null;
    if (slashIndexes.size() >= 2) {
      lastIndex = slashIndexes.get(slashIndexes.size() - 1);

      secondToLast = slashIndexes.get(slashIndexes.size() - 2);
      className = filePath.substring(secondToLast+1, lastIndex);
    }

    return className;
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
    return map.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue(/*Collections.reverseOrder()*/))
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (e1, e2) -> e1,
                    LinkedHashMap::new
            ));
  }


  public static <T> boolean contains(final T[] array, final T v) {
    for (final T e : array) {
      if (e == v || v != null && v.equals(e)) {
        return true;
      }
    }
    return false;
  }
}
