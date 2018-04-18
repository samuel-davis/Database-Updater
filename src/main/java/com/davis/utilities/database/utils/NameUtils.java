package com.davis.utilities.database.utils;

/**
 * This software was created for me rights to this software belong to me and appropriate licenses and
 * restrictions apply.
 *
 * @author Samuel Davis created on 7/21/17.
 */
public class NameUtils {
  public static final String NAME_REPAIR_REGEX_2 = "[`~!@#$%^&*()_+[\\]\\\\;\',./{}|:\"<>?]";

  private static final String NAME_REPAIR_REGEX ="[\\~\\#\\%\\&\\*\\{\\}\\,\\ \\\\\\:\\<\\>\\?\\/\\+\\|]";
  private NameUtils() {}

  /**
   * Sanitize dir name string.
   *
   * @param name the name
   * @return the string
   */
  public static String sanitizeDirName(String name) {
    String sanitized = name.replaceAll(" ", "-");
    sanitized = sanitized.replaceAll("_", "---");
    sanitized = sanitized.replaceAll("\\,", "");
    sanitized = sanitized.replaceAll(",", "");
    sanitized =
        sanitized.substring(0, 1).toLowerCase() + sanitized.substring(1, sanitized.length());
    return sanitized;
  }

  /**
   * Sanitize name string.
   *
   * @param name the name
   * @return the string
   */
  public static String sanitizeName(String name) {
    name = name.replaceAll(NAME_REPAIR_REGEX, "-");
    name = name.replaceAll("\\/","-");
    name = name.replaceAll("\\\\","-");
    name = name.replaceAll("[^a-zA-Z0-9.-]", "+");
    name = name.replaceAll("\\\'","-");
    return name.replaceAll("\\\"","-");
  }
}
