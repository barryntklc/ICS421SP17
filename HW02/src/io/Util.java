package io;

import SQLProc.SQLProc;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * A bunch of utility methods.
 *
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class Util {

    /**
     * numChars
     *
     * @param c the character to count
     * @param s the string to parse through
     * @return the number of specified character in given string
     */
    public static int numChars(char c, String s) {
        int x = 0;
        for (int n = 0; n < s.length(); n++) {
            if (s.charAt(n) == c) {
                x++;
            }
        }
        return x;
    }

    /**
     * searchPattern
     *
     * Searches for a pattern in a given string
     *
     * @param pattern the pattern string to look for
     * @param string the string string to search
     * @return true if the
     */
    public static boolean searchPattern(String pattern, String string) {
        return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(string).find();
    }

    /**
     * extractTableName
     *
     * @param beginSlot where the tablename is expected to be found after a
     * space delimiter
     * @param query a given query
     * @return
     */
    public static String extractTableName(int beginSlot, String query) {
        String[] replace = {"(", ")", ";"};
        String prefilter = query;

        for (String n : replace) {
            if (prefilter.contains(n)) {
                prefilter = prefilter.replace(n, " ");
            }
        }
        prefilter = prefilter.split(" ")[beginSlot];
        return prefilter;
    }

    /**
     * ParseQuery
     *
     * Parses given queries for information.
     *
     * @param query a given query
     * @return a String array with query operation type and a given table name,
     * if applicable
     *
     * Used
     * http://stackoverflow.com/questions/2912894/how-to-match-any-character-in-java-regular-expression
     */
    public static String[] ParseQuery(String query) {
        String[] buffer = {"", ""};

        System.out.println(query);
        //System.out.println(searchPattern("create table", query));
        //System.out.println(searchPattern("drop table", query));
        //System.out.println("contains pattern: " + searchPattern("select.*from.*", query));

        if (searchPattern("create table", query)) {
            buffer[0] = "CREATE";
            buffer[1] = extractTableName(2, query);
        } else if (searchPattern("alter table", query)) {
            buffer[0] = "ALTER";
            buffer[1] = extractTableName(2, query);
        } else if (searchPattern("drop table", query)) {
            buffer[0] = "DROP";
            buffer[1] = extractTableName(2, query);
        } else if (searchPattern("insert into", query)) {
            buffer[0] = "INSERT";
            buffer[1] = extractTableName(2, query);
        } else if (searchPattern("delete.{2,}from", query)) {
            buffer[0] = "DELETE";
            buffer[1] = extractTableName(3, query);
        } else if (searchPattern("delete.*from", query)) {
            buffer[0] = "DELETE";
            buffer[1] = extractTableName(2, query);
        } else if (searchPattern("select.*from.*", query)) {
            buffer[0] = "SELECT";
            buffer[1] = extractTableName(3, query);
        }

        if (SQLProc.DEBUG == true) {
            System.out.println("QUERY TYPE: " + buffer[0]);
            System.out.println("TABLE NAME: " + buffer[1]);
        }
        return buffer;
    }

    /**
     * Prints an error notice, with embedded error message
     *
     * @param errormsg a given error message
     */
    public static void printErrorNotice(String errormsg) {
        System.out.println("ERROR: " + errormsg);
        System.out.println("Syntax:");
        System.out.println("./run3.sh [clustercfg path] [sqlfile path]");
        System.out.println("./run4.sh [clustercfg path] [csv path]");
    }

    /**
     * Prints an error notice, based on given SQL Exception error code
     *
     * TODO apply to other SQL methods in DatabaseOps
     *
     * @param e a given exception
     */
    public static void printSQLException(SQLException e) {
        if (e.getMessage().contains("42000")) {
            System.out.println("ERROR: Invalid SQL syntax!");
        } else if (e.getMessage().contains("1146")) {
            System.out.println("ERROR: Table not found!");
        } else if (e.getMessage().contains("42S02")) {
            System.out.println("ERROR: Base table or view not found!");
        } else if (e.getMessage().contains("1050")) {
            System.out.println("ERROR: Table already exists!");
        } else {
            System.out.println(e.getMessage());
            System.out.println("ERROR: SQL Exception!");
        }
    }
}
