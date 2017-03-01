package io;

import SQLProc.SQLProc;
import structures.CFGSettings;
import com.opencsv.CSVReader;
import static io.Util.numChars;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import structures.Conn;
import structures.ConnList;

/**
 * FileIO
 *
 * Contains methods that read files.
 *
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class FileIO {

    /**
     * readCFG()
     *
     * Reads and interprets a clustercfg file.
     *
     * @param CFGPath
     * @return
     */
    public static CFGSettings readCFG(String CFGPath) {
        CFGSettings settings = new CFGSettings();
        ConnList connectionBuffer = new ConnList();

        String tablename = "";
        int numnodes = 0;
        int partition_method = 0;
        String partition_column = "";
        String global_param1 = "";
        String global_param2 = "";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(CFGPath));

            String buffer;
            while ((buffer = reader.readLine()) != null) {
                String key;
                String val;
                if (buffer.contains("=")) {
                    key = (String) buffer.substring(0, buffer.indexOf("="));
                    val = buffer.substring(buffer.indexOf("=") + 1);

                    int separators = numChars('.', key);
                    if (separators == 0) {
                        switch (key) {
                            case "tablename":
                                tablename = val;
                                break;
                            case "numnodes":
                                numnodes = Integer.parseInt(val);
                                break;
                            default:
                                System.out.println("ERROR: unknown attribute, skipping:");
                                System.out.println(String.format("\t%s", buffer));
                                break;
                        }
                    } else if (separators == 1) {
                        switch (key.substring(0, buffer.indexOf("."))) {
                            /*case "catalog":
                             catalog.put(key.substring(key.indexOf(".") + 1), val);
                             break;*/
                            case "partition":
                                switch (key.substring(key.indexOf(".") + 1)) {
                                    case "method":
                                        switch (val) {
                                            case "hash":
                                            case "2":
                                                partition_method = 2;
                                                break;
                                            case "range":
                                            case "1":
                                                partition_method = 1;
                                                break;
                                            case "none":
                                            case "0":
                                                partition_method = 0;
                                                break;
                                            default:
                                                System.out.println("ERROR: invalid partition method specified. Assuming no partitions.");
                                                partition_method = 0;
                                                break;
                                        }
                                        break;
                                    case "column":
                                        partition_column = val;
                                        break;
                                    default:
                                        String subkey = key.substring(key.indexOf('.') + 1);
                                        switch (subkey) {
                                            case "param1":
                                                global_param1 = val;
                                                break;
                                            case "param2":
                                                global_param2 = val;
                                                break;
                                            default:
                                                System.out.println("ERROR: invalid partition parameter.");
                                                break;
                                        }
                                        break;
                                }
                                break;
                            default: //the handler for node info
                                String nodename = key.substring(0, key.indexOf('.'));
                                String subkey = key.substring(key.indexOf('.') + 1);

                                if (connectionBuffer.contains(nodename)) {
                                    connectionBuffer.get(nodename).insertVal(subkey, val);
                                } else {
                                    connectionBuffer.add(new Conn(nodename));
                                    connectionBuffer.get(nodename).insertVal(subkey, val);
                                }
                                break;
                        }
                    } else if (separators == 2) { //takes for granted that the user has provided other details for these connections
                        switch (key.substring(0, buffer.indexOf("."))) {
                            case "partition":
                                String subkey = key.substring(buffer.indexOf(".") + 1);
                                String nodename = subkey.substring(0, subkey.indexOf("."));
                                String param = subkey.substring(subkey.indexOf(".") + 1);

                                if (connectionBuffer.contains(nodename)) {
                                    connectionBuffer.get(nodename).insertVal(param, val);
                                } else {
                                    connectionBuffer.add(new Conn(nodename));
                                    connectionBuffer.get(nodename).insertVal(param, val);
                                }
                                break;
                            default:
                                System.out.println("ERROR: invalid partition parameter.");
                                break;
                        }
                    } else {
                        //no condition
                    }
                }
            }

        } catch (FileNotFoundException ex) {
            System.out.println("ERROR: clustercfg file not found");
        } catch (IOException ex) {
            System.out.println("ERROR: could not read clustercfg file");
        }

        settings.CONNECTIONS = connectionBuffer;
        settings.TABLENAME = tablename;
        settings.NUMNODES = numnodes;
        settings.PARTITION_METHOD = partition_method;
        settings.PARTITION_COLUMN = partition_column;

        if (settings.PARTITION_METHOD == 2) {
            for (int n = 0; n < settings.CONNECTIONS.size(); n++) {
                if (!settings.CONNECTIONS.get(n).identifier.equals("catalog")) {
                    settings.CONNECTIONS.get(n).partparam1 = global_param1;
                    settings.CONNECTIONS.get(n).partparam2 = global_param2;
                }
            }
        }

        if (SQLProc.DEBUG == true) {

        }
        return settings;
    }

    /**
     * readSQL
     *
     * Reads the specified SQL file.
     *
     * @param SQLPath
     * @return
     * @throws java.io.FileNotFoundException
     */
    public static ArrayList<String> readSQL(String SQLPath) throws FileNotFoundException, IOException {
        ArrayList<String> queryBuffer = new ArrayList<>();
        BufferedReader readSQLFile;

        readSQLFile = new BufferedReader(new FileReader(SQLPath));

        ArrayList<String> lineBuffer = new ArrayList<>();
        String x;
        while ((x = readSQLFile.readLine()) != null) {
            if (!x.substring(0, 2).equals("//")) {
                lineBuffer.add(x);
            }
        }
        String buffer = "";
        int y;
        for (String s : lineBuffer) {
            for (int c = 0; c < s.length(); c++) {
                y = s.charAt(c);
                if (!(y == ';')) {
                    buffer = buffer + (char) y;
                } else {
                    buffer = buffer + (char) y;
                    queryBuffer.add(buffer);
                    buffer = "";
                }
            }
        }
        return queryBuffer;
    }

    /**
     * readCSV
     *
     * Reads a CSV file.
     *
     * @param CSVPath
     * @return
     * @throws java.io.FileNotFoundException
     */
    public static ArrayList<String[]> readCSV(String CSVPath) throws FileNotFoundException, IOException {
        ArrayList<String[]> entryBuffer = new ArrayList<>();

        CSVReader csvReader = new CSVReader(new FileReader(CSVPath));

        String[] lineBuffer;
        while ((lineBuffer = csvReader.readNext()) != null) {
            entryBuffer.add(lineBuffer);
        }
        return entryBuffer;
    }
}
