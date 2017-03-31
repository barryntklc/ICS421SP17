package SQLProc;

import static SQLProc.DatabaseOps.ClusterTest;
import structures.CFGSettings;
import static SQLProc.DatabaseOps.CreateCatalog;
import static SQLProc.DatabaseOps.GetColumnIndex;
import static SQLProc.DatabaseOps.UpdateCatalog;
import static io.FileIO.readCFG;
import static io.FileIO.readCSV;
import static io.FileIO.readSQL;
import static io.Util.ParseQuery;
import static io.Util.printErrorNotice;
import structures.Conn;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQL Processor 1.0 (DDL Processor 2.0)
 *
 * (ICS 421 HW 2)
 *
 * @author Barryn Chun
 * @date January 30, 2017
 */
public class SQLProc {

    public static boolean DEBUG = false;

    static String CLUSTERCFG = "";
    static String SQLFILE = "";
    static String CSVFILE = "";

    //static ConnList connections = new ConnList();
    static ArrayList<Conn> nodecluster = new ArrayList<>();
    static ArrayList<String> queries = new ArrayList<>();
    static ArrayList<String[]> entries = new ArrayList<>();
    static ArrayList<Boolean> runResults = new ArrayList<>();

    static CFGSettings settings = new CFGSettings();

    /**
     * Main
     *
     * ./run3.sh - runSQL ./run4.sh - loadCSV
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        /*try {
            Class<?> driverClass = Class.forName("com.mysql.jdbc.Driver");*/
            
            if (args.length == 2) { //RUN SQL
                CLUSTERCFG = args[0];
                SQLFILE = args[1];
                try {
                    settings = readCFG(CLUSTERCFG);
                    queries = readSQL(SQLFILE);
                    
                    if (DEBUG == true) {
                        ClusterTest(settings);
                    }

                    if (queries.isEmpty()) {
                        System.out.println("ERROR: No valid SQL found, no queries to run!");
                    } else {
                        
                    }
                    
                    //check if a catalog is found and there are nodes
                    if (settings.CONNECTIONS.contains("catalog") && (settings.NUMNODES > 0)) {

                        try {
                            //add node info to nodecluster
                            for (int z = 0; z < settings.CONNECTIONS.size(); z++) {
                                if (!settings.CONNECTIONS.get(z).identifier.equals("catalog")) {
                                    nodecluster.add(settings.CONNECTIONS.get(z));
                                }
                            }

                            //for each query item, run it on each node in nodecluster
                            //TODO perhaps make this changable here if the user selects any different partition config
                            //TODO make changable via partition type
                            RunQueryThreads();
                            
                        } catch (Exception e) {
                            System.out.println("ERROR: Could not connect to the catalog database!");
                            e.printStackTrace();
                        }
                    } else {
                        if (!settings.CONNECTIONS.contains("catalog")) {
                            System.out.println("ERROR: No catalog found!");
                        }
                        if (settings.NUMNODES <= 0) {
                            System.out.println("ERROR: No cluster nodes found!");
                        }
                    }
                } catch (FileNotFoundException ex) {
                    printErrorNotice("A given path was invalid!");
                } catch (IOException ex) {
                    printErrorNotice("IOException!");
                }
                
            } else if (args.length == 3) { //READ CSV
                if (args[1].equals("-csv")) {
                    CLUSTERCFG = args[0];
                    CSVFILE = args[2];
                    System.out.println("Load CSV");
                    try {
                        settings = readCFG(CLUSTERCFG);
                        entries = readCSV(CSVFILE);
                        
                        if (DEBUG == true) {
                            System.out.println("ClusterCFG Settings:");
                            
                            String debug_string = "\n\ttablename: " + settings.TABLENAME
                                    + "\n\tnumnodes: " + settings.NUMNODES
                                    + "\n\tpart method: " + settings.PARTITION_METHOD
                                    + "\n\tpart column: " + settings.PARTITION_COLUMN;
                            
                            System.out.println(debug_string);
                            System.out.println(settings.CONNECTIONS.toString());
                            
                            System.out.println("CSV Entries:");
                            for (String[] n : entries) {
                                for (String m : n) {
                                    System.out.println("\t" + m);
                                }
                            }
                        }
                        
                        if (settings.CONNECTIONS.contains("catalog") && (settings.NUMNODES > 0)) {
                            
                            try {
                                //add node info to nodecluster
                                for (int z = 0; z < settings.CONNECTIONS.size(); z++) {
                                    if (!settings.CONNECTIONS.get(z).identifier.equals("catalog")) {
                                        nodecluster.add(settings.CONNECTIONS.get(z));
                                    }
                                }
                                
                                //for each query item, run it on each node in nodecluster
                                //TODO perhaps make this changable here if the user selects any different partition config
                                //TODO make changable via partition type
                                switch (settings.PARTITION_METHOD) {
                                    case 0:
                                        NoPart();
                                        break;
                                    case 1:
                                        RangePart();
                                        break;
                                    case 2:
                                        HashPart();
                                        break;
                                    default:
                                        System.out.println("ERROR: No partition method specified.");
                                        break;
                                }
                                
                            } catch (Exception e) {
                                System.out.println("ERROR: Could not connect to the catalog database!");
                                e.printStackTrace();
                            }
                        } else {
                            if (!settings.CONNECTIONS.contains("catalog")) {
                                System.out.println("ERROR: No catalog found!");
                            }
                            if (settings.NUMNODES <= 0) {
                                System.out.println("ERROR: No cluster nodes found!");
                            }
                        }
                    } catch (IOException ex) {
                        System.out.println("ERROR: IOException!");
                        //Logger.getLogger(SQLProc.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    printErrorNotice("Invalid arguments!");
                }
            } else {
                printErrorNotice("Invalid arguments!");
            }
        /*} catch (ClassNotFoundException ex) {
            System.out.println("ERROR: ClassNotFoundException!");
            //Logger.getLogger(SQLProc.class.getName()).log(Level.SEVERE, null, ex);
        }*/
    }

    /**
     * Entries are added to each database.
     */
    public static void NoPart() {
        for (String[] s : entries) {
            String queryBuffer = "";
            for (String t : s) {
                queryBuffer = queryBuffer + "\"" + t + "\",";
            }
            queries.add(String.format("insert into %s values (%s)", settings.TABLENAME, queryBuffer.substring(0, (queryBuffer.length() - 1))));
        }
        boolean state = true;
        String queryType;
        String tableName;
        Conn catalog = settings.CONNECTIONS.get("catalog");
        CreateCatalog(catalog);

        queryType = "INSERT";
        tableName = settings.TABLENAME;

        //for each node, start a Query session
        for (Conn node : nodecluster) {
            QuerySession session = new QuerySession(); //TODO make threaded, implement runnable
            int successfulQueries = session.RunQuery(node, queries);
            System.out.println("[" + node.hostname + "]: " + successfulQueries + " rows inserted.");

            if (successfulQueries != queries.size()) {
                state = false;
            }
        }
        if (state == true) {
            UpdateCatalog(catalog, tableName, queryType, nodecluster, settings);
        } else { //otherwise, announce query failure
            System.out.println("ERROR: One or more query operations failed. Catalog not updated.");
        }
    }

    /**
     * Entries are added based on a given range.
     */
    public static void RangePart() {
        boolean state = true;
        String queryType;
        String tableName;
        Conn catalog = settings.CONNECTIONS.get("catalog");
        CreateCatalog(catalog);

        queryType = "INSERT";
        tableName = settings.TABLENAME;

        //for each node, start a Query session
        for (Conn node : nodecluster) {
            ArrayList<String> nodeQueries = new ArrayList<>();
            int columnIndex = GetColumnIndex(node, settings.PARTITION_COLUMN, tableName);

            for (String[] s : entries) {
                if ((Integer.parseInt(s[columnIndex]) > Integer.parseInt(node.partparam1)) && (Integer.parseInt(s[columnIndex]) <= Integer.parseInt(node.partparam2))) {
                    String queryBuffer = "";
                    for (String t : s) {
                        queryBuffer = queryBuffer + "\"" + t + "\",";
                    }
                    nodeQueries.add(String.format("insert into %s values (%s)", settings.TABLENAME, queryBuffer.substring(0, (queryBuffer.length() - 1))));
                }
            }

            QuerySession session = new QuerySession(); //TODO make threaded, implement runnable
            int successfulQueries = session.RunQuery(node, nodeQueries);
            System.out.println("[" + node.hostname + "]: " + successfulQueries + " rows inserted.");

            if (successfulQueries != nodeQueries.size()) {
                state = false;
            }
        }
        if (state == true) {
            UpdateCatalog(catalog, tableName, queryType, nodecluster, settings);
        } else { //otherwise, announce query failure
            System.out.println("ERROR: One or more query operations failed. Catalog not updated.");
        }
    }

    public static void HashPart() {
        boolean state = true;
        String queryType;
        String tableName;
        Conn catalog = settings.CONNECTIONS.get("catalog");
        CreateCatalog(catalog);

        queryType = "INSERT";
        tableName = settings.TABLENAME;

        //for each node, start a Query session
        for (int x = 0; x < nodecluster.size(); x++) {
            Conn node = nodecluster.get(x);
            ArrayList<String> nodeQueries = new ArrayList<>();
            int columnIndex = GetColumnIndex(node, settings.PARTITION_COLUMN, tableName);
            for (String[] s : entries) {
                if ((Integer.parseInt(s[columnIndex]) % Integer.parseInt(node.partparam1)) == (x)) {
                    String queryBuffer = "";
                    for (String t : s) {
                        queryBuffer = queryBuffer + "\"" + t + "\",";
                    }
                    nodeQueries.add(String.format("insert into %s values (%s)", settings.TABLENAME, queryBuffer.substring(0, (queryBuffer.length() - 1))));
                }
            }
            QuerySession session = new QuerySession(); //TODO make threaded, implement runnable
            int successfulQueries = session.RunQuery(node, nodeQueries);
            System.out.println("[" + node.hostname + "]: " + successfulQueries + " rows inserted.");

            if (successfulQueries != nodeQueries.size()) {
                state = false;
            }
        }
        if (state == true) {
            UpdateCatalog(catalog, tableName, queryType, nodecluster, settings);
        } else { //otherwise, announce query failure
            System.out.println("ERROR: One or more query operations failed. Catalog not updated.");
        }
    }

    /**
     * RunQueryThreads
     *
     * Runs a given query in a mirrored configuration, starting a thread for
     * each.
     *
     */
    public static void RunQueryThreads() { //TODO reconfigure for flexibility with other modules

        boolean state = true;
        String[] parseResults;
        String queryType;
        String tableName;
        Conn catalog = settings.CONNECTIONS.get("catalog");
        CreateCatalog(catalog);

        for (String query : queries) {

            //advanced query parser here
            parseResults = ParseQuery(query);

            queryType = parseResults[0];
            tableName = parseResults[1];

            ArrayList<Thread> allThreads = new ArrayList<>();
            Thread instance = new Thread();
            //for each node, start a thread
            for (Conn node : nodecluster) {
                instance = new Thread((Runnable) new QueryThread(node, query, queryType));
                instance.start();
                allThreads.add(instance);
            }
            //only joins the threads once all have started, to prevent them from being forced to execute in a specific order
            //joining the threads should force them to finish before results are counted
            for (Thread n : allThreads) {
                try {
                    n.join();
                } catch (InterruptedException ex) {
                    Logger.getLogger(SQLProc.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            //reads the results of each thread run
            for (Boolean n : runResults) {
                if (n == null || n == false) {
                    state = false;
                }
            }
            runResults.clear();

            //if all threads are successful, update the catalog's database
            if (state == true) {
                UpdateCatalog(catalog, tableName, queryType, nodecluster);
            } else { //otherwise, announce query failure
                System.out.println("ERROR: One or more query operations failed. Catalog not updated.");
            }
            state = true;
        }
    }
}
