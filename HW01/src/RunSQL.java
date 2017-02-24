import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DDL Processor 1.0
 *
 * ICS 421 HW 1
 *
 * @author Barryn Chun
 * @date January 30, 2017
 */
public class RunSQL {
    
    static String CLUSTERCFG = "";
    static String DDLFILE = "";
    static boolean DEBUG = false;

    static ConnList connections = new ConnList();
    static ArrayList<Conn> nodecluster = new ArrayList<Conn>();
    static ArrayList<String> queries = new ArrayList<String>();
    static int nodes = 0;
    static ArrayList<Boolean> runResults = new ArrayList<Boolean>();

    /**
     * Main
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 2) {
            CLUSTERCFG = args[0];
            DDLFILE = args[1];
            //read clustercfg - TODO: perhaps move to separate method for continuity
            try {
                BufferedReader readClusterConfig = new BufferedReader(new FileReader(CLUSTERCFG));
                String x = readClusterConfig.readLine();
                while (x != null) {

                    if (x.contains(".")) {
                        String identifier = x.substring(0, x.indexOf('.'));

                        if (connections.contains(x.substring(0, x.indexOf('.')))) { //else if catalog exists, error
                            System.out.println("Connection %s already exists! Skipping this entry.");
                        } else { //if catalog does not exist, get next lines
                            String driver = x.substring(x.indexOf('=') + 1);
                            x = readClusterConfig.readLine();
                            String hostname = x.substring(x.indexOf('=') + 1);
                            x = readClusterConfig.readLine();
                            String username = x.substring(x.indexOf('=') + 1);
                            x = readClusterConfig.readLine();
                            String password = x.substring(x.indexOf('=') + 1);
                            //TODO add exception catch if this part fails due to incorrect formatting

                            connections.add(identifier, driver, hostname, username, password);
                        }
                    } else if ((x.length() > 9) && (x.substring(0, 9).equals("numnodes="))) {
                        nodes = Integer.parseInt(x.substring(9));
                    }
                    x = readClusterConfig.readLine();
                }
                ReadDDLFile();

                if (DEBUG == true) {
                    ClusterTest();
                }

                //check if a catalog is found and there are nodes
                if (connections.contains("catalog") && (nodes > 0)) {

                    try {
                        //add node info to nodecluster
                        for (int z = 0; z < connections.size(); z++) {
                            if (!connections.get(z).identifier.equals("catalog")) {
                                nodecluster.add(connections.get(z));
                            }
                        }

                        //for each query item, run it on each node in nodecluster
                        //TODO perhaps make this changable here if the user selects any different partition config
                        RaidOne();
                    } catch (Exception e) {
                        System.out.println("ERROR: Could not connect to the catalog database!");
                        e.printStackTrace();
                    }
                } else {
                    if (!connections.contains("catalog")) {
                        System.out.println("ERROR: No catalog found!");
                    }
                    if (nodes <= 0) {
                        System.out.println("ERROR: No cluster nodes found!");
                    }
                }
            } catch (FileNotFoundException ex) {
                System.out.println("ERROR: A given path was invalid!");
                System.out.println("Syntax: ./run.sh [clustercfg path] [ddlfile path] ");
            } catch (IOException ex) {
                Logger.getLogger(RunSQL.class.getName()).log(Level.SEVERE, null, ex);
            }

        } else {
            System.out.println("ERROR: Invalid arguments!");
            System.out.println("Syntax: ./run.sh [clustercfg path] [ddlfile path] ");
        }
    }

    /**
     * RaidOne
     * 
     * Runs a given query in a mirrored configuration, starting a thread for each.
     * 
     */
    public static void RaidOne() {
        boolean state = true;
        boolean currentState;
        String queryType;
        Conn catalog = connections.get("catalog");
        CreateCatalog();

        for (String query : queries) {
            queryType = ParseQuery(query).replaceAll("\\s", "");
            String tablename = "";
            if (queryType.equals("CREATE")) {
                tablename = query.substring((query.indexOf("TABLE ") + 6), query.indexOf('('));
            } else if (queryType.equals("DROP")) {
                tablename = query.substring((query.indexOf("TABLE ") + 6), query.indexOf(';'));
            }
            ArrayList<Thread> allThreads = new ArrayList<Thread>();
            Thread instance = new Thread();
            //for each node, start a thread
            for (Conn node : nodecluster) {
                instance = new Thread((Runnable) new QueryHandler(node, query));
                instance.start();
                allThreads.add(instance);
            }
            //only joins the threads once all have started, to prevent them from being forced to execute in a specific order
            //joining the threads should force them to finish before results are counted
            for (Thread n : allThreads) {
                try {
                    n.join();
                } catch (InterruptedException ex) {
                    Logger.getLogger(RunSQL.class.getName()).log(Level.SEVERE, null, ex);
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
                UpdateCatalog(catalog, tablename, queryType);
            } else { //otherwise, announce query failure
                System.out.println("ERROR: One or more query operations failed. Catalog not updated.");
            }
            state = true;
        }
    }

    /**
     * ParseQuery
     * 
     * This will be a method that parses given queries. May be modified in
     * the future.
     *
     * @param query
     * @return
     */
    public static String ParseQuery(String query) {
        return query.substring(0, query.indexOf(" TABLE"));
    }
    
    /**
     * CreateCatalog
     * 
     * Creates a table in the catalog table if it does not exist.
     */
    public static void CreateCatalog() {
        try {
            Conn catalog = connections.get("catalog");

            //connect to the designated catalog database
            Connection catSession = DriverManager.getConnection(catalog.hostname, catalog.username, catalog.password);
            PreparedStatement addCatalogTable = null;
            try {
                catSession.setAutoCommit(false);
                //add catalog dtables if it does not exist
                addCatalogTable = catSession.prepareStatement("CREATE TABLE IF NOT EXISTS `" + catSession.getCatalog() + "`.`dtables` ("
                        + "`tname` CHAR(32), "
                        + "`nodedriver` CHAR(64), "
                        + "`nodeurl` CHAR(128), "
                        + "`nodeuser` CHAR(16), "
                        + "`nodepasswd` CHAR(16), "
                        + "`partmtd` INT, "
                        + "`nodeid` INT, "
                        + "`partcol` CHAR(32), "
                        + "`partparam1` CHAR(32), "
                        + "`partparam2` CHAR(32));");
                addCatalogTable.executeUpdate();
                //creates a table in the catalog database
                catSession.commit();
            } catch (Exception e) {
                System.out.println("ERROR: Could not access the catalog database!");
                e.printStackTrace();
            } finally {
                catSession.setAutoCommit(true);
            }
        } catch (SQLException e) {
            System.out.println("ERROR: SQLException - could not create a catalog table!");
        }
    }

    /**
     * UpdateCatalog
     * 
     * @param catalog
     * @param tablename
     * @param queryType 
     */
    public static void UpdateCatalog(Conn catalog, String tablename, String queryType) {
        try {
            //connect to the designated catalog database
            Connection catSession = DriverManager.getConnection(catalog.hostname, catalog.username, catalog.password);
            Statement updateCatalogTable = catSession.createStatement();
            catSession.setAutoCommit(false);

            if (queryType.equals("CREATE")) {
                catSession.setAutoCommit(false);
                for (int n = 0; n < nodecluster.size(); n++) {
                    Conn c = nodecluster.get(n);
                    String updateQuery = "INSERT INTO " + catSession.getCatalog() + ".dtables (tname, nodedriver, nodeurl, nodeuser, nodepasswd, nodeid) VALUES ("
                            + "'" + tablename + "', "
                            + "'" + c.driver + "', "
                            + "'" + c.hostname + "', "
                            + "'" + c.username + "', "
                            + "'" + c.password + "', "
                            + "'" + n + "' "
                            + ");";
                    updateCatalogTable.executeUpdate(updateQuery);
                    catSession.commit();
                }
                catSession.setAutoCommit(true);

                System.out.println("[" + catalog.hostname + "]: catalog updated.");
            } else if (queryType.equals("DROP")) {
                catSession.setAutoCommit(false);
                for (int n = 0; n < nodecluster.size(); n++) {
                    Conn c = nodecluster.get(n);
                    String updateQuery = "DELETE FROM " + catSession.getCatalog() + ".dtables WHERE tname='" + tablename + "' AND nodeid=" + n + ";";
                    updateCatalogTable.executeUpdate(updateQuery);
                    catSession.commit();
                }
                catSession.setAutoCommit(true);

                System.out.println("[" + catalog.hostname + "]: catalog updated.");
            } else {
                System.out.println("ERROR: The catalog operation \"" + queryType + "\" is not yet supported!");
            }
        } catch (SQLException e) {
            System.out.println("ERROR: SQLException - could not run the UpdateCatalog query!");
            e.printStackTrace();
        }
    }

    /**
     * ReadDDLFile
     * 
     * Reads
     */
    public static void ReadDDLFile() {
        BufferedReader readDDLFile;
        try {
            readDDLFile = new BufferedReader(new FileReader(DDLFILE));
            ArrayList<String> lineBuffer = new ArrayList<String>();
            String x;
            while ((x = readDDLFile.readLine()) != null) {
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
                        queries.add(buffer);
                        buffer = "";
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RunSQL.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RunSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * ClusterTest
     * 
     * Tests connections to each of the connections described in the clustercfg file.
     */
    public static void ClusterTest() {
        for (int x = 0; x < connections.size(); x++) {
            System.out.println(String.format("Testing connection \"%s\" (%s@%s)", connections.get(x).identifier, connections.get(x).username, connections.get(x).hostname));
            try {
                Connection connect = DriverManager.getConnection(connections.get(x).hostname, connections.get(x).username, connections.get(x).password);
                System.out.println("Successfully connected to database " + "\"" + connections.get(x).identifier + "\".");
            } catch (Exception e) {
                System.out.println("ERROR: Could not connect to database " + "\"" + connections.get(x).identifier + "\"!");
            }
        }
    }
}
