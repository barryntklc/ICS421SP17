package SQLProc;

import structures.CFGSettings;
import static SQLProc.SQLProc.nodecluster;
import static io.Util.printSQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import structures.Conn;

/**
 * Database Ops
 *
 * Contains methods for database operations.
 *
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class DatabaseOps {

    /**
     * CreateCatalog
     *
     * Creates a table in the catalog table if it does not exist.
     *
     * @param catalog
     */
    public static void CreateCatalog(Conn catalog) {
        try {
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
            printSQLException(e);
            //System.out.println("ERROR: SQLException - could not create a catalog table!");
        }
    }

    /**
     * UpdateCatalog
     *
     * @param catalog
     * @param tablename
     * @param queryType
     * @param nodes
     */
    public static void UpdateCatalog(Conn catalog, String tablename, String queryType, ArrayList<Conn> nodes) {
        try {
            //connect to the designated catalog database
            Connection catSession = DriverManager.getConnection(catalog.hostname, catalog.username, catalog.password);
            Statement updateCatalogTable = catSession.createStatement();
            catSession.setAutoCommit(false);

            switch (queryType) {
                case "CREATE":
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
                    break;
                case "DROP":
                    catSession.setAutoCommit(false);
                    for (int n = 0; n < nodecluster.size(); n++) {
                        Conn c = nodecluster.get(n);
                        String updateQuery = "DELETE FROM " + catSession.getCatalog() + ".dtables WHERE tname='" + tablename + "' AND nodeid=" + n + ";";
                        updateCatalogTable.executeUpdate(updateQuery);
                        catSession.commit();
                    }
                    catSession.setAutoCommit(true);
                    System.out.println("[" + catalog.hostname + "]: catalog updated.");
                    break;
                case "SELECT":
                    System.out.println("hello");
                    break;
                default:
                    System.out.println("ERROR: The catalog operation \"" + queryType + "\" is not yet supported!");
                    break;
            }
        } catch (SQLException e) {
            System.out.println("ERROR: SQLException - could not run the UpdateCatalog query!");
            //e.printStackTrace();
        }
    }

    /**
     * UpdateCatalog
     *
     * @param catalog
     * @param tablename
     * @param queryType
     * @param nodes
     * @param settings
     */
    public static void UpdateCatalog(Conn catalog, String tablename, String queryType, ArrayList<Conn> nodes, CFGSettings settings) {
        try {
            //connect to the designated catalog database
            Connection catSession = DriverManager.getConnection(catalog.hostname, catalog.username, catalog.password);
            Statement updateCatalogTable = catSession.createStatement();
            catSession.setAutoCommit(false);

            switch (queryType) {
                case "INSERT":
                    catSession.setAutoCommit(false);
                    for (int n = 0; n < nodecluster.size(); n++) {
                        Conn c = nodecluster.get(n);
                        String updateQuery = "INSERT INTO " + catSession.getCatalog() + ".dtables (tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2) VALUES ("
                                + "'" + tablename + "', "
                                + "'" + c.driver + "', "
                                + "'" + c.hostname + "', "
                                + "'" + c.username + "', "
                                + "'" + c.password + "', "
                                + "'" + settings.PARTITION_METHOD + "', "
                                + "'" + n + "', "
                                + "'" + settings.PARTITION_COLUMN + "', "
                                + "'" + c.partparam1 + "', "
                                + "'" + c.partparam2 + "' "
                                + ");";

                        if (SQLProc.DEBUG == true) {
                            System.out.println(updateQuery);
                        }

                        updateCatalogTable.executeUpdate(updateQuery);
                        catSession.commit();
                    }
                    catSession.setAutoCommit(true);
                    System.out.println("[" + catalog.hostname + "]: catalog updated.");
                    break;
                default:
                    System.out.println("ERROR: The catalog operation \"" + queryType + "\" is not yet supported!");
                    break;
            }
        } catch (SQLException e) {
            printSQLException(e);
            System.out.println("ERROR: SQLException - could not run the UpdateCatalog query!");
            //e.printStackTrace();
        }
    }

    /**
     * GetColumnIndex
     * 
     * Using a given identifier, return the column index of the field on a table
     * 
     * Used source:
     * https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
     *
     * @param connection
     * @param identifier
     * @param tablename
     * @return
     */
    public static int GetColumnIndex(Conn connection, String identifier, String tablename) {
        int index = -1;

        Statement n = null;
        String query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "';";

        try {
            n = DriverManager.getConnection(connection.hostname, connection.username, connection.password).createStatement();
            ResultSet rs = n.executeQuery(query);
            int x = 0;
            while (rs.next()) {
                if (rs.getString("COLUMN_NAME").equals(identifier)) {
                    index = x;
                    break;
                }
                x++;
            }
        } catch (SQLException e) {
            //TODO error handling here
        }
        return index;
    }
    
    /**
     * ClusterTest
     *
     * Tests connections to each of the connections described in the clustercfg
     * file.
     */
    public static void ClusterTest(CFGSettings settings) {
        for (int x = 0; x < settings.CONNECTIONS.size(); x++) {
            System.out.println(String.format("Testing connection \"%s\" (%s@%s)", settings.CONNECTIONS.get(x).identifier, settings.CONNECTIONS.get(x).username, settings.CONNECTIONS.get(x).hostname));
            try {
                Connection connect = DriverManager.getConnection(settings.CONNECTIONS.get(x).hostname, settings.CONNECTIONS.get(x).username, settings.CONNECTIONS.get(x).password);
                System.out.println("Successfully connected to database " + "\"" + settings.CONNECTIONS.get(x).identifier + "\".");
            } catch (Exception e) {
                System.out.println("ERROR: Could not connect to database " + "\"" + settings.CONNECTIONS.get(x).identifier + "\"!");
            }
        }
    }
}
