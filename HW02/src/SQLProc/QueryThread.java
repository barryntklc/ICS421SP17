package SQLProc;

import static io.Util.printSQLException;
import structures.Conn;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * QueryThread
 *
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class QueryThread implements Runnable {

    Conn connection;
    String query;
    String queryType;

    /**
     * QueryHandler
     *
     * Given a connection and query, attempts to run the query on that given
     * connection
     *
     * @param connection
     * @param query
     */
    public QueryThread(Conn connection, String query, String queryType) {
        this.connection = connection;
        this.query = query;
        this.queryType = queryType;
    }

    @Override
    public void run() {

        Boolean state = false;
        try {

            try {
                switch (queryType) {
                    case "SELECT":
                        Statement n = DriverManager.getConnection(connection.hostname, connection.username, connection.password).createStatement();
                        ResultSet rs = n.executeQuery(query);
                        int columns = rs.getMetaData().getColumnCount();
                        //System.out.println(rsmd.getColumnCount());

                        //http://stackoverflow.com/questions/24229442/java-print-the-data-in-resultset
                        while (rs.next()) {
                            ArrayList<String> row = new ArrayList<>();
                            for (int x = 1; x <= columns; x++) {
                                row.add(rs.getString(x));
                                //System.out.println(rs.getString(x));
                            }
                            for (String s : row) {
                                System.out.print(s + '\t');
                            }
                            System.out.println();
                        }
                        break;
                    default:
                        Connection querySession = DriverManager.getConnection(connection.hostname, connection.username, connection.password);
                        PreparedStatement addCatalogTable = null;
                        querySession.setAutoCommit(false);
                        addCatalogTable = querySession.prepareStatement(query);
                        addCatalogTable.executeUpdate();
                        querySession.commit();
                        state = true;
                        System.out.println("[" + connection.hostname + "]: " + SQLProc.SQLFILE + " success.");
                        querySession.setAutoCommit(true);
                        break;
                }
                System.out.println("[" + connection.hostname + "]: " + SQLProc.SQLFILE + " success.");
            } catch (SQLException e) {
                printSQLException(e);
                state = false;
                System.out.println("[" + connection.hostname + "]: " + SQLProc.SQLFILE + " failed.");
            } finally {
                //querySession.setAutoCommit(true);
            }
        } catch (Exception e) {
            System.out.println(String.format("ERROR: Could not open a connection on \"%s\"!", connection.hostname));
            e.printStackTrace();
        }
        SQLProc.runResults.add(state);
    }

}
