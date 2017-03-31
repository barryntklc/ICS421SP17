package SQLProc;

import static io.Util.printSQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import structures.Conn;

/**
 * QuerySession
 * 
 * A modified version of QueryThread, perhaps to be made runnable in the future
 * 
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class QuerySession {
    public static int RunQuery(Conn connection, ArrayList<String> queries) {
        int successfulQueries = 0;
        
        try {
            Connection querySession = DriverManager.getConnection(connection.hostname, connection.username, connection.password);

            PreparedStatement addCatalogTable = null;
            try {
                querySession.setAutoCommit(false);
                
                for (String q : queries) {
                    addCatalogTable = querySession.prepareStatement(q);
                    addCatalogTable.executeUpdate();
                    successfulQueries++;
                }
                
                querySession.commit();
            } catch (SQLException e) {
                printSQLException(e);
            } finally {
                querySession.setAutoCommit(true);
            }
        } catch (Exception e) {
            System.out.println(String.format("ERROR: Could not open a connection on \"%s\"!", connection.hostname));
            System.out.println(e.getMessage());
        }
        return successfulQueries;
    }
}
