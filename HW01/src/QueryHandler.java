import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * QueryHandler
 * 
 * @author Barryn Chun
 */
public class QueryHandler implements Runnable {

    Conn connection;
    String query;
    
    /**
     * QueryHandler
     * 
     * Given a connection and query, attempts to run the query on that given connection
     * 
     * @param connection
     * @param query 
     */
    public QueryHandler (Conn connection, String query) {
        this.connection = connection;
        this.query = query;
    }
    @Override
    public void run() {
        Boolean state = false;
        try {
            //connect to the designated catalog database
            Connection querySession = DriverManager.getConnection(connection.hostname, connection.username, connection.password);
            
            PreparedStatement addCatalogTable = null;
            try {
                querySession.setAutoCommit(false);
                addCatalogTable = querySession.prepareStatement(query);
                addCatalogTable.executeUpdate();
                querySession.commit();
                state = true;
                System.out.println("[" + connection.hostname + "]: " + RunSQL.DDLFILE + " success.");
            } catch (Exception e) {
                state = false;
                System.out.println("[" + connection.hostname + "]: " + RunSQL.DDLFILE + " failed.");
            } finally {
                querySession.setAutoCommit(true);
            }
        } catch (Exception e) {
            System.out.println(String.format("ERROR: Could not open a connection on \"%s\"!", connection.hostname));
            e.printStackTrace();
        }
        RunSQL.runResults.add(state);
    }
    
}
