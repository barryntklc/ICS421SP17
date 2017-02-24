/**
 * Conn
 * 
 * Stores the variables defined in a connection given in the clustercfg file.
 * 
 * @author Barryn Chun
 * @date January 30, 2017
 */
public class Conn {
    public String identifier = "";
    
    public String driver = "";
    public String hostname = "";
    public String username = "";
    public String password = "";
    
    public Conn (String identifier, String driver, String hostname, String username, String password) {
        this.identifier = identifier;
        this.driver = driver;
        this.hostname = hostname;
        this.username = username;
        this.password = password;
    }
    
    @Override
    public String toString () {
        return String.format("connection: %s\ndriver: %s\nhostname: %s\nusername: %s\npassword: %s\n", identifier, driver, hostname, username, password);
    }
}
