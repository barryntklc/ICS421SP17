package structures;

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

    public String partparam1 = "";
    public String partparam2 = "";

    public Conn(String identifier, String driver, String hostname, String username, String password) {
        this.identifier = identifier;
        this.driver = driver;
        this.hostname = hostname;
        this.username = username;
        this.password = password;
    }

    public Conn(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public String toString() {
        return String.format("connection: %s\ndriver: %s\nhostname: %s\nusername: %s\npassword: %s\nparam1: %s\nparam2: %s\n", identifier, driver, hostname, username, password, partparam1, partparam2);
    }

    public void insertVal(String key, String val) {
        switch (key) {
            case "driver":
                this.driver = val;
                break;
            case "hostname":
                this.hostname = val;
                break;
            case "username":
                this.username = val;
                break;
            case "passwd":
                this.password = val;
                break;
            case "param1":
                this.partparam1 = val;
                break;
            case "param2":
                this.partparam2 = val;
                break;
            default:
                //ERROR: unknown parameter
                break;
        }
    }
}
