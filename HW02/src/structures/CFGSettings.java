package structures;

/**
 * CFGSettings
 *
 * Stores settings from a clustercfg file
 *
 * @author Barryn Chun
 * @date February 18, 2017
 */
public class CFGSettings {

    public ConnList CONNECTIONS = new ConnList();

    public String TABLENAME = "";
    public int NUMNODES = 0;
    public int PARTITION_METHOD = 0;
    public String PARTITION_COLUMN = "";

    public CFGSettings() {

    }
}
