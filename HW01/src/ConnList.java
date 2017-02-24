import java.util.ArrayList;

/**
 * ConnList
 * 
 * Stores Conn objects.
 * 
 * @author Barryn Chun
 * @date January 30, 2017
 */
public class ConnList {
    static ArrayList<Conn> items = new ArrayList<Conn>();
    
    public ConnList() {
    }
    
    public boolean contains (String identifier) {
        for (Conn item : items) {
            if (item.identifier.equals(identifier)) {
                return true;
            }
        }
        return false;
    }
    
    public void add (Conn item) {
        items.add(item);
    }
    
    public void add (String identifier, String driver, String hostname, String username, String password) {
        items.add(new Conn(identifier, driver, hostname, username, password));
    }
    
    public Conn get (int index) {
        return items.get(index);
    }
    
    public Conn get (String identifier) {
        for (Conn item : items) {
            if (item.identifier.equals(identifier)) {
                return item;
            }
        }
        return null;
    }
    
    public int size () {
        return items.size();
    }
    
    @Override
    public String toString() {
        String buffer = "";
        for (Conn item : items) {
            buffer = buffer + '\n' + item.toString();
        }
        return buffer;
    }
}
