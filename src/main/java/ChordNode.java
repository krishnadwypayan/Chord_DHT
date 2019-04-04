import java.util.Map;

public class ChordNode {

    private String address, existingAddress = null;
    private Finger predecessor = null;
    private Finger successor;
    private int port, existingPort;
    private long id;
    private String hex;
    private Map<Integer, Finger> fingerTable;

    // Create a new Chord Ring
    ChordNode(String address, int port) {
        this.address = address;
        this.port = port;

        SHA1Hasher sha1Hash = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hash.getLong();
        this.hex = sha1Hash.getHex();

        this.successor = new Finger(this.address, this.port);
    }

    // If a Chord Ring already exists, an existing node is contacted by the node
    // that wishes to join the network.
    ChordNode(String address, int port, String existingAddress, int existingPort) {
        this.address = address;
        this.port = port;
        this.existingAddress = existingAddress;
        this.existingPort = existingPort;

        SHA1Hasher sha1Hash = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hash.getLong();
        this.hex = sha1Hash.getHex();
    }
    
    private void initializeFingerTable() {

    }

    public String getAddress() { return address; }

    public String getExistingAddress() { return existingAddress; }

    public int getPort() { return port; }

    public int getExistingPort() { return existingPort; }

    public long getId() { return id; }

    public String getHex() { return hex; }

    public Finger getPredecessor() { return predecessor; }

    public Finger getSuccessor() { return successor; }

    public Map<Integer, Finger> getFingerTable() { return fingerTable; }

}
