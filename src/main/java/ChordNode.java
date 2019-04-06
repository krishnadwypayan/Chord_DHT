import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class ChordNode {

    private String address, existingAddress = null;
    private int port, existingPort;
    private long id;
    private String hex;
    private Finger predecessor = null;
    private Finger successor;
    private Map<Integer, Finger> fingerTable;
    private Semaphore semaphore = new Semaphore(1);

    // Create a new Chord Ring
    ChordNode(String address, int port) {
        this.address = address;
        this.port = port;

        // Convert the address and port to SHA1
        SHA1Hasher sha1Hash = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hash.getLong();
        this.hex = sha1Hash.getHex();

        // Print statements
        System.out.println("Welcome to the Chord Network!");
        System.out.println("You are running on IP: " + this.address + " and port: " + this.port);
        System.out.println("Your ID: " + this.getId());

        // initialize finger table and successor
        this.initializeFingerTable();
        this.initializeSuccessors();

        // Launch server thread that will listen to clients
        new Thread(new ChordServer(this)).start();

        // Launch the NodeStabilizer that will stabilize the ChordNode periodically
        new Thread(new ChordNodeStabilizer(this)).start();

        // print the finger table
        this.printFingerTable();
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

        // Print statements
        System.out.println("Welcome to the Chord Network!");
        System.out.println("You are running on IP: " + this.address + " and port: " + this.port);
        System.out.println("Your ID: " + this.getId());

        // initialize finger table and successor
        this.initializeFingerTable();
        this.initializeSuccessors();

        // Launch server thread that will listen to clients
        new Thread(new ChordServer(this)).start();

        this.printFingerTable();
    }

    // Initialize the finger table for the current node.
    // All finger table entries will point to self if this is the only node in the Chord Ring.
    // Else, an existing node will be contacted and will be used to initialize the fingers.
    private void initializeFingerTable() {
        this.fingerTable = new HashMap<>();

        if (this.getExistingAddress() == null) {
            for (int i = 0; i < 32; i++) {
                this.fingerTable.put(i, new Finger(this.getAddress(), this.getPort()));
            }
        } else {
            try {
                // Create a socket that will contact the existing ChordServer for the fingers
                Socket socket = new Socket(this.getExistingAddress(), this.getExistingPort());
                System.out.println("Connection successfully established with: " + this.getExistingAddress() + " " + this.getExistingPort());

                //Create writers/readers for the socket streams
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                BigInteger bigPow = BigInteger.valueOf(2L);
                BigInteger bigCurrentNodeId = BigInteger.valueOf(this.getId());

                for (int i = 0; i < 32; i++) {
                    // Find successor for Node ID (id + 2^i)
                    BigInteger pow = bigPow.pow(i);
                    BigInteger queryId = bigCurrentNodeId.add(pow);

                    // Send the queryId to the client
                    socketWriter.println(ChordMain.FIND_SUCCESSOR + ":" + queryId.toString());
                    System.out.println("Sent => " + ChordMain.FIND_SUCCESSOR + ":" + queryId.toString());

                    // Client response
                    String clientResponse = socketReader.readLine();
                    String[] response = clientResponse.split(":");
                    String address = response[1];
                    int port = Integer.valueOf(response[2]);
                    System.out.println("Received => " + clientResponse);

                    // Put the finger entry into the FingerTable
                    this.getFingerTable().put(i, new Finger(address, port));
                }

                // Close the connections
                socketWriter.close();
                socketReader.close();
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initializeSuccessors() {
        this.successor = this.fingerTable.get(0);
        this.predecessor = new Finger(this.getAddress(), this.getPort());

        // Establish a connection with the successor to notify that we are the new predecessor.
        // The successor should not be equal to the current node.
        if ((!this.successor.getAddress().equals(this.getAddress()))
                || (this.successor.getPort() != this.getPort())) {
            try {
                Socket socket = new Socket(this.successor.getAddress(), this.successor.getPort());
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);

                // Notify the successor that we are the new predecessor
                String notification = ChordMain.NEW_PREDECESSOR + ":" + this.getAddress() + ":" + this.getPort();
                socketWriter.println(notification);
                System.out.println("Sent => " + notification);

                socketWriter.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void printFingerTable() {
        for (Integer i : this.fingerTable.keySet()) {
            System.out.println(i + " " + this.fingerTable.get(i).getId());
        }
    }

    public String getAddress() { return address; }

    public String getExistingAddress() { return existingAddress; }

    public int getPort() { return port; }

    public int getExistingPort() { return existingPort; }

    public long getId() { return id; }

    public String getHex() { return hex; }

    public Finger getPredecessor() { return predecessor; }

    public void setPredecessor(Finger predecessor) {
        this.predecessor = predecessor;
    }

    public Finger getSuccessor() { return successor; }

    public void setSuccessor(Finger successor) {
        this.successor = successor;
    }

    public Map<Integer, Finger> getFingerTable() { return fingerTable; }

    public void acquire() {
        try {
            this.semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void release() {
        this.semaphore.release();
    }

    public Semaphore getSemaphore() { return this.semaphore; }

}
