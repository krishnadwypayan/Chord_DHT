import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ChordClient implements Runnable {

    private ChordNode chordNode;
    private Socket clientSocket;

    ChordClient(ChordNode chordNode, Socket clientSocket) {
        this.chordNode = chordNode;
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            // Create readers/writers from socket stream
            PrintWriter socketWriter = new PrintWriter(this.clientSocket.getOutputStream(), true);
            BufferedReader socketReader = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));

            String line;
            // Get the command from the ChordNode as "command : arg"
            while ((line = socketReader.readLine()) != null) {
                String[] socketReaderLine = line.split(":");
                String command = socketReaderLine[0];
                String arg = socketReaderLine[1];
                System.out.println("Received => " + socketReaderLine[0] + ":" + socketReaderLine[1]);

                switch (command) {
                    case ChordMain.FIND_SUCCESSOR:
                        // Find the successor of the id received from the InputStream
                        Finger successor = findSuccessor(Long.valueOf(arg));

                        // Populate the response back to the node
                        socketWriter.println(ChordMain.SUCCESSOR_FOUND + ":" + successor.getAddress() + ":" + successor.getPort());
                        System.out.println("Sent => " + ChordMain.SUCCESSOR_FOUND + ":" + successor.getAddress() + ":" + successor.getPort());

                        break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Ask the current node to find the successor of id
    private Finger findSuccessor(long id) {
        id = id % ChordMain.CHORD_RING_SIZE;

        if (id == this.chordNode.getSuccessor().getId()) {
            return this.chordNode.getSuccessor();
        }

        if (id > this.chordNode.getId() && id <= this.chordNode.getSuccessor().getId()) {
            return this.chordNode.getSuccessor();
        } else {
            Finger n_ = closestPrecedingNode(id);
            System.out.println("Finding successor for: " + n_.getId());
            return findSuccessor(n_.getId());
        }
    }

    // Search the local finger table for the highest predecessor pf the id
    private Finger closestPrecedingNode(long id) {
        for (int i = 31; i >= 0; i--) {
            long fingerId = this.chordNode.getFingerTable().get(i).getId();
            if (fingerId > this.chordNode.getId() && fingerId < id) {
                return this.chordNode.getFingerTable().get(i);
            }
        }
        return new Finger(this.chordNode.getAddress(), this.chordNode.getPort());
    }
}
