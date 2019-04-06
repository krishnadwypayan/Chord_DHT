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
                System.out.println("Received => " + line);

                switch (command) {
                    case ChordMain.FIND_SUCCESSOR:
                        // Find the successor of the id received from the InputStream
                        String queryId = socketReaderLine[1];
                        Finger successor = findSuccessor(Long.valueOf(queryId));

                        // Populate the response back to the node
                        socketWriter.println(ChordMain.SUCCESSOR_FOUND + ":" + successor.getAddress() + ":" + successor.getPort());
                        System.out.println("Sent => " + ChordMain.SUCCESSOR_FOUND + ":" + successor.getAddress() + ":" + successor.getPort());

                        break;

                    case ChordMain.NEW_PREDECESSOR:
                        String predAddress = socketReaderLine[1];
                        int predPort = Integer.valueOf(socketReaderLine[2]);

                        // Update the current node's predecessor
                        this.chordNode.acquire();
                        this.chordNode.setPredecessor(new Finger(predAddress, predPort));
                        this.chordNode.release();

                        break;

                    case ChordMain.FIND_PREDECESSOR:
                        System.out.println("Received => " + ChordMain.FIND_PREDECESSOR);
                        String response = this.chordNode.getPredecessor().getAddress() + ":" + this.chordNode.getPredecessor().getPort();
                        socketWriter.println(response);
                        System.out.println("Sent => " + response);
                        break;
                }
            }

            socketReader.close();
            socketWriter.close();
            clientSocket.close();

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
        id = id % ChordMain.CHORD_RING_SIZE;

        for (int i = 31; i >= 0; i--) {
            long fingerId = this.chordNode.getFingerTable().get(i).getId();
            if (fingerId > this.chordNode.getId() && fingerId < id) {
                return this.chordNode.getFingerTable().get(i);
            }
        }
        return new Finger(this.chordNode.getAddress(), this.chordNode.getPort());
    }
}
