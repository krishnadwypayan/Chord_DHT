import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.Buffer;

public class ChordNodeStabilizer implements Runnable {

    private ChordNode chordNode;
    private static int initialStabilizerDelay = 1000;
    private static int stabilizerDelayTime = 10;

    ChordNodeStabilizer(ChordNode chordNode) {
        this.chordNode = chordNode;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(initialStabilizerDelay);

            // Periodically call the node stabilizer to verify n's
            // immediate successor, and tell the successor about n.
            while (true) {
                stabilize();
                fixFingers();

                Thread.sleep(stabilizerDelayTime);
            }


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void stabilize() {
        // Establish a connection with successor if it is not self
        if ((!this.chordNode.getSuccessor().getAddress().equals(this.chordNode.getAddress()))
                || (this.chordNode.getSuccessor().getPort() != this.chordNode.getPort())) {
            try {
                Socket socket = new Socket(this.chordNode.getSuccessor().getAddress(), this.chordNode.getSuccessor().getPort());
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Request for the successor's predecessor
                socketWriter.println(ChordMain.FIND_PREDECESSOR + ": for current node");
                System.out.println("Sent => " + ChordMain.FIND_PREDECESSOR + ": for current node");
                String response = socketReader.readLine();
                System.out.println("Received => " + response);

                String[] x = response.split(":");
                String xAddress = x[0];
                int xPort = Integer.valueOf(x[1]);

                // If the successor's predecessor is not ourselves, then we need to update our successor.
                // Then inform the successor that we are its new predecessor.
                if ((!this.chordNode.getAddress().equals(xAddress))
                        || (this.chordNode.getPort() != xPort)) {

                    this.chordNode.acquire();

                    // Update our successor
                    Finger newSuccessor = new Finger(xAddress, xPort);
                    this.chordNode.getFingerTable().put(0, newSuccessor);
                    this.chordNode.setSuccessor(newSuccessor);

                    // Establish a connection with the newSuccessor
                    socket = new Socket(xAddress, xPort);
                    socketWriter = new PrintWriter(socket.getOutputStream(), true);

                    // Notify the newSuccessor that we are the new predecessor
                    String notification = ChordMain.NEW_PREDECESSOR + ":" + this.chordNode.getAddress() + ":" + this.chordNode.getPort();
                    socketWriter.println(notification);
                    System.out.println("Sent => " + notification);

                    this.chordNode.release();
                }

                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void fixFingers() {
        // Establish a connection with successor if it is not self
        if ((!this.chordNode.getSuccessor().getAddress().equals(this.chordNode.getAddress()))
                || (this.chordNode.getSuccessor().getPort() != this.chordNode.getPort())) {

            // Establish a connection with our successor and refresh FingerTable
            try {
                Socket socket = new Socket(this.chordNode.getSuccessor().getAddress(), this.chordNode.getSuccessor().getPort());
                PrintWriter socketWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                BigInteger bigPow = BigInteger.valueOf(2L);
                BigInteger bigCurrentNodeId = BigInteger.valueOf(this.chordNode.getId());

                this.chordNode.acquire();

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
                    this.chordNode.getFingerTable().put(i, new Finger(address, port));
                }

                // Set the successor to the first entry in the fingerTable
                this.chordNode.setSuccessor(this.chordNode.getFingerTable().get(0));

                this.chordNode.release();

                socketWriter.close();
                socketReader.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
