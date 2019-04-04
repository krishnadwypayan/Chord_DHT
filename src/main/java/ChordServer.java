import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ChordServer implements Runnable {

    private ChordNode chordNode;

    ChordServer(ChordNode chordNode) {
        this.chordNode = chordNode;
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.chordNode.getPort());
            System.out.println("Started server. Listening for clients...");
            while (true) {
                // Keep listening for clients
                Socket clientSocket = serverSocket.accept();
                System.out.println("Connection established with " + clientSocket.getLocalAddress() + " "+ clientSocket.getLocalPort());

                // Launch ChordClient threads to perform the various operations
                new Thread(new ChordClient(this.chordNode, clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
