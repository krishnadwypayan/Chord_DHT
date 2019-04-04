public class ChordMain {

    public static final String FIND_SUCCESSOR = "Find Successor";
    public static final String SUCCESSOR_FOUND = "Successor Found";
    public static final long CHORD_RING_SIZE = 4294967296L;    // m = 32; RingSize = 2^32

    public static void main(String[] args) {
        // If 2 args are given, its the first node in the Chord Ring.
        // Else if args = 4, a new node joins the Ring by contacting
        // an existing node.
        if (args.length == 2) {
            new ChordNode(args[0], Integer.valueOf(args[1]));
        } else if (args.length == 4) {
            new ChordNode(args[0], Integer.valueOf(args[1]), args[2], Integer.valueOf(args[3]));
        } else {
            System.err.println("Incorrect args passed!");
            System.out.println("To start a new ChordRing, give 2 args: [address] [port]");
            System.out.println("To join a new ChordNode, give 4 args: [address] [port] [existingAddress] [existingPort]");
            System.exit(1);
        }
    }
}
