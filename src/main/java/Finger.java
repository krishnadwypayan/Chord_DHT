public class Finger {

    private String address;
    private int port;
    private long id;

    Finger(String address, int port) {
        this.address = address;
        this.port = port;

        SHA1Hasher sha1Hasher = new SHA1Hasher(this.address + ":" + this.port);
        this.id = sha1Hasher.getLong();
    }

    public long getId() {
        return id;
    }

    public String getAddress() { return address; }

    public int getPort() { return port; }

}
