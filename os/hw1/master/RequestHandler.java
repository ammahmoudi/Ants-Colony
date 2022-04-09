package os.hw1.master;

import java.io.*;
import java.net.Socket;

public interface RequestHandler {
    public void handleRequest(String request) throws IOException;


}
