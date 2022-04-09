package os.hw1.master;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;

public class CacheServer {
    public static CacheServerHandler cacheServerHandler;
    public static void main(String[] args) throws IOException {
        cacheServerHandler = new CacheServerHandler();
        new Thread(cacheServerHandler).start();
    }
}

class Cache {
    public int input;
    public int program;
    public int output;

    public Cache(int input, int program, int output) {
        this.input = input;
        this.program = program;
        this.output = output;
    }

    public int getInput() {
        return input;
    }

    public int getProgram() {
        return program;
    }

    public int getOutput() {
        return output;
    }
}

class CacheServerHandler implements Runnable {

    public long pid;
    public int port;
    public ServerSocket serverSocket;
    public LinkedList<Cache> caches;


    @Override

    public void run() {
        try {
            pid = ProcessHandle.current().pid();
            serverSocket = new ServerSocket(1282);
            port = serverSocket.getLocalPort();
            caches=new LinkedList<>();
            //System.out.println(port);
            //    System.out.println("["+pid+"]Worker running on: " + serverSocket.getInetAddress().getHostAddress()+":"+port);
            while (!serverSocket.isClosed()) {

                Socket socket = serverSocket.accept();
                //create a new connection
                CacheConnectionHandler cacheConnectionHandler = new CacheConnectionHandler(this, socket);
                cacheConnectionHandler.start();
                System.out.println(ProcessHandle.current().pid() + ": main server connected to Cache as socket " + socket + " in connection " + cacheConnectionHandler.getName());

            }

        } catch (Exception e) {

            System.out.println(ProcessHandle.current().pid() + ": Something went wrong while creating the serversocket for worker");

        } finally {
            try {
                if (serverSocket != null & !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {

                System.out.println(ProcessHandle.current().pid() + ": Something went wrong while closing the serversocket for worker");

            }
        }

    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }
}

class CacheConnectionHandler extends Thread implements RequestHandler {

    Socket socket;
    CacheServerHandler cacheServerHandler;

    private
    InputStream inputStream;
    private
    OutputStream outputStream;
    private
    BufferedReader bufferedReader;
    private PrintWriter printWriter;

    public CacheConnectionHandler() {

    }

    public CacheConnectionHandler(CacheServerHandler cacheServerHandler, Socket socket) {
        this.cacheServerHandler = cacheServerHandler;
        this.socket = socket;
    }

    @Override
    public void run() {

        try {
            inputStream = socket.getInputStream();
        } catch (IOException ioException) {
            System.out.println(ProcessHandle.current().pid() + ": Error in input stream of" + this.getId());
        }
        try {
            outputStream = socket.getOutputStream();
        } catch (IOException ioException) {
            System.out.println(ProcessHandle.current().pid() + ": Error in out stream of" + this.getId());
        }
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        printWriter = new PrintWriter(outputStream, true);

        while (true) {
            String request = getRequest(bufferedReader);
            if (request == null) {
                System.out.println(ProcessHandle.current().pid() + ": " + this.getName() + " disconnected");
                return;
            }
           // System.out.println(ProcessHandle.current().pid()+": Getting Request from main server  :[" + request + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            handleRequest(request);

            // sendResponse(printWriter, "hello");

        }
    }

    public void handleRequest(String request) {
        String response = null;
        String[] strings = request.split(" ");
        int input;
        int program;
        switch (strings[0]) {
            case "get":

                input = Integer.parseInt(strings[1]);
                program = Integer.parseInt(strings[2]);
                response = "Not Found";
                for (Cache cache : cacheServerHandler.caches) {
                    if (cache.input == input && cache.program == program) {
                        response = String.valueOf(cache.output);
                        break;
                    }
                }
                break;
            case "insert":
                input = Integer.parseInt(strings[1]);
                program = Integer.parseInt(strings[2]);
                int output = Integer.parseInt(strings[3]);
                Cache cache = new Cache(input, program, output);
                cacheServerHandler.caches.add(cache);
                response = "cache added";
        }
        sendResponse(printWriter, response);
    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public CacheConnectionHandler setBufferedReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
        return this;
    }

    public PrintWriter getPrintWriter() {
        return printWriter;
    }

    public CacheConnectionHandler setPrintWriter(PrintWriter printWriter) {
        this.printWriter = printWriter;
        return this;
    }


    public String getRequest(BufferedReader bufferedReader) {

        String request = null;
        try {
            request = bufferedReader.readLine();
        } catch (IOException e) {
            System.out.println(ProcessHandle.current().pid() + " :Error in getting request.");
        }
        return request;
    }

    public Socket getSocket() {
        return socket;
    }

    public void sendResponse(PrintWriter printWriter, String response) {
     //    System.out.println(ProcessHandle.current().pid()+": Sending Response to main server:[" + response + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        printWriter.println(response);

    }
}





