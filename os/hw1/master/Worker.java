package os.hw1.master;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Worker {
    public static ServerHandler serverHandler;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        serverHandler = new ServerHandler();
       new Thread(serverHandler).start();
//        Thread.sleep(4000);
//        System.exit(0);
      //
        //  Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println(ProcessHandle.current().pid() + ": worker " + serverHandler.id + " stop " + serverHandler.pid + " " + serverHandler.port)));
Runtime.getRuntime().addShutdownHook(new Thread(){
    @Override
    public void run() {
      //  System.out.println("hi");
    }
});


    }
}

class ServerHandler implements Runnable {
    public int id;
    public long pid;
    public int port;
    public int w;
    public int busyWeight;
    public int commonArgsLength;
    public String[] commonArgs;
    public ServerSocket serverSocket;


    @Override
    public void run() {
        try {
            pid = ProcessHandle.current().pid();
            serverSocket = new ServerSocket(0);
            port = serverSocket.getLocalPort();

            System.out.println(port);
            //ystem.out.println("[" + pid + "]Worker running on: " + serverSocket.getInetAddress().getHostAddress() + ":" + port);
            while (!serverSocket.isClosed()) {

                Socket socket = serverSocket.accept();
                //create a new connection
                ConnectionHandler connectionHandler = new ConnectionHandler(this, socket);
                connectionHandler.start();
               // System.out.println(ProcessHandle.current().pid() + ": main server connected as socket " + socket + " in connection " + connectionHandler.getName());

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

class ConnectionHandler extends Thread {
    private ServerHandler serverHandler;
    private Socket socket;
    private
    InputStream inputStream;
    private
    OutputStream outputStream;
    private
    BufferedReader bufferedReader;
    private PrintWriter printWriter;


    public ConnectionHandler(ServerHandler serverHandler, Socket socket) {
        this.serverHandler = serverHandler;
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
                //  System.out.println(ProcessHandle.current().pid() + ": " + this.getName() + " disconnected");
                return;
            }

            try {
                //System.out.println("Getting Request from " + this.getId() + " :[" + request + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

                handleRequest(request);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        //  sendResponse(printWriter, "hello");

    }


    public void handleRequest(String request) throws IOException {
        String response = "hi";
        String[] strings = request.split(" ");

        switch (strings[0]) {
            case "id" -> {
                serverHandler.id = Integer.parseInt(strings[1]);
                response = "id assigned to " + strings[1];
            }
            case "commonArgs" -> {
                strings = request.split(" ");
                serverHandler.commonArgsLength = Integer.parseInt(strings[1]);
                serverHandler.commonArgs = new String[serverHandler.commonArgsLength];
                for (int i = 0; i < serverHandler.commonArgsLength; i++) {
                    serverHandler.commonArgs[i] = strings[i + 2];
                }
                response = "commonArg accepted";
            }
            case "job" -> {
                String programName = strings[1];
                int input = Integer.parseInt(strings[2]);
                //   System.out.println(Arrays.toString(serverHandler.commonArgs));
                ProcessBuilder processBuilder = new ProcessBuilder(
                        serverHandler.commonArgs[0], serverHandler.commonArgs[1], serverHandler.commonArgs[2], programName
                );
                // System.out.println(programName);
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
                // processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                Process program = processBuilder.start();
                System.out.println(serverHandler.id+": starting program " + programName + " on input " + input + " :" + program.pid());
                Scanner scanner = new Scanner(program.getInputStream());
                PrintStream printStream = new PrintStream(program.getOutputStream());
                printStream.println(input);
                printStream.flush();
                int output = scanner.nextInt();
                response = String.valueOf(output);
            }
        }
        sendResponse(printWriter, response);
    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public ConnectionHandler setBufferedReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
        return this;
    }

    public PrintWriter getPrintWriter() {
        return printWriter;
    }

    public ConnectionHandler setPrintWriter(PrintWriter printWriter) {
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
        //  System.out.println("Sending Response to main server:[" + response + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        printWriter.println(response);

    }
}





