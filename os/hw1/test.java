package os.hw1;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class test {
    public static void main(String[] args) throws IOException {
        ServerHandler.connectToServer();
        ServerHandler.transmitter("hi");

    }
}

class ServerHandler {
    public static int SERVER_PORT = 10792;
    public  static String SERVER_IP="127.0.0.1";
    static DataOutputStream dataOut;
    private static Socket socket;
    private static DataInputStream dataIn;
    private static PrintWriter printWriter;
    private static BufferedReader bufferedReader;

    public static DataOutputStream getDataOut() {
        return dataOut;
    }

    public static void setDataOut(DataOutputStream dataOut) {
        ServerHandler.dataOut = dataOut;
    }

    public static Socket getSocket() {
        return socket;
    }
    public  static void   setServer(String ip,int port){
        SERVER_IP=ip;
        SERVER_PORT=port;
    }
    public static void setSocket(Socket socket1) {
        socket = socket1;

        try {
            if (socket1 != null) {
                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                printWriter = new PrintWriter(socket.getOutputStream(), true);
            } else {
                bufferedReader = null;
                printWriter = null;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static DataInputStream getDataIn() {
        return dataIn;
    }

    public static void setDataIn(DataInputStream dataIn) {
        ServerHandler.dataIn = dataIn;

    }



    public static String transmitter(String request) {

        if (request != null) {
            if (printWriter == null) {

               System.out.println("Error:No server (print writer is null)");
                return "No Server";
            }


            printWriter.println(request);
           System.out.println("Sending request: ["+request+"] at"+ LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            if (bufferedReader == null) {

               System.out.println("Error:No server (buffer reader is null)");
                return "No Server";
            }

        }
        String response = null;

        try {

            response = bufferedReader.readLine();
           System.out.println("getting response: ["+response+"] at "+ LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            if ( response == null ||response.equals("")) {

                disconnectFromServer();

               System.out.println("Error: Response is null or empty");
                return "Error empty";
            }
        } catch (IOException e) {

            System.out.println("Couldn't get response from server");
           System.out.println("Error: Couldn't get response from server.");
            disconnectFromServer();

        }



        return response;


    }



    public static void disconnectFromServer() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                setSocket(null);
               System.out.println("Disconnected from server.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String reconnectToServer() {

        if (socket == null) {
           System.out.println("Reconnecting to server.");
            connectToServer();

        }
       System.out.println("Already Connected to Server.");
        return "Already Connected";
    }

    public static boolean connectToServer() {
        try {

           System.out.println("Connecting to server "+SERVER_IP+" : "+SERVER_PORT);
            setSocket(new Socket(SERVER_IP, SERVER_PORT));
           System.out.println("Connected to Server.");
            return true;

        } catch (IOException e) {
           System.out.println("Error: Cannot connect to server.");
            return false;


        }
    }

    public static PrintWriter getPrintWriter() {
        return printWriter;
    }

    public static void setPrintWriter(PrintWriter printWriter) {
        ServerHandler.printWriter = printWriter;
    }

    public static BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public static void setBufferedReader(BufferedReader bufferedReader) {
        ServerHandler.bufferedReader = bufferedReader;
    }


}