package os.hw1.master;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static os.hw1.master.Master.*;


public class Master {

    public static int port = 11893;
    public static int workerCount;
    public static int w;
    public static int commonArgsLength;
    public static int programsLength;
    public static String[] commonArgs;
    public static String[] programs;
    public static LinkedList<WorkerCard> workers = new LinkedList<>();
    public static LinkedList<Socket> clients;
    public static Process cacheProcess;
    public static Socket cachesocket;
    public static int cacheServerPort = 1024;
    private static volatile LinkedList<Chain> chainQueue = new LinkedList<>();
    public static LinkedList<Chain> processingList = new LinkedList<>();
    public static Runnable runnable;

    synchronized public static LinkedList<Chain> getChainQueue() {
        return chainQueue;
    }

    synchronized public static LinkedList<Chain> getProcessingList() {
        return processingList;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        runnable = new Runnable() {
            @Override
            public void run() {
                chainQueue = new LinkedList<>();
                for (int i = 0; i < workerCount; i++) {
                    WorkerCard workerCard = null;
                    try {
                        workerCard = new WorkerCard(i, w);
                    } catch (IOException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    workers.add(i, workerCard);
                }
                //Thread.sleep(4000);

                //System.out.println("waiting");
                while (true) {

                    if (!getChainQueue().isEmpty()) {
                      if(getChainQueue().size()>=2)  getChainQueue().sort(Comparator.comparing(Chain::getLocalDateTime).reversed());
                        //  System.out.println("has jobs in queue");
//                        Iterator<Chain> iterator = chainQueue.iterator();
//                        while ( iterator.hasNext()) {
//                            Chain chain = iterator.next();
//                            if (isProgramInProcess(chain.getInput(), chain.getLast())) continue;
//                            Integer output = getCache(chain.getInput(), chain.getChain()[chain.getChain().length - 1]);
//                            if (output != null) {
//                                //   System.out.println("found in cache");
//                                chain.setInput(output);
//                                chain.setChain(Arrays.copyOfRange(chain.getChain(), 0, chain.getChain().length - 1));
//                                // System.out.println(chain);
//                                if (chain.getChain().length == 0) {
//                                    chain.clientHandler.sendResponse(output);
//                                    chainQueue.remove(chain);
//                                }
//                            }
//
//                        }

                        //if (chainQueue.isEmpty()) continue;
                        Chain chain = getChainQueue().getLast();
                        if (isProgramInProcess(chain.getInput(), chain.getLast())) continue;
                        Integer output = getCache(chain.getInput(), chain.getChain()[chain.getChain().length - 1]);
                        if (output != null) {
                            //   System.out.println("found in cache");
                            chain.setInput(output);
                            chain.setChain(Arrays.copyOfRange(chain.getChain(), 0, chain.getChain().length - 1));
                            // System.out.println(chain);
                            if (chain.getChain().length == 0) {
                                try {
                                    chain.clientHandler.sendResponse(output);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                getChainQueue().remove(chain);
                            }
                        }else {
                            WorkerCard workerCard = workerChooser(getChainQueue().getLast().getLastProgramWeight());

                            if (workerCard != null && !isProgramInProcess(getChainQueue().getLast().getInput(), getChainQueue().getLast().getLast())) {
                                //System.out.println(workerCard.getId());

                                chain = getChainQueue().remove();
                                getProcessingList().add(chain);
                                Chain finalChain = chain;
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        workerCard.takeJob(finalChain);
                                    }
                                }).start();
                            }

                        }
                    }
                }
            }
        };

        // System.out.println("Master started");
        initialize();
        //  insertCache(10,2,20);
        //  System.out.println(getCache(10,2));
//        Thread.sleep(100);
        Server.port = port;
        Server server = new Server();
       new Thread(server).start();
//        Chain chain = new Chain();
//        chain.setInput(10);
//        chain.setChain(new int[]{1});
//        chainQueue.add(chain);
//        Chain chain1 = new Chain();
//        chain1.setInput(11);
//        chain1.setChain(new int[]{1, 2});
//        chainQueue.add(chain1);

        //   System.exit(0);

    }

    public static WorkerCard workerChooser(int weight) {
        workers.sort(Comparator.comparing(WorkerCard::getBusyWeight));
        for (WorkerCard workerCard : workers) {
            if (workerCard.getBusyWeight() + weight <= w) {
                return workerCard;
            }
        }
        return null;

    }

    public static boolean isProgramInProcess(int input, int program) {
        for (Chain chain : getProcessingList()) {
            if (chain.getInput() == input && chain.getLast() == program) {
         //       System.out.println("Process ["+input+":"+program+"] is being processed.wait until it is done");
                return true;
            }
        }
        return false;
    }

    public static void initialize() {
        Scanner scanner = new Scanner(System.in);
        port = scanner.nextInt();
        //   System.out.println(port);
        workerCount = scanner.nextInt();
        //    System.out.println(workerCount);
        w = scanner.nextInt();
        //   System.out.println(w);
        commonArgsLength = scanner.nextInt();
        scanner.nextLine();
        //   System.out.println(commonArgsLength);
        commonArgs = new String[commonArgsLength];
        for (int i = 0; i < commonArgsLength; i++) {
            commonArgs[i] = scanner.nextLine();
            //     System.out.println(commonArgs[i]);
        }
        programsLength = scanner.nextInt();
        //  System.out.println(programsLength);
        scanner.nextLine();
        programs = new String[programsLength];
        for (int i = 0; i < programsLength; i++) {
            programs[i] = scanner.nextLine();
            //    System.out.println(programs[i]);
        }
        chainQueue = new LinkedList<>();
        PrintStream printStream;
        ProcessBuilder processBuilder = new ProcessBuilder(
                commonArgs[0], commonArgs[1], commonArgs[2], "os.hw1.master.CacheServer");
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    //    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        try {
            cacheProcess = processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        printStream = new PrintStream(cacheProcess.getOutputStream());
        printStream.flush();
          Scanner scanner2 = new Scanner(cacheProcess.getInputStream());
          cacheServerPort = scanner2.nextInt();
        try {
            cachesocket = new Socket(InetAddress.getLocalHost(), cacheServerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(ProcessHandle.current().pid() + ": CacheServer start " + cacheProcess.pid() + " " + cacheServerPort);
        new Thread(() -> {
            while (true) {
                if (scanner.hasNext()) {
                    String message = scanner2.nextLine();
                    if (message != null && !message.equals(" ")) System.out.println(message);
                }
            }
        }).start();
        new Thread(() -> {

            try {
                cacheProcess.waitFor();
                System.out.println(ProcessHandle.current().pid() + ": CacheServer stop " + cacheProcess.pid() + " " + port);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(Master.runnable).start();
    }

    private static Integer getCache(int input, int program) {
        String request = "get " + input + " " + program;
        try {
            Scanner scanner = new Scanner(cachesocket.getInputStream());
            PrintStream printStream = new PrintStream(cachesocket.getOutputStream());
            printStream.println(request);
            String response = scanner.nextLine();
            //  cachesocket.close();
            if (response.equals("Not Found")) return null;
            else return Integer.parseInt(response);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void insertCache(int input, int program, int output) {
        String request = "insert " + input + " " + program + " " + output;
        try {

            Scanner scanner = new Scanner(cachesocket.getInputStream());
            PrintStream printStream = new PrintStream(cachesocket.getOutputStream());
            printStream.println(request);
            String response = scanner.nextLine();
           // System.out.println("inserted " + input + ":" + program + ":" + output);
            //   cachesocket.close();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}

class WorkerCard {
    private int id;
    private long pid;
    private Process process;

    private int port;
    private int w;
    private int busyWeight;
    PrintStream printStream;

    public WorkerCard(int id, int w) throws IOException, ExecutionException, InterruptedException {
        this.id = id;
        this.w = w;
        ProcessBuilder processBuilder = new ProcessBuilder(commonArgs[0], commonArgs[1], commonArgs[2], "os.hw1.master.Worker");
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        process = processBuilder.start();

        printStream = new PrintStream(process.getOutputStream());
        printStream.flush();
        Scanner scanner = new Scanner(process.getInputStream());
        port = scanner.nextInt();
//        new Thread(() -> {
//            while (true) {
//                if (scanner.hasNext()) {
//                    String message = scanner.nextLine();
//                    if (message != null && !message.equals(" ")) System.out.println(message);
//                }
//            }
//        }).start();

        System.out.println(ProcessHandle.current().pid() + ": worker " + id + " start " + process.pid() + " " + port);
//       CompletableFuture completableFuture=process.toHandle().onExit();
//       completableFuture.thenAccept(p1->{});
        transmitter("id " + id);
        String commonArgstemp = "commonArgs " + commonArgsLength + " " + Arrays.stream(commonArgs).collect(Collectors.joining(" "));
        //  System.out.println(commonArgstemp);
        transmitter(commonArgstemp);
new Thread(new Runnable() {
    @Override
    public void run() {

        CompletableFuture<ProcessHandle> completableFuture=process.toHandle().onExit();
        try {
            completableFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        completableFuture.thenAccept(p1 -> {
            try {
                workers.set(id,new WorkerCard(id, w));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // System.out.println(ProcessHandle.current().pid() + ": worker " + id + " stop " + process.pid() + " " + port);
        });
    }
}).start();

//



    }
    public int getBusyWeight() {
        return busyWeight;
    }

    public int getId() {
        return id;
    }

    public boolean takeJob(Chain chain) {
        int program = chain.getLast();
        int input = chain.getInput();
        String programName = programs[program - 1].split(" ")[0];
        int weight = Integer.parseInt(programs[program - 1].split(" ")[1]);
        if (busyWeight + weight <= w) {
            busyWeight+=weight;
            sendJob(programName, input, weight, chain);
            return true;
        }
        return false;
    }

    public void sendJob(String programName, int input, int weight, Chain chain) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    // System.out.println(chain);
                    String response = transmitter("job " + programName + " " + input);
                    int output = Integer.parseInt(response);
                    Master.insertCache(input, chain.getChain()[chain.getChain().length - 1], output);
                    busyWeight -= weight;
                    chain.setInput(output);
                    chain.setChain(Arrays.copyOfRange(chain.getChain(), 0, chain.getChain().length - 1));
                    //  System.out.println(chain);
                    processingList.remove(chain);
                    if (chain.getChain().length != 0) {
                        getChainQueue().add(chain);
                    } else {
                           chain.clientHandler.sendResponse(output);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        new Thread(runnable).start();
    }
    public String transmitter(String request) throws IOException {
       Socket socket = new Socket(InetAddress.getLocalHost(), port);
     //   System.out.println(socket.getPort());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
        if (request != null) {
            if (printWriter == null) {

                System.out.println((ProcessHandle.current().pid() + ": Error:No server (print writer is null)"));
                return "No Server";
            }
            LocalDateTime sentTime = LocalDateTime.now();
          //  System.out.println(ProcessHandle.current().pid() + ": Sending request: [" + request + "] to worker " + id + " at " + sentTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            printWriter.println(request);
         //   printWriter.close();
            if (bufferedReader == null) {
                System.out.println(ProcessHandle.current().pid() + ": Error:No server (buffer reader is null)");
                return "No Server";
            }

        }
        String response = null;

        try {
            response = bufferedReader.readLine();
            LocalDateTime responseTime = LocalDateTime.now();
       //     System.out.println(ProcessHandle.current().pid() + ": getting response: [" + response + "] from worker " + id + " at " + responseTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            if (response == null || response.equals("")) {

                System.out.println(ProcessHandle.current().pid() + ": Error: Response is null or empty");
                return "Error empty";
            }
        } catch (IOException e) {
            System.out.println(ProcessHandle.current().pid() + ": Couldn't get response from server");

        }
        bufferedReader.close();
    socket.close();
        return response;


    }
}

class Server implements Runnable {
    public static int port;
    public static ServerSocket serverSocket;


    @Override
    public void run() {
        try {

            serverSocket = new ServerSocket(port);
            port = serverSocket.getLocalPort();
          //  System.out.println(ProcessHandle.current().pid() + ":Master Server running on: " + serverSocket.getInetAddress().getHostAddress() + ": " + port);
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                //create a new connection
                ClientHandler clientHandler = new ClientHandler(socket);
                clientHandler.start();
            //    System.out.println(ProcessHandle.current().pid() + ":client connected as socket " + socket + " in ClientSideHandler " + clientHandler.getName());
            }

        } catch (Exception e) {

            System.out.println("Something went wrong while creating the serversocket");

        } finally {
            try {
                if (serverSocket != null & !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {

                System.out.println("Something went wrong while closing the serversocket");

            }
        }

    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        Server.port = port;
    }

}

class ClientHandler extends Thread implements RequestHandler {

    private Socket socket;

    private
    InputStream inputStream;
    private
    OutputStream outputStream;
    private
    BufferedReader bufferedReader;
    private PrintWriter printWriter;
    int[] chain;
    int input;


    private static Lock lock = new ReentrantLock();

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }


    @Override
    public void run() {
      //  System.out.println("Starting thread of " + this.getId());
        try {
            inputStream = socket.getInputStream();
        } catch (IOException ioException) {
            System.out.println("Error in input stream of" + this.getId());
        }
        try {
            outputStream = socket.getOutputStream();
        } catch (IOException ioException) {
            System.out.println("Error in out stream of" + this.getId());
        }
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        printWriter = new PrintWriter(outputStream, true);

        while (true) {
            String request = getRequest(bufferedReader);

            if (request == null) {
               // System.out.println(this.getName() + " disconnected");
                return;

            }
           // System.out.println("Getting Request from client " + this.getId() + " :[" + request + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            handleRequest(request);
        }
    }


    public String getRequest(BufferedReader bufferedReader) {
        String request = null;
        try {
            request = bufferedReader.readLine();
        } catch (IOException e) {
            System.out.println("Error in getting request.");
        }
        return request;
    }

    public Socket getSocket() {
        return socket;
    }

    public void sendResponse(int response) throws IOException {
   //     System.out.println("Sending Response to client:[" + response + "] at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        printWriter.println(response);
        printWriter.println("\n");
     //   socket.close();

    }

    @Override
    public void handleRequest(String request) {
        LocalDateTime localDateTime = LocalDateTime.now();
        int response = 0;
        String[] temp = request.split(" ");
        input = Integer.parseInt(temp[1]);
        this.chain = Arrays.stream(temp[0].split("\\|")).mapToInt(Integer::parseInt).toArray();
        Chain chain = new Chain();
        chain.setChain(this.chain);
        chain.setInput(input);
        chain.clientHandler = this;
        chain.localDateTime = localDateTime;
        getChainQueue().add(chain);
        // System.out.println(chainQueue.size());
      //  System.out.println("chain added");
    }
}

class Chain {
    private int[] chain;
    private int input;
    ClientHandler clientHandler;
    LocalDateTime localDateTime;

    public int getLast() {
        if (chain.length != 0) return chain[chain.length - 1];
        return 0;
    }

    public Chain() {
        localDateTime = LocalDateTime.now();
    }

    synchronized public int getLastProgramWeight() {
        return Integer.parseInt(programs[chain[chain.length - 1] - 1].split(" ")[1]);
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    synchronized public int[] getChain() {
        return chain;
    }

    synchronized public void setChain(int[] chain) {
        this.chain = chain;
    }

    synchronized public int getInput() {
        return input;
    }

    synchronized public void setInput(int input) {
        this.input = input;
    }

    @Override
    public String toString() {
        return "Chain{" +
                "chain=" + Arrays.toString(chain) +
                ", input=" + input +
                '}';
    }
}
