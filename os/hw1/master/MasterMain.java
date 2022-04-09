package os.hw1.master;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class MasterMain {
    public static int port;
    public static int workerCount;
    public static int w;
    public static int commonArgsLength;
    public static int programsLength;
    public static String[] commonArgs;
    public static String[] programs;
    public static Process mainProcess;

    public static void main(String[] args) throws InterruptedException, IOException {
        //  System.out.println("MasterMain started");
        initialize();
        ProcessBuilder processBuilder = new ProcessBuilder(
                commonArgs[0], commonArgs[1], commonArgs[2], "os.hw1.master.Master"
        );
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        mainProcess = processBuilder.start();
        System.out.println("master start " + mainProcess.pid() + " " + port);
        PrintStream printStream = new PrintStream(mainProcess.getOutputStream());
        printStream.println(port);
        printStream.println(workerCount);
        printStream.println(w);
        printStream.println(commonArgs.length);
        Arrays.stream(commonArgs).forEach(printStream::println);
        printStream.println(programs.length);
        Arrays.stream(programs).forEach(printStream::println);
        printStream.flush();

        new Thread(new Runnable() {
            @Override
            public void run() {
                CompletableFuture<ProcessHandle> completableFuture = mainProcess.toHandle().onExit();
                try {
                    completableFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                completableFuture.thenAccept(p1 -> {
                    List<ProcessHandle> list = mainProcess.descendants().toList();
                    for (ProcessHandle processHandle : list) {
                        processHandle.destroy();
                    }
                    System.out.println("master stop " + mainProcess.pid() + " " + port);
                });
            }
        }).start();

        //  Thread.sleep(8000);
        // mainProcess.destroy();

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

    }
}


