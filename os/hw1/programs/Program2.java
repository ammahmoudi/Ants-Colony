package os.hw1.programs;

import java.util.Scanner;

import static os.hw1.Tester.WAIT_P2;

public class Program2 {
    public static void main(String[] args) throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        //Thread.sleep(5000);
        Thread.sleep(WAIT_P2 - 50);
       // System.out.println(10);
       System.out.println((scanner.nextInt() / 2) % 5);
    }
}
