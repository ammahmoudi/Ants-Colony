package os.hw1.programs;

import java.util.Scanner;

import static os.hw1.Tester.WAIT_P1;

public class Program1 {
    public static void main(String[] args) throws InterruptedException {

        Scanner scanner = new Scanner(System.in);
        Thread.sleep(WAIT_P1 - 50);
       // Thread.sleep(30000);
        System.out.println(scanner.nextInt() - 1);
    }
}
