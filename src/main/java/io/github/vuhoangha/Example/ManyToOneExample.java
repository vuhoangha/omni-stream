package io.github.vuhoangha.Example;

import io.github.vuhoangha.ManyToOne.Collector;
import io.github.vuhoangha.ManyToOne.CollectorCfg;
import io.github.vuhoangha.ManyToOne.Snipper;
import io.github.vuhoangha.ManyToOne.SnipperCfg;

import java.util.concurrent.locks.LockSupport;

public class ManyToOneExample {

    public static String collectorPath = "xxx";

    public static void run() {
        new Thread(ManyToOneExample::runCollector).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(() -> ManyToOneExample.runSnipper(5557, 1)).start();
        LockSupport.parkNanos(500_000_000L);
        new Thread(() -> ManyToOneExample.runSnipper(5557, 100000)).start();
    }


    public static void runCollector() {
        try {
            new Collector<>(
                    CollectorCfg.builder()
                            .setQueuePath(collectorPath)
                            .setReaderName("reader_name"),
                    PeopleTest.class,
                    (people, index) -> {
                        System.out.println("\uD83D\uDCE9Received");
                        System.out.println("index: " + index);
                        System.out.println("people: " + people);
                    }
            );
        } catch (Exception ex) {
        }
    }


    public static void runSnipper(int port, int startIndex) {
        try {
            Snipper<PeopleTest> snipper = new Snipper<>(SnipperCfg.builder().setCollectorIP("localhost").setPort(port));
            int count = startIndex;
            while (true) {
                PeopleTest people = new PeopleTest(count, "people " + count);
                System.out.println("\n\uD83D\uDE80Send: " + people);
                snipper.send(people);

                count++;
                LockSupport.parkNanos(1_000_000_000);
            }
        } catch (Exception ex) {
        }
    }


}
