package io.github.vuhoangha.Example;

import io.github.vuhoangha.ManyToOne.Collector;
import io.github.vuhoangha.ManyToOne.CollectorCfg;
import io.github.vuhoangha.ManyToOne.Snipper;
import io.github.vuhoangha.ManyToOne.SnipperCfg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class ManyToOneExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManyToOneExample.class);

    public static String collectorPath = "/Users/vuhoangha/Desktop/chronicle-queue-data/collector";
    public static int numberItem = 100000;

    public static void run(){
        new Thread(ManyToOneExample::runCollector).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(ManyToOneExample::runSnipper).start();
    }


    public static void runCollector() {
        try {
            AtomicLong start = new AtomicLong(0);
            AtomicInteger itemReceivedCount = new AtomicInteger(0);

            Collector<PeopleTest> collector = new Collector<>(
                    CollectorCfg.builder()
                            .setQueuePath(collectorPath)
                            .setReaderName("exchange-core-collector"),
                    PeopleTest.class,
                    (people, index) -> {
                        itemReceivedCount.incrementAndGet();
                        if (itemReceivedCount.get() % numberItem == 1)
                            start.set(System.currentTimeMillis());
                        if (itemReceivedCount.get() % numberItem == 0) {
                            long end = System.currentTimeMillis();
                            LOGGER.info("Run time: " + (end - start.get()));
                        }

//                        Logger.info("index: " + index);
//                        Logger.info("people: " + people.toString());
                    }
            );
        } catch (Exception ex) {
        }
    }


    public static void runSnipper() {
        try {
            Snipper<PeopleTest> snipper = new Snipper<>(SnipperCfg.builder().setCollectorIP("localhost").setPort(5557));

            for (int j = 0; j < 10; j++) {
                for (int i = 1; i <= numberItem; i++) {
//                System.out.println("");
//                Logger.info("Sending " + (i + 1));

                    snipper.send(new PeopleTest(i, "Kendrick " + i));

//                LockSupport.parkNanos(10_000_000);
//                Logger.info("Done " + (i + 1));
                }
                LockSupport.parkNanos(1_000_000_000);
            }
        } catch (Exception ex) {
        }
    }


}
