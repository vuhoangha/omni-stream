package io.github.vuhoangha.Example;

import io.github.vuhoangha.Example.structure_example.AnimalTest;
import io.github.vuhoangha.ManyToOneStateless.Anubis;
import io.github.vuhoangha.ManyToOneStateless.AnubisConfig;
import io.github.vuhoangha.ManyToOneStateless.Saraswati;
import io.github.vuhoangha.ManyToOneStateless.SaraswatiConfig;
import io.github.vuhoangha.common.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class SaraswatiAnubisExample {


    //region NORMAL

    public static void runNormal() {
        new Thread(SaraswatiAnubisExample::runSaraswatiNormal).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(() -> SaraswatiAnubisExample.runAnubisNormal("anubis_queue_temp")).start();
    }

    public static void runSaraswatiNormal() {
        Saraswati saraswati = new Saraswati(
                SaraswatiConfig.standard(),
                bytes -> {
                    System.out.println("Saraswati: " + new AnimalTest(bytes));
                }
        );
    }

    public static void runAnubisNormal(String queuePath) {
        Anubis anubis = new Anubis(
                AnubisConfig.getDefault().setSaraswatiIP("127.0.0.1").setQueueTempPath(queuePath)
        );

        int count = 1;
        while (true) {
            AnimalTest animal = new AnimalTest(
                    count, // index
                    count * 10L, // age
                    count * 10L, // weight
                    count * 10L, // height
                    count * 20L, // speed
                    count * 20L, // energy
                    count * 20L, // strength
                    count * 30L, // agility
                    count * 30L, // intelligence
                    count * 30L, // lifespan
                    count * 100L, // offspring
                    count * 100L  // territorySize
            );
            count++;

            boolean sendSuccess = anubis.sendMessage(animal);
            log.info("Anubis send {} result: {}", animal.getIndex(), sendSuccess);

            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    //endregion


    //region BENCHMARK LATENCY

    public static void runBenchmark() {
        new Thread(SaraswatiAnubisExample::runSaraswatiBenchmark).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(() -> SaraswatiAnubisExample.runAnubisBenchmark("anubis_queue_temp_1")).start();
//        new Thread(() -> SaraswatiAnubisExample.runAnubisBenchmark("anubis_queue_temp_2")).start();
    }

    public static void runSaraswatiBenchmark() {

        AtomicInteger count = new AtomicInteger(0);
        new Thread(() -> {
            while (true) {
                System.out.println("Rate " + count.get());
                count.set(0);
                LockSupport.parkNanos(1_000_000_000L);
            }
        }).start();

        Saraswati saraswati = new Saraswati(
                SaraswatiConfig.standard(),
                bytes -> {
                    count.incrementAndGet();
                }
        );
    }

    public static void runAnubisBenchmark(String queuePath) {
        Anubis anubis = new Anubis(
                AnubisConfig.bestPerf().setSaraswatiIP("127.0.0.1").setQueueTempPath(queuePath)
        );

        int count = 1;
        AnimalTest animal = new AnimalTest(
                count, // index
                count * 10L, // age
                count * 10L, // weight
                count * 10L, // height
                count * 20L, // speed
                count * 20L, // energy
                count * 20L, // strength
                count * 30L, // agility
                count * 30L, // intelligence
                count * 30L, // lifespan
                count * 100L, // offspring
                count * 100L  // territorySize
        );

        LockSupport.parkNanos(5_000_000_000L);

        for (int i = 0; i < 100_000_000; i++) {
            for (int j = 0; j < 100_000; j++) {
                anubis.sendMessageAsync(animal, new Promise<>());
            }

            LockSupport.parkNanos(50_000_000L);
        }

    }

    //endregion


}
