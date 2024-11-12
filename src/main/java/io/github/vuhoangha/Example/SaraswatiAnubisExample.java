package io.github.vuhoangha.Example;

import io.github.vuhoangha.Example.structure_example.AnimalTest;
import io.github.vuhoangha.Example.structure_example.CarTest;
import io.github.vuhoangha.Example.structure_example.CombineClass;
import io.github.vuhoangha.ManyToOneStateless.Anubis;
import io.github.vuhoangha.ManyToOneStateless.AnubisConfig;
import io.github.vuhoangha.ManyToOneStateless.Saraswati;
import io.github.vuhoangha.ManyToOneStateless.SaraswatiConfig;
import io.github.vuhoangha.common.Promise;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class SaraswatiAnubisExample {


    //region BATCHING

    public static void runBatching() {
        new Thread(SaraswatiAnubisExample::runSaraswatiBatching).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(() -> {
            try {
                SaraswatiAnubisExample.runAnubisBatching();
            } catch (Exception e) {
            }
        }).start();
    }


    public static void runSaraswatiBatching() {
        Saraswati saraswati = new Saraswati(
                SaraswatiConfig.standardConfig(),
                bytes -> {
                    System.out.println("\uD83D\uDCE9Received");

                    int size = bytes.readInt();
                    for (int i = 0; i < size; i++) {
                        byte type = bytes.readByte();
                        if (type == 1) {
                            System.out.println("AnimalTest: " + new AnimalTest(bytes));
                        } else if (type == 2) {
                            System.out.println("CarTest: " + new CarTest(bytes));
                        }
                    }
                }
        );
    }


    public static void runAnubisBatching() throws Exception {
        Anubis anubis = new Anubis(
                AnubisConfig.standardConfig().setSaraswatiIP("127.0.0.1")
        );

        LockSupport.parkNanos(1_000_000_000L);

        int count = 1;
        while (true) {
            CombineClass msg1 = new CombineClass();
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
            msg1.type = 1;
            msg1.animalTest = animal;

            CombineClass msg2 = new CombineClass();
            msg2.type = 2;
            msg2.carTest = new CarTest(
                    count,
                    count * 10
            );

            Promise<Boolean> callback = new Promise<>();
            anubis.sendListMessageAsync(Arrays.asList(msg1, msg2), callback);
            boolean isSuccess = callback.get(10);
            log.info("Anubis send {} result: {}", count, isSuccess);

            count++;
            LockSupport.parkNanos(1_000_000_000L);
        }
    }
    //endregion


    //region NORMAL

    public static void runNormal() {
        new Thread(SaraswatiAnubisExample::runSaraswatiNormal).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(SaraswatiAnubisExample::runAnubisNormal).start();
    }

    public static void runSaraswatiNormal() {
        Saraswati saraswati = new Saraswati(
                SaraswatiConfig.standardConfig(),
                bytes -> {
                    System.out.println("Saraswati: " + new AnimalTest(bytes));
                }
        );
    }

    public static void runAnubisNormal() {
        Anubis anubis = new Anubis(
                AnubisConfig.standardConfig().setSaraswatiIP("127.0.0.1")
        );

        LockSupport.parkNanos(1_000_000_000L);

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
        new Thread(SaraswatiAnubisExample::runAnubisBenchmark).start();
//        new Thread(() -> SaraswatiAnubisExample.runAnubisBenchmark("anubis_queue_temp_2")).start();
    }

    public static void runSaraswatiBenchmark() {

        AtomicInteger count = new AtomicInteger(0);
        new Thread(() -> {
            while (true) {
                System.out.println("Rate " + count.getAndSet(0));
                LockSupport.parkNanos(1_000_000_000L);
            }
        }).start();

        Saraswati saraswati = new Saraswati(
                SaraswatiConfig.bestPerformanceConfig(),
                bytes -> {
                    count.incrementAndGet();
                }
        );
    }

    public static void runAnubisBenchmark() {

        Anubis anubis = new Anubis(
                AnubisConfig.bestPerformanceConfig().setSaraswatiIP("127.0.0.1")
        );

        new Thread(() -> {
            AffinityLock al = AffinityLock.acquireLock(-1);

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

            AtomicInteger count1 = new AtomicInteger(0);
            new Thread(() -> {
                while (true) {
                    System.out.println("Rate send " + count1.getAndSet(0));
                    LockSupport.parkNanos(1_000_000_000L);
                }
            }).start();

            for (int i = 0; i < 100_000_000; i++) {
                for (int j = 0; j < 100_000; j++) {
                    count1.getAndIncrement();
                    anubis.sendMessageAsync(animal, new Promise<>());
                }

                LockSupport.parkNanos(50_000_000L);
            }

        }).start();
    }

    //endregion


}
