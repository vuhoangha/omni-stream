package io.github.vuhoangha.Example;

import io.github.vuhoangha.Example.structure_example.AnimalTest;
import io.github.vuhoangha.ManyToOneStateless.Anubis;
import io.github.vuhoangha.ManyToOneStateless.AnubisCfg;
import io.github.vuhoangha.ManyToOneStateless.Saraswati;
import io.github.vuhoangha.ManyToOneStateless.SaraswatiCfg;
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
        new Thread(SaraswatiAnubisExample::runAnubisNormal).start();
    }

    public static void runSaraswatiNormal() {
        Saraswati saraswati = new Saraswati(
                SaraswatiCfg.getDefault(),
                bytes -> {
                    System.out.println("Saraswati: " + new AnimalTest(bytes));
                }
        );
    }

    public static void runAnubisNormal() {
        Anubis anubis = new Anubis(
                AnubisCfg.getDefault().setSaraswatiIP("127.0.0.1")
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

            boolean sendSuccess = anubis.send(animal);
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
                SaraswatiCfg.getDefault(),
                bytes -> {
                    count.incrementAndGet();
                }
        );
    }

    public static void runAnubisBenchmark() {
        Anubis anubis = new Anubis(
                AnubisCfg.getDefault().setSaraswatiIP("127.0.0.1")
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
            for (int j = 0; j < 80_000; j++) {
                anubis.sendAsync(animal, new Promise<>());
            }

            LockSupport.parkNanos(50_000_000L);
        }

    }

    //endregion


}
