package io.github.vuhoangha.Example;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.SinkinHandler;
import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.Example.structure_example.AnimalTest;
import io.github.vuhoangha.OneToMany.*;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class OneToManyExample {


    public static void run() {
        new Thread(OneToManyExample::runSinkin).start();
        LockSupport.parkNanos(1_000_000_000L);
        new Thread(OneToManyExample::runFanout).start();
    }


    public static void fanoutBenchmark(int numberItem) {

        String sourcePath = "fanout_benchmark_folder";

        Utils.deleteFolder(sourcePath);

        Fanout fanout = new Fanout(FanoutConfig.defaultCfg().setQueuePath(sourcePath).setCompress(true));

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

        Utils.benchmark("Fanout benchmark", () -> {
            for (int i = 0; i < numberItem; i++) {
                fanout.write(animal);
            }
        });

        Utils.deleteFolder(sourcePath);
    }


    public static void generateExampleData(int numberItem, String path) {

        Utils.deleteFolder(path);

        Fanout fanout = new Fanout(FanoutConfig.defaultCfg().setQueuePath(path).setCompress(true));

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

        for (int i = 0; i < numberItem; i++) {
            fanout.write(animal);
        }

        fanout.shutdown();
    }


    public static void runSinkin() {

        String path = "sinkin_queue";

        Utils.deleteFolder(path);

        AtomicInteger count = new AtomicInteger(0);
        new Thread(() -> {
            while (true) {
                System.out.println("Rate " + count.get());
                count.set(0);
                LockSupport.parkNanos(1_000_000_000L);
            }
        }).start();

        SinkinHandler handler = (long localIndex, long currentSeq, long endSyncedSeq, Bytes<ByteBuffer> data) -> {
            count.incrementAndGet();

            System.out.println("\uD83D\uDCE9Received");
            System.out.println("LocalIndex: " + localIndex);
            System.out.println("currentSeq: " + currentSeq);
            System.out.println("endSyncedSeq: " + endSyncedSeq);

            AnimalTest animalTest = new AnimalTest(data);
            System.out.println("Animal: " + animalTest);
        };

        new Sinkin(SinkinConfig.builder()
                .setQueuePath(path)
                .setSourceIP("127.0.0.1")
                .setReaderName("onus_spot_market_core")
                .setStartId(-1L)
                .setSubMsgCpu(Constance.CPU_TYPE.ANY)
                .setDisruptorProcessMsgCpu(Constance.CPU_TYPE.ANY)
                .setCompress(true), handler);

    }


    public static void runFanout() {
        String sourcePath = "fanout_queue";

        Utils.deleteFolder(sourcePath);

        Fanout fanout = new Fanout(FanoutConfig.defaultCfg().setQueuePath(sourcePath).setCompress(true));

        LockSupport.parkNanos(5_000_000_000L);


        for (int i = 0; i < 100_000_000; i++) {
            for (int j = 0; j < 1; j++) {

                int count = i + 1;
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

                fanout.write(animal);
            }
            LockSupport.parkNanos(500_000_000L);
        }

        Utils.deleteFolder(sourcePath);
    }

}
