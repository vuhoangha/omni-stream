package io.github.vuhoangha.Example;

import io.github.vuhoangha.Common.SinkinHandler;
import io.github.vuhoangha.Example.structure_example.AnimalTest;
import io.github.vuhoangha.OneToMany.*;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

public class OneToManyExample {


    public static String sourcePath = "fanout_queue";
    public static String sinkPath = "sinkin_queue";


    public static void run() {
        new Thread(OneToManyExample::runSource).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(OneToManyExample::runSink).start();
    }


    public static void runSink() {
        SinkinHandler handler = (long localIndex, long sequence, Bytes<ByteBuffer> data) -> {
            System.out.println("\uD83D\uDCE9Received");
            System.out.println("LocalIndex: " + localIndex);
            System.out.println("Sequence: " + sequence);

            AnimalTest animalTest = new AnimalTest(data);
            System.out.println("Animal: " + animalTest.toString());
        };

        new Sinkin(SinkinCfg.builder()
                .setQueuePath(sinkPath)
                .setSourceIP("127.0.0.1")
                .setReaderName("onus_spot_market_core")
                .setStartId(2733660784558086L), handler);
    }


    public static void runSource() {
        Fanout fanout = new Fanout(FanoutCfg.defaultCfg().setQueuePath(sourcePath));

        int count = 0;
        while (true) {
            count++;

            AnimalTest animal = new AnimalTest(
                    count, // index
                    count * 10L, // age
                    count * 20L, // weight
                    count * 30L, // height
                    count * 40L, // speed
                    count * 50L, // energy
                    count * 60L, // strength
                    count * 70L, // agility
                    count * 80L, // intelligence
                    count * 90L, // lifespan
                    count * 100L, // offspring
                    count * 110L  // territorySize
            );

            System.out.println("\n\uD83D\uDE80Send: " + animal);
            fanout.write(animal);

            LockSupport.parkNanos(1_000_000_000L);
        }
    }

}
