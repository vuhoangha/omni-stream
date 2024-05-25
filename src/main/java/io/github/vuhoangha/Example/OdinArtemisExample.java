package io.github.vuhoangha.Example;

import io.github.vuhoangha.OneToManyStateless.*;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class OdinArtemisExample {

    public static void run() {
        new Thread(OdinArtemisExample::runArtemis).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(OdinArtemisExample::runOdin).start();
    }

    public static void runOdin() {
        Odin<CarTest> odin = new Odin<>(
                OdinCfg.getDefault(),
                CarTest.class,
                CarTest::clone
        );

        int count = 0;
        while (true) {
            count++;
            CarTest car = new CarTest(count, count + 1000);
            System.out.println("\n\uD83D\uDE80Send: " + car);
            odin.send(car);
            LockSupport.parkNanos(100_000_000L);
        }
    }

    public static void runArtemis() {
        ArtemisHandler<CarTest> onData = (long version, long seq, CarTest data) -> {
            System.out.println("\uD83D\uDCE9Received");
            System.out.println("Version: " + version);
            System.out.println("Car: " + data.toString());
            System.out.println("Seq: " + seq);
        };
        Consumer<String> onInterrupt = (String reason) -> {
            System.out.println("\uD83D\uDCE9Interrupt: " + reason);
        };
        Consumer<String> onWarning = (String reason) -> {
            System.out.println("\uD83D\uDCE9Warning: " + reason);
        };

        new Artemis<>(
                ArtemisCfg.getDefault().setSourceIP("127.0.0.1"),
                CarTest.class,
                CarTest::reader,
                onData,
                onInterrupt,
                onWarning
        );
    }

}
