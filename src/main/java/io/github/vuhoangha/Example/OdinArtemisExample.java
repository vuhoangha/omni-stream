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
        Odin odin = new Odin<>(
                OdinCfg.getDefault(),
                PeopleTest.class
        );

        int count = 0;
        while (true) {
            count++;
            PeopleTest people = new PeopleTest(count, "people " + count);
            System.out.println("\n\uD83D\uDE80Send: " + people);
            odin.send(people);
            LockSupport.parkNanos(100_000_000L);
        }
    }

    public static void runArtemis() {
        ArtemisHandler<PeopleTest> onData = (long version, long seq, PeopleTest data) -> {
            System.out.println("\uD83D\uDCE9Received");
            System.out.println("Version: " + version);
            System.out.println("People: " + data.toString());
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
                PeopleTest.class,
                onData,
                onInterrupt,
                onWarning
        );
    }

}
