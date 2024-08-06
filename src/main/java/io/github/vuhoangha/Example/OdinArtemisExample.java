package io.github.vuhoangha.Example;

import io.github.vuhoangha.Example.structure_example.CarTest;
import io.github.vuhoangha.OneToManyStateless.*;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class OdinArtemisExample {

    public static void run() {
        new Thread(OdinArtemisExample::runArtemis).start();
        LockSupport.parkNanos(2_000_000_000L);
        new Thread(OdinArtemisExample::runOdin).start();
    }

    public static void runOdin() {
        Odin odin = new Odin(
                OdinConfig.standardConfig()
        );

        LockSupport.parkNanos(2_000_000_000L);

        int count = 0;
        while (true) {
            count++;
            CarTest car = new CarTest(count, count + 1000);
            System.out.println("\n\uD83D\uDE80Send: " + car);
            odin.sendMessage(car);
            LockSupport.parkNanos(1_000_000_000L);
        }
    }


    public static void runArtemis() {
        CarTest car = new CarTest();
        ArtemisHandler onData = (long seq, Bytes<ByteBuffer> data) -> {
            System.out.println("\uD83D\uDCE9Received");
            System.out.println("Seq: " + seq);
            CarTest.reader(car, data);
            System.out.println("Car: " + car.toString());
        };
        Consumer<String> onInterrupt = (String reason) -> {
            System.out.println("\uD83D\uDCE9Interrupt: " + reason);
        };

        Artemis artemis = new Artemis(
                ArtemisConfig.standardConfig().setSourceIP("127.0.0.1"),
                onData,
                onInterrupt
        );
        LockSupport.parkNanos(5_000_000_000L);
        artemis.startRealtimeData();
    }

}
