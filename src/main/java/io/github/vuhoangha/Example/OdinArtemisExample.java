package io.github.vuhoangha.Example;

import io.github.vuhoangha.OneToManyStateless.*;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

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
                OdinCfg.getDefault()
        );

        int count = 0;
        Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();
        while (true) {
            count++;
            CarTest car = new CarTest(count, count + 1000);
            System.out.println("\n\uD83D\uDE80Send: " + car);
            data.clear();
            car.writeMarshallable(data);
            odin.send(data);
            LockSupport.parkNanos(100_000_000L);
        }
    }


    public static void runArtemis() {
        CarTest car = new CarTest();
        ArtemisHandler onData = (long version, long seq, Bytes<ByteBuffer> data) -> {
            System.out.println("\uD83D\uDCE9Received");
            System.out.println("Version: " + version);
            System.out.println("Seq: " + seq);
            CarTest.reader(car, data);
            System.out.println("Car: " + car.toString());
        };
        Consumer<String> onInterrupt = (String reason) -> {
            System.out.println("\uD83D\uDCE9Interrupt: " + reason);
        };
        Consumer<String> onWarning = (String reason) -> {
            System.out.println("\uD83D\uDCE9Warning: " + reason);
        };

        new Artemis(
                ArtemisCfg.getDefault().setSourceIP("127.0.0.1"),
                onData,
                onInterrupt,
                onWarning
        );
    }

}
