package io.github.vuhoangha.Example;

import io.github.vuhoangha.Common.SinkinHandler;
import io.github.vuhoangha.OneToMany.*;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

public class OneToManyExample {


    public static String sourcePath = "xxx";
    public static String sinkPath = "zzz";


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
            System.out.println("People: " + data.toString());
        };

        new Sinkin(SinkinCfg.builder().setQueuePath(sinkPath).setSourceIP("127.0.0.1"), handler);
    }


    public static void runSource() {
//        Fanout<PeopleTest> fanout = new Fanout<>(
//                FanoutCfg.builder().setQueuePath(sourcePath),
//                PeopleTest.class);
//
//        PeopleTest people = new PeopleTest();
//        int count = 0;
//        while (true) {
//            count++;
//            people.setIndex(count);
//            people.setName("people " + count);
//            System.out.println("\n\uD83D\uDE80Send: " + people);
//            fanout.write(people);
//
//            LockSupport.parkNanos(2_000_000_000L);
//        }
    }

}
