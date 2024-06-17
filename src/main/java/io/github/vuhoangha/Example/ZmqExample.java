package io.github.vuhoangha.Example;

import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.Example.structure_example.AnimalTest;
import net.openhft.chronicle.bytes.Bytes;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class ZmqExample {

    public static void run() {
        new Thread(ZmqExample::runRouter).start();
        LockSupport.parkNanos(1_000_000_000L);
        new Thread(ZmqExample::runDealer).start();
        new Thread(ZmqExample::runDealer).start();
        new Thread(ZmqExample::runDealer).start();
        new Thread(ZmqExample::runDealer).start();
        new Thread(ZmqExample::runDealer).start();
        new Thread(ZmqExample::runDealer).start();
    }

    public static void runRouter() {

        AtomicInteger count = new AtomicInteger(0);
        new Thread(() -> {
            while (true) {
                System.out.println("Rate " + count.get());
                count.set(0);
                LockSupport.parkNanos(1_000_000_000L);
            }
        }).start();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.setSndHWM(1000000);
            socket.setHeartbeatIvl(10000);
            socket.setHeartbeatTtl(15000);
            socket.setHeartbeatTimeout(15000);
            socket.bind("tcp://*:3003");

            while (true) {
                byte[] clientAddress = socket.recv(0);
                byte[] request = socket.recv(0);

                socket.send(clientAddress, ZMQ.SNDMORE);
                socket.send(Utils.longToBytes(System.currentTimeMillis()), 0);

                count.incrementAndGet();
            }
        }
    }


    public static void runDealer() {

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
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        animal.writeMarshallable(bytes);
        byte[] aaa = bytes.toByteArray();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            socket.setRcvHWM(1000000);
            socket.setHeartbeatIvl(30000);
            socket.setHeartbeatTtl(45000);
            socket.setHeartbeatTimeout(45000);
            socket.setReconnectIVL(10000);
            socket.setReconnectIVLMax(10000);
            socket.connect("tcp://127.0.0.1:3003");

            for (int i = 0; i < 100_000; i++) {
                for (int j = 0; j < 500_000; j++) {
                    socket.send(aaa, 0);
                }

                LockSupport.parkNanos(100_000_000L);
            }
        }
    }

}
