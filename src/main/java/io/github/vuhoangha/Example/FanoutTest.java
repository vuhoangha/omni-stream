package io.github.vuhoangha.Example;

import io.github.vuhoangha.OneToMany.Fanout;
import io.github.vuhoangha.OneToMany.FanoutCfg;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

public class FanoutTest {

    private final String sourcePath = "fanout_queue";


    public void run() {
        clear();

        Fanout fanout = new Fanout(FanoutCfg.defaultCfg().setQueuePath(sourcePath).setCompress(false));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 2_000_000; i++) {
//            ObjectMultiField obj = new ObjectMultiField();
//            Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
//            obj.writeMarshallable(bytes);
//            bytes.clear();

            AnimalTest animal = new AnimalTest(
                    i, // index
                    i * 10L, // age
                    i * 20L, // weight
                    i * 30L, // height
                    i * 40L, // speed
                    i * 50L, // energy
                    i * 60L, // strength
                    i * 70L, // agility
                    i * 80L, // intelligence
                    i * 90L, // lifespan
                    i * 100L, // offspring
                    i * 110L  // territorySize
            );
            fanout.write(animal);
        }
        long total = System.currentTimeMillis() - start;
        System.out.println("Fanout: " + total + " ms");

        LockSupport.parkNanos(3_000_000_000L);
        fanout.shutdown();

        clear();
    }


    public void clear() {
        File folder = new File(sourcePath);
        if (folder.exists() && folder.isDirectory()) {
            deleteFolder(folder);
        }
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file);
                } else {
                    file.delete();
                }
            }
        }
        folder.delete();
    }

}
