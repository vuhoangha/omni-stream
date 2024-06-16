package io.github.vuhoangha;

import io.github.vuhoangha.Common.Lz4Compressor;
import io.github.vuhoangha.Example.AnimalTest;
import io.github.vuhoangha.Example.FanoutTest;
import io.github.vuhoangha.Example.PeopleTest;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Main {


    public static void main(String[] args) {

//        run1();

        new FanoutTest().run();

    }


    public static void run1(){
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 3_000_000; i++) {
            run(bytes);
            bytes.clear();
        }
        long total = System.currentTimeMillis() - start;


        System.out.println("Run: " + total + " ms");
    }


    public static void run(Bytes<ByteBuffer> bytes) {
        int numberItem = 1;

        // Khởi tạo 10 đối tượng AnimalTest
        AnimalTest[] animals = new AnimalTest[numberItem];
        for (int i = 0; i < animals.length; i++) {
            animals[i] = new AnimalTest(
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
        }

        for (AnimalTest animal : animals) {
            animal.writeMarshallable(bytes);
        }

        byte[] serializedData = bytes.toByteArray();

        // Nén dữ liệu
        byte[] compressedData = Lz4Compressor.compressData(serializedData);

        if(true)return;

        // Giải nén dữ liệu
        byte[] restoredData = Lz4Compressor.decompressData(compressedData);

        // Tái sử dụng đối tượng Bytes để giải tuần tự hóa (Deserialization)
        Bytes<ByteBuffer> restoredBytes = Bytes.elasticByteBuffer();
        restoredBytes.write(restoredData);
        AnimalTest[] restoredAnimals = new AnimalTest[numberItem];
        for (int i = 0; i < restoredAnimals.length; i++) {
            restoredAnimals[i] = new AnimalTest(restoredBytes);
        }

        // In kết quả
        for (AnimalTest animal : restoredAnimals) {
//            System.out.println(animal);
        }

        // Giải phóng bộ nhớ cho restoredBytes
        restoredBytes.releaseLast();
    }


}