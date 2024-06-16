package io.github.vuhoangha.Example.structure_example;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.nio.ByteBuffer;

@AllArgsConstructor
@NoArgsConstructor
public class CarTest implements WriteBytesMarshallable {

    private int id;

    private int age;

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(id);
        bytes.writeInt(age);
    }

    public static void clone(CarTest src, CarTest dst) {
        dst.id = src.id;
        dst.age = src.age;
    }

    public static void reader(CarTest dst, Bytes<ByteBuffer> bytesIn) {
        dst.id = bytesIn.readInt();
        dst.age = bytesIn.readInt();
    }

    @Override
    public String toString() {
        return "CarTest{" +
                "id=" + id +
                ", age='" + age + '\'' +
                '}';
    }

}
