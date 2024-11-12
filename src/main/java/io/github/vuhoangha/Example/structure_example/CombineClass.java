package io.github.vuhoangha.Example.structure_example;

import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public class CombineClass implements WriteBytesMarshallable {

    public byte type;

    public AnimalTest animalTest;
    public CarTest carTest;

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(type);
        if (type == 1) {
            animalTest.writeMarshallable(bytes);
        } else if (type == 2) {
            carTest.writeMarshallable(bytes);
        }
    }

}
