package io.github.vuhoangha.Example.structure_example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.nio.ByteBuffer;

@AllArgsConstructor
@Getter
@Setter
public class AnimalTest implements WriteBytesMarshallable {

    private int index;
    private long age;
    private long weight;
    private long height;
    private long speed;
    private long energy;
    private long strength;
    private long agility;
    private long intelligence;
    private long lifespan;
    private long offspring;
    private long territorySize;

    public AnimalTest(Bytes<ByteBuffer> bytes) {
        index = bytes.readInt();
        age = bytes.readLong();
        weight = bytes.readLong();
        height = bytes.readLong();
        speed = bytes.readLong();
        energy = bytes.readLong();
        strength = bytes.readLong();
        agility = bytes.readLong();
        intelligence = bytes.readLong();
        lifespan = bytes.readLong();
        offspring = bytes.readLong();
        territorySize = bytes.readLong();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(index);
        bytes.writeLong(age);
        bytes.writeLong(weight);
        bytes.writeLong(height);
        bytes.writeLong(speed);
        bytes.writeLong(energy);
        bytes.writeLong(strength);
        bytes.writeLong(agility);
        bytes.writeLong(intelligence);
        bytes.writeLong(lifespan);
        bytes.writeLong(offspring);
        bytes.writeLong(territorySize);
    }

    @Override
    public String toString() {
        return "AnimalTest{" +
                "index=" + index +
                ", age=" + age +
                ", weight=" + weight +
                ", height=" + height +
                ", speed=" + speed +
                ", energy=" + energy +
                ", strength=" + strength +
                ", agility=" + agility +
                ", intelligence=" + intelligence +
                ", lifespan=" + lifespan +
                ", offspring=" + offspring +
                ", territorySize=" + territorySize +
                '}';
    }
}