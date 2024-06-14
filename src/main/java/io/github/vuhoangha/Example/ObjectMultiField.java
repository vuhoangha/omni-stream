package io.github.vuhoangha.Example;

import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public class ObjectMultiField implements WriteBytesMarshallable {

    public long field1 = 1;
    public long field2 = 2;
    public long field3 = 3;
    public long field4 = 4;
    public long field5 = 5;
    public long field6 = 6;
    public long field7 = 7;
    public long field8 = 8;
    public long field9 = 9;
    public long field10 = 10;
    public long field11 = 11;
    public long field12 = 12;
    public long field13 = 13;
    public long field14 = 14;
    public long field15 = 15;
    public long field16 = 16;
    public long field17 = 17;
    public long field18 = 18;
    public long field19 = 19;
    public long field20 = 20;

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeLong(this.field1);
        bytes.writeLong(this.field2);
        bytes.writeLong(this.field3);
        bytes.writeLong(this.field4);
        bytes.writeLong(this.field5);
        bytes.writeLong(this.field6);
        bytes.writeLong(this.field7);
        bytes.writeLong(this.field8);
        bytes.writeLong(this.field9);
        bytes.writeLong(this.field10);
        bytes.writeLong(this.field11);
        bytes.writeLong(this.field12);
        bytes.writeLong(this.field13);
        bytes.writeLong(this.field14);
        bytes.writeLong(this.field15);
        bytes.writeLong(this.field16);
        bytes.writeLong(this.field17);
        bytes.writeLong(this.field18);
        bytes.writeLong(this.field19);
        bytes.writeLong(this.field20);
    }

}
