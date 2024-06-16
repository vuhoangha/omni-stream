package io.github.vuhoangha.Common;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Lz4Compressor {


    public final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    public final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();


    public static byte[] compressData(byte[] data) {
        int originalLength = data.length;

        int maxCompressedLength = InstanceHolder.instance.compressor.maxCompressedLength(originalLength);
        maxCompressedLength = (int) (maxCompressedLength * 1.1);     // tăng kích cỡ tối đa thêm 10% phòng trường hợp dữ liệu nén còn lớn hơn dữ liệu gốc
        byte[] compressedData = new byte[maxCompressedLength + 4]; // +4 để lưu trữ độ dài dữ liệu gốc

        int compressedLength = InstanceHolder.instance.compressor.compress(data, 0, originalLength, compressedData, 4, maxCompressedLength);

        // Lưu trữ độ dài của dữ liệu gốc vào đầu mảng byte nén
        ByteBuffer.wrap(compressedData).order(ByteOrder.LITTLE_ENDIAN).putInt(originalLength);

        return Arrays.copyOf(compressedData, compressedLength + 4);
    }


    public static byte[] decompressData(byte[] compressedData) {
        // Lấy độ dài dữ liệu gốc
        int originalLength = ByteBuffer.wrap(compressedData, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();

        // Giải nén dữ liệu sử dụng LZ4SafeDecompressor
        byte[] restoredData = new byte[originalLength];
        InstanceHolder.instance.decompressor.decompress(compressedData, 4, compressedData.length - 4, restoredData, 0);

        return restoredData;
    }


    private static final class InstanceHolder {
        private static final Lz4Compressor instance = new Lz4Compressor();
    }

}
