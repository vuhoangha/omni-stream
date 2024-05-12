package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;

@Data
public class OdinCacheEvent {

    private long seq;

    private long time;

    private byte[] data;

}
