package io.github.vuhoangha.OneToManyStateless;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class OdinConfig {

    private Integer realtimePort = 5570;
    private Integer routerPort = 5571;

    private int recentEventsSize = 1_000_000;

    // độ dài tối thiểu (tính theo byte) để bắt đầu nén dữ liệu
    private Integer minLengthForCompression = 1024;

    private OmniWaitStrategy waitStrategy;

    // số message tối đa muốn lấy trong 1 request fetch bởi DEALER
    private int maxMessagesPerFetch = 10_000;

    // cấu hình core/cpu cho luồng chính
    private Boolean core;
    private Integer cpu;


    public static OdinConfig standardConfig() {
        return new OdinConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setWaitStrategy(OmniWaitStrategy.YIELD);
    }

    public static OdinConfig bestPerformanceConfig() {
        return new OdinConfig()
                .setCore(true)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.BUSY);
    }

    public static OdinConfig minimalCpuConfig() {
        return new OdinConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.SLEEP);
    }

}
