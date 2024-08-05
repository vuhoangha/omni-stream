package io.github.vuhoangha.OneToManyStateless;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;

@Getter
@Setter
@Accessors(chain = true)
public class ArtemisConfig {

    // IP của Fanout host
    private String sourceIP;

    // port của Fanout dùng để publish dữ liệu cho các Sinkin
    private int realtimePort = 5570;

    // port của Fanout dùng để response các yêu cầu lấy message bị miss của Sinkin
    private int dealerPort = 5571;

    // kiểu WaitStrategy để chờ giữa các lần quét thông tin
    private OmniWaitStrategy waitStrategy;

    // 1 msg ở trong hàng chờ quá lâu đồng nghĩa với việc các msg trước nó đã bị loss, Artemis sẽ request sang Odin để lấy các message đó
    private int messageExpirationDuration = 2_000;

    // thời gian định kỳ lấy message mới nhất từ Odin (miliseconds)
    private int latestMessageFetchInterval = 3_000;

    // thời gian định kỳ check và lấy các message bị thiếu
    private int lostMessageScanInterval = 3_000;

    // nếu 1 msg đã nằm quá lâu trong hàng đợi chứng tỏ đã miss các msg trước đó, cần restart lại
    private int messageQueueTimeoutBeforeRestart = 120_000;

    private int messagePoolSize = 1_000_000;

    // toàn bộ hệ thống chạy trên 1 core/cpu riêng
    private Boolean core;
    private Integer cpu;

    public String getRealTimeUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, realtimePort + "");
    }

    public String getDealerUrl() {
        return MessageFormat.format("tcp://{0}:{1}", sourceIP, dealerPort + "");
    }

    public static ArtemisConfig standardConfig() {
        return new ArtemisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setWaitStrategy(OmniWaitStrategy.YIELD);
    }

    public static ArtemisConfig bestPerformanceConfig() {
        return new ArtemisConfig()
                .setCore(true)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.BUSY);
    }

    public static ArtemisConfig minimalCpuConfig() {
        return new ArtemisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.SLEEP);
    }

}
