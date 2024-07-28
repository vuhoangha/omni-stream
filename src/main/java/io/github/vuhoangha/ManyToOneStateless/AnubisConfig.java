package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;

@Setter
@Getter
@Accessors(chain = true)
@NoArgsConstructor
public class AnubisConfig {

    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean core;
    private Integer cpu;

    // IP, port của Saraswati
    private String saraswatiIP;
    private Integer saraswatiPort = 5590;

    // kiểu WaitStrategy được sử dụng để xử lý toàn bộ hoạt động của Anubis
    private OmniWaitStrategy waitStrategy;

    // timeout gửi/nhận message dành cho user (quản lý trên local Anubis)
    private Long localMsgTimeout = 30000L;
    // thời gian message còn hiệu lực khi Saraswati nhận được, quá thời gian này Saraswati từ chối xử lý msg
    // 'remoteMsgTimeout' nên nhỏ hơn 'localMsgTimeout' để chắc chắn rằng message này không thể được xử lý bởi Saraswati
    private Long remoteMsgTimeout = 15000L;

    public static AnubisConfig standardConfig() {
        return new AnubisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setWaitStrategy(OmniWaitStrategy.YIELD);
    }


    public static AnubisConfig bestPerformanceConfig() {
        return new AnubisConfig()
                .setCore(true)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.BUSY);
    }


    public static AnubisConfig minimalCpuConfig() {
        return new AnubisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setWaitStrategy(OmniWaitStrategy.SLEEP);
    }


    public String getUrl() {
        return MessageFormat.format("tcp://{0}:{1}", saraswatiIP, saraswatiPort + "");
    }


}
