package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.*;
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

    // lắng nghe msg ghi vào queue
    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean coreForListenQueue;
    private Integer cpuForListenQueue;
    // folder chứa queue tạm và reset mỗi khi chạy ứng dụng
    private String queueTempPath = "anubis_temp_queue";
    // thời gian tồn tại của 1 file ".cq4" trong chronicle queue temp. Mặc định 5 ngày xóa 1 lần
    private Long queueTempTtl = (long) 5 * 24 * 60 * 60;
    // kiểu WaitStrategy để lắng nghe msg trong queue
    private OmniWaitStrategy queueWaitStrategy;

    // kiểu WaitStrategy được sử dụng bởi Lmax để gom message từ nhiều thread --> ghi vào queue
    private WaitStrategy disruptorWaitStrategy;
    // lắng nghe msg được ứng dụng gửi
    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean coreForReceiveMessage;
    private Integer cpuForReceiveMessage;

    // timeout gửi/nhận message dành cho user (quản lý trên local Anubis)
    private Long msgTimeout = 30000L;
    // thời gian message còn hiệu lực khi Saraswati nhận được, quá thời gian này Saraswati từ chối xử lý msg
    // 'ttl' nên nhỏ hơn 'timeout' để chắc chắn rằng message này không thể được xử lý bởi Saraswati
    private Long msgTtl = 15000L;

    public static AnubisConfig standardConfig() {
        return new AnubisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setCoreForListenQueue(false)
                .setCpuForListenQueue(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.YIELD)
                .setCoreForReceiveMessage(false)
                .setCpuForReceiveMessage(Constance.CPU_TYPE.NONE)
                .setDisruptorWaitStrategy(new YieldingWaitStrategy());
    }


    public static AnubisConfig bestPerformanceConfig() {
        return new AnubisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setCoreForListenQueue(true)
                .setCpuForListenQueue(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.BUSY)
                .setCoreForReceiveMessage(true)
                .setCpuForReceiveMessage(Constance.CPU_TYPE.NONE)
                .setDisruptorWaitStrategy(new BusySpinWaitStrategy());
    }


    public static AnubisConfig minimalCpuConfig() {
        return new AnubisConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setCoreForListenQueue(false)
                .setCpuForListenQueue(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.SLEEP)
                .setCoreForReceiveMessage(false)
                .setCpuForReceiveMessage(Constance.CPU_TYPE.NONE)
                .setDisruptorWaitStrategy(new SleepingWaitStrategy());
    }


    public String getUrl() {
        return MessageFormat.format("tcp://{0}:{1}", saraswatiIP, saraswatiPort + "");
    }


}
