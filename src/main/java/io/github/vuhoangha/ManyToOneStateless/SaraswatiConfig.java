package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;

@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class SaraswatiConfig {

    // port lắng nghe msg của Anubis
    private Integer port = 5590;

    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean core;
    private Integer cpu;

    // lắng nghe msg được Anubis gửi lên
    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean coreForListenAnubis;
    private Integer cpuForListenAnubis;

    // lắng nghe msg ghi vào queue
    // chạy trên 1 CPU core hoặc logical processor (Constance.CPU_TYPE) cụ thể ?
    private Boolean coreForListenQueue;
    private Integer cpuForListenQueue;
    // kiểu WaitStrategy để lắng nghe msg trong queue
    private OmniWaitStrategy queueWaitStrategy;
    // folder chứa queue tạm và reset mỗi khi chạy ứng dụng
    private String queueTempPath = "saraswati_input_queue";
    // thời gian tồn tại của 1 file ".cq4" trong chronicle queue temp. Mặc định 5 ngày xóa 1 lần
    private Long queueTempTtl = (long) 5 * 24 * 60 * 60;


    // cân bằng giữa hiệu suất và tiêu tốn CPU
    public static SaraswatiConfig standardConfig() {
        return new SaraswatiConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setCoreForListenAnubis(false)
                .setCpuForListenAnubis(Constance.CPU_TYPE.NONE)
                .setCoreForListenQueue(false)
                .setCpuForListenQueue(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.YIELD);
    }

    // cho hiệu suất tốt nhất
    public static SaraswatiConfig bestPerformanceConfig() {
        return new SaraswatiConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY)
                .setCoreForListenAnubis(true)
                .setCpuForListenAnubis(Constance.CPU_TYPE.ANY)
                .setCoreForListenQueue(true)
                .setCpuForListenQueue(Constance.CPU_TYPE.ANY)
                .setQueueWaitStrategy(OmniWaitStrategy.BUSY);
    }

    // tiết kiệm CPU nhất
    public static SaraswatiConfig minimalCpuConfig() {
        return new SaraswatiConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE)
                .setCoreForListenAnubis(false)
                .setCpuForListenAnubis(Constance.CPU_TYPE.NONE)
                .setCoreForListenQueue(false)
                .setCpuForListenQueue(Constance.CPU_TYPE.NONE)
                .setQueueWaitStrategy(OmniWaitStrategy.SLEEP);
    }


    public String getUrl() {
        return MessageFormat.format("tcp://*:{0}", port + "");
    }


}
