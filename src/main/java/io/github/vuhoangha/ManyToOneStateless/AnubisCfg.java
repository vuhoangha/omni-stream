package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;

@Setter
@Getter
@Accessors(chain = true)
public class AnubisCfg {

    // IP của Saraswati
    private String saraswatiIP;

    // đường dẫn tới folder chứa queue tạm
    private String queueTempPath;

    // timeout gửi nhận message dành cho user (quản lý trên local Anubis)
    private Long timeout;

    // thời gian message còn hiệu lực khi Saraswati nhận được, quá thời gian này Saraswati từ chối xử lý msg
    // 'ttl' nên nhỏ hơn 'timeout' để chắc chắn rằng message này không thể được xử lý bởi Saraswati
    private Long ttl;

    // port mà Saraswati lắng nghe
    private Integer port;

    // port mà Saraswati gửi lại Time Server
    private Integer timePort;


    private Boolean enableBindingCore;

    private Integer cpu;


    // kiểu WaitStrategy được sử dụng bởi Lmax để gom message từ nhiều thread --> ghi vào queue để chờ gửi sang Saraswati
    private WaitStrategy disruptorWaitStrategy;

    // kích cỡ ring buffer của disruptor gửi/nhận message. Phải là dạng 2^n
    private Integer ringBufferSize;

    // cho phép disruptor chạy trên 1 CPU core riêng ?
    private Boolean enableDisruptorBindingCore;

    /*
     * cho phép disruptor chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer disruptorCpu;


    // kiểu WaitStrategy được sử dụng để quét xem queue có item mới không và gửi đi. Ngoòi ra nó còn xem xét có data nào được Saraswati phản hồi không
    private OmniWaitStrategy queueWaitStrategy;

    private Boolean enableQueueBindingCore;

    private Integer queueCpu;


    // bao lâu đồng bộ time server 1 lần
    private Long syncTimeServerInterval;

    // thời gian tồn tại của 1 file ".cq4" trong chronicle queue
    private Long ttlQueueTempFile;


    private AnubisCfg() {
    }


    public static AnubisCfg getDefault() {
        AnubisCfg cfg = new AnubisCfg();

        cfg.setPort(5590);
        cfg.setTimePort(5591);
        cfg.setTimeout(30000L);
        cfg.setTtl(15000L);

        cfg.setEnableBindingCore(false);
        cfg.setCpu(Constance.CPU_TYPE.ANY);

        cfg.setDisruptorWaitStrategy(new BlockingWaitStrategy());
        cfg.setRingBufferSize(2 << 10);     // 1024 TODO xem xét đoạn này xem size nào phù hợp nhé
        cfg.setDisruptorCpu(Constance.CPU_TYPE.NONE);
        cfg.setEnableDisruptorBindingCore(false);

        cfg.setQueueCpu(Constance.CPU_TYPE.NONE);
        cfg.setEnableQueueBindingCore(false);
        cfg.setQueueWaitStrategy(OmniWaitStrategy.SLEEP);

        cfg.setSyncTimeServerInterval(5000L);
        cfg.setTtlQueueTempFile(60 * 60 * 24 * 5L);    // 5 ngày

        return cfg;
    }


    public static AnubisCfg bestPerf() {
        AnubisCfg cfg = new AnubisCfg();

        cfg.setPort(5590);
        cfg.setTimePort(5591);
        cfg.setTimeout(30000L);
        cfg.setTtl(15000L);

        cfg.setEnableBindingCore(true);
        cfg.setCpu(Constance.CPU_TYPE.ANY);

        cfg.setDisruptorWaitStrategy(new BusySpinWaitStrategy());
        cfg.setRingBufferSize(2 << 10);     // 1024 TODO xem xét đoạn này xem size nào phù hợp nhé
        cfg.setDisruptorCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableDisruptorBindingCore(true);

        cfg.setQueueCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableQueueBindingCore(true);
        cfg.setQueueWaitStrategy(OmniWaitStrategy.BUSY);

        cfg.setSyncTimeServerInterval(5000L);

        return cfg;
    }


    public String getTimeServerUrl() {
        return MessageFormat.format("tcp://{0}:{1}", saraswatiIP, timePort + "");
    }


    public String getUrl() {
        return MessageFormat.format("tcp://{0}:{1}", saraswatiIP, port + "");
    }


}
