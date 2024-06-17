package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.BlockingWaitStrategy;
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

    // timeout gửi nhận message dành cho user (quản lý trên local Anubis)
    private Long timeout;

    // thời gian message còn hiệu lực khi Saraswati nhận được, quá thời gian này Saraswati từ chối xử lý msg
    // 'ttl' nên nhỏ hơn 'timeout' để chắc chắn rằng message này không thể được xử lý bởi Saraswati
    private Long ttl;

    // kiểu WaitStrategy được sử dụng để gom message từ nhiều thread để dùng ZeroMQ gửi qua Saraswati
    private WaitStrategy waitStrategy;

    // port mà Saraswati lắng nghe
    private Integer port;

    // port mà Saraswati gửi lại Time Server
    private Integer timePort;

    // kích cỡ ring buffer của disruptor gửi/nhận message. Phải là dạng 2^n
    private Integer ringBufferSize;

    // kiểu WaitStrategy được sử dụng để lắng nghe các msg yêu cầu gửi đi
    private OmniWaitStrategy disruptorWaitStrategy;

    // cho phép disruptor chạy trên 1 CPU core riêng ?
    private Boolean enableDisruptorBindingCore;

    /*
     * cho phép disruptor chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer disruptorCpu;

    // bao lâu đồng bộ time server 1 lần
    private Long syncTimeServerInterval;


    private AnubisCfg() {
    }

    public static AnubisCfg getDefault() {
        AnubisCfg cfg = new AnubisCfg();

        cfg.setPort(5590);
        cfg.setTimePort(5591);
        cfg.setTimeout(30000L);
        cfg.setTtl(15000L);
        cfg.setWaitStrategy(new BlockingWaitStrategy());
        cfg.setRingBufferSize(2 << 16);     // 131072
        cfg.setDisruptorWaitStrategy(OmniWaitStrategy.YIELD);
        cfg.setDisruptorCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableDisruptorBindingCore(false);
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
