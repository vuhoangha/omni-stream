package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.Constance;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.MessageFormat;

@Getter
@Setter
@Accessors(chain = true)
public class SaraswatiCfg {

    // port mà Collector lắng nghe các request của Snipper
    private Integer port;

    // cho phép ZMQ Router chạy trên 1 CPU core riêng ?
    private Boolean enableZRouterBindingCore;

    /*
     * cho phép ZMQ Router chạy trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer zRouterCpu;

    // cho phép toàn bộ Collector chạy chung trên 1 CPU core riêng ?
    private Boolean enableBindingCore;

    /*
     * cho phép Collector chạy chung trên 1 logical processor riêng ?
     *      "-1" là chạy trên 1 logical processor bất kỳ. Nếu có Logical Processor isolate dự trữ thì nó sẽ ưu tiên chạy trước. Nếu ko sẽ dùng chung với hệ điều hành
     *      "-2": chạy trên nhiều logical processor bất kỳ theo sự điều phối của hệ điều hành
     */
    private Integer cpu;

    // port để lắng nghe các yêu cầu lấy time
    private Integer timePort;


    private SaraswatiCfg() {
    }

    public static SaraswatiCfg getDefault() {
        SaraswatiCfg cfg = new SaraswatiCfg();

        // set default value
        cfg.setPort(5590);
        cfg.setTimePort(5591);
        cfg.setEnableBindingCore(false);
        cfg.setCpu(Constance.CPU_TYPE.ANY);
        cfg.setEnableZRouterBindingCore(false);
        cfg.setZRouterCpu(Constance.CPU_TYPE.NONE);

        return cfg;
    }


    public String getTimeUrl() {
        return MessageFormat.format("tcp://*:{0}", timePort + "");
    }


    public String getUrl() {
        return MessageFormat.format("tcp://*:{0}", port + "");
    }


}
