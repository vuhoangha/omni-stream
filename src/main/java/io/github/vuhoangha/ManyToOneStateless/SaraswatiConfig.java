package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.Constance;
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


    // cân bằng giữa hiệu suất và tiêu tốn CPU
    public static SaraswatiConfig standardConfig() {
        return new SaraswatiConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.ANY);
    }

    // cho hiệu suất tốt nhất
    public static SaraswatiConfig bestPerformanceConfig() {
        return new SaraswatiConfig()
                .setCore(true)
                .setCpu(Constance.CPU_TYPE.NONE);
    }

    // tiết kiệm CPU nhất
    public static SaraswatiConfig minimalCpuConfig() {
        return new SaraswatiConfig()
                .setCore(false)
                .setCpu(Constance.CPU_TYPE.NONE);
    }


    public String getUrl() {
        return MessageFormat.format("tcp://*:{0}", port + "");
    }


}
