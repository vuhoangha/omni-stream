package io.github.vuhoangha.OneToManyStateless;

import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.Utils;
import lombok.AllArgsConstructor;

import java.util.concurrent.ThreadFactory;

@AllArgsConstructor
public class OdinThreadProcessor implements ThreadFactory {

    private OdinCfg odinCfg;

    @Override
    public synchronized Thread newThread(Runnable runnable) {
        return new Thread(() -> {
            AffinityCompose affinityCompose = Utils.runWithThreadAffinity(
                    "Odin Processor",
                    false,
                    odinCfg.getEnableBindingCore(),
                    odinCfg.getCpu(),
                    false,
                    odinCfg.getDisruptorHandlerCpu(),
                    runnable);
            affinityCompose.release();
        });
    }

}
