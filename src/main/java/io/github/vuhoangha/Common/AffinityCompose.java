package io.github.vuhoangha.Common;

import net.openhft.affinity.AffinityLock;

import java.util.concurrent.locks.LockSupport;

public class AffinityCompose {

    public Thread thread;

    public AffinityLock lock;

    public AffinityCompose() {
    }

    public void release() {
        if (thread != null) {
            LockSupport.unpark(thread);
        }
        if (lock != null) {
            lock.release();
        }
    }

}
