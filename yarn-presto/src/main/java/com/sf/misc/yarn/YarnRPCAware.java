package com.sf.misc.yarn;

import org.apache.hadoop.yarn.ipc.YarnRPC;

public interface YarnRPCAware {
    public YarnRPC rpc();
}
