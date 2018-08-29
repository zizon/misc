package com.sf.misc.presto;

import com.facebook.presto.server.ServerConfig;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class PrestoContainerConfig {

    protected boolean coordinator;
    protected String tuning;
    protected int memory;
    protected int cpu = 1;

    public int getMemory() {
        return memory;
    }

    @Config("airlift.presto.memory")
    @ConfigDescription("presto node memroy usage")
    public void setMemory(int memory) {
        this.memory = memory;
    }


    @Config("coordinator")
    @ConfigDescription("wether is presto coordinator")
    public boolean getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(boolean coordinator) {
        this.coordinator = coordinator;
    }

    public String getTuning() {
        return tuning;
    }

    @Config("presto.tuning")
    @ConfigDescription("tunign parmarmeters for presto nodes")
    public void setTuning(String tuning) {
        this.tuning = tuning;
    }

    public int getCpu() {
        return this.cpu;
    }

    @Config("airlift.presto.cpu")
    @ConfigDescription("presto node cpu usage")
    public void setCpu(int cpu) {
        this.cpu = cpu;
    }
}
