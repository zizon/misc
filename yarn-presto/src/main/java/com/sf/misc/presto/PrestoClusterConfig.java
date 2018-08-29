package com.sf.misc.presto;

import com.google.common.collect.ImmutableMap;
import com.sf.misc.yarn.ContainerConfiguration;

import java.util.Map;

public class PrestoClusterConfig {

    protected int coordinator_memroy = 512;
    protected int num_of_workers;
    protected int worker_memory = coordinator_memroy;
    protected String cluster_name;
    protected Map<String, String> tuning_parameters;

    protected PrestoClusterConfig(int coordinator_memory, int num_of_worker, int worker_memeory, String cluster_name, Map<String, String> tuning_parameters) {
        this.coordinator_memroy = coordinator_memory;
        this.num_of_workers = num_of_worker;
        this.worker_memory = worker_memeory;
        this.cluster_name = cluster_name;
        this.tuning_parameters = tuning_parameters;
    }

    public int getCoordinatorMemroy() {
        return coordinator_memroy;
    }

    public void setCoordinatorMemroy(int coordinator_memroy) {
        this.coordinator_memroy = coordinator_memroy;
    }

    public int getNumOfWorkers() {
        return num_of_workers;
    }

    public void setNumOfWorkers(int num_of_workers) {
        this.num_of_workers = num_of_workers;
    }

    public int getWorkerMemory() {
        return worker_memory;
    }

    public void setWorkerMemory(int worker_memory) {
        this.worker_memory = worker_memory;
    }

    public String getClusterName() {
        return cluster_name;
    }

    public void setClusterName(String cluster_name) {
        this.cluster_name = cluster_name;
    }

    public Map<String, String> getTuningParameters() {
        return tuning_parameters;
    }

    public void setTuning_parameters(Map<String, String> tuningParameters) {
        this.tuning_parameters = tuning_parameters;
    }

    public static class Builder {
        private int coordinator_memory;
        private int num_of_worker;
        private int worker_memeory;
        private String cluster_name;
        private Map<String, String> tuning_parameters;
        private String classloader;
        private Map<String, String> log_levels;


        public Builder setCoordinatorMemory(int coordinator_memory) {
            this.coordinator_memory = coordinator_memory;
            return this;
        }

        public Builder setNumOfWorker(int num_of_worker) {
            this.num_of_worker = num_of_worker;
            return this;
        }

        public Builder setWorkerMemeory(int worker_memeory) {
            this.worker_memeory = worker_memeory;
            return this;
        }

        public Builder setClusterName(String cluster_name) {
            this.cluster_name = cluster_name;
            return this;
        }

        public Builder setTuningParameters(Map<String, String> tuning_parameters) {
            this.tuning_parameters = tuning_parameters;
            return this;
        }

        public Builder setClassloader(String classloader) {
            this.classloader = classloader;
            return this;
        }

        public Builder setLogLevels(Map<String, String> log_levels) {
            this.log_levels = ImmutableMap.copyOf(log_levels);
            return this;
        }

        public ContainerConfiguration buildMasterContainerConfig() {
            PrestoClusterConfig cluster_config = new PrestoClusterConfig(coordinator_memory, num_of_worker, worker_memeory, cluster_name, tuning_parameters);
            ContainerConfiguration container_config = new ContainerConfiguration( //
                    AirliftPresto.class, //
                    cluster_config.getClusterName(), //
                    1, //
                    1024, //
                    this.classloader, //
                    log_levels //
            );

            // add context config
            container_config.addContextConfig(cluster_config);
            return container_config;
        }
    }
}
