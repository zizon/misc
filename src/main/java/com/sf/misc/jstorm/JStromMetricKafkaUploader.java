package com.sf.misc.jstorm;

import backtype.storm.Config;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsContext;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.BaseMetricUploaderWithFlowControl;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.TopologyMetricDataInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public class JStromMetricKafkaUploader extends BaseMetricUploaderWithFlowControl implements MetricUploader {

    private static final Log LOGGER = LogFactory.getLog(JStromMetricKafkaUploader.class);

    protected ClusterMetricsContext metric_context;

    @Override
    public void init(NimbusData nimbus_data) throws Exception {
        LOGGER.info("initialized metric uploader");
        this.metric_context = new ClusterMetricsContext(nimbus_data);
    }

    @Override
    public void cleanup() {
        LOGGER.info("cleanup metric uploader");

        // help gc
        this.metric_context = null;
    }

    @Override
    public boolean registerMetrics(String cluster_name, String toplogy_id, Map<String, Long> metric_summary) throws Exception {
        LOGGER.info("register metric:" + metric_summary + " of topology:" + toplogy_id + " to cluster:" + cluster_name);
        return true;
    }

    @Override
    public boolean upload(String cluster_name, String toplogy_id, TopologyMetric tpMetric, Map<String, Object> metric_summary) {
        throw new UnsupportedOperationException("deprecated operation,disabled");
    }

    @Override
    public boolean upload(String cluster_name, String toplogy_id, Object metric_cache_index, Map<String, Object> metric_summary) {
        Future<?> async = this.upload(cluster_name, toplogy_id, metric_summary, (Integer) metric_cache_index);

        // under flow control?
        if (!syncToUpload()) {
            try {
                async.get();
            } catch (InterruptedException | ExecutionException exception) {
                // silent it
            }
        }

        // aways return true
        return true;
    }

    @Override
    public boolean sendEvent(String cluster_name, MetricEvent event) {
        //no particular events that are interested in
        // note： if handle all events here, it may cause a continuously
        // pushing behavior eg. invoking this sendEvent with identical parameters
        return true;
    }

    protected Future<?> upload(String cluster_name, String toplogy_id, Map<String, Object> metric_summary, int metric_cache_index) {
        return ForkJoinPool.commonPool().submit(() -> {
            try {
                // get metric and its meta info
                TopologyMetric metric = metric_context.getMetricDataFromCache(metric_cache_index);
                TopologyMetricDataInfo info = metric_context.getMetricDataInfoFromCache(metric_cache_index);

                //TODO
                //push to kafka
                LOGGER.info("pushing metric:" + info + " \n\t with content:" + metric + " \n\t and summary:" + metric_summary);
            } catch (Exception exception) {
                LOGGER.error("fail to upload metric:" + metric_summary + " of topology:" + toplogy_id + " to cluster:" + cluster_name + " failed index:" + metric_cache_index);
            } finally {
                metric_context.markUploaded(metric_cache_index);
            }
        });
    }


    public static void main(String args[]) {
        try {
            Map<String, Object> conf = Utils.readStormConfig();
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("10.202.77.200", "10.202.77.201", "10.202.77.202"));
            conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();

            TopologyMetric metric = new TopologyMetric();

            MetricInfo metric_info = new MetricInfo();
            metric.set_topologyMetric(metric_info);

            metric.set_componentMetric(metric_info);

            metric.set_workerMetric(metric_info);

            metric.set_taskMetric(metric_info);

            metric.set_streamMetric(metric_info);

            metric.set_nettyMetric(metric_info);

            client.uploadTopologyMetrics("test-topplogy", metric);
            LOGGER.info(client.getClusterInfo());
        } catch (Exception e) {
            LOGGER.error("unexpected exception", e);
        } finally {
            LOGGER.info("done");
        }
    }
}
