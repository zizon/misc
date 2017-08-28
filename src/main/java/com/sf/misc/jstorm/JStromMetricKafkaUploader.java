package com.sf.misc.jstorm;

import backtype.storm.Config;
import backtype.storm.generated.MetricInfo;
import backtype.storm.generated.MetricSnapshot;
import backtype.storm.generated.TopologyMetric;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.daemon.nimbus.metric.ClusterMetricsContext;
import com.alibaba.jstorm.daemon.nimbus.metric.MetricEvent;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.BaseMetricUploaderWithFlowControl;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.MetricUploader;
import com.alibaba.jstorm.daemon.nimbus.metric.uploader.TopologyMetricDataInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class JStromMetricKafkaUploader extends BaseMetricUploaderWithFlowControl implements MetricUploader {

    private static final Log LOGGER = LogFactory.getLog(JStromMetricKafkaUploader.class);

    public static final String METRIC_KAFKA_TOPIC = "jstorm-metrics";

    protected ClusterMetricsContext metric_context;
    protected Gson gson;
    protected Future<Boolean> topic_ok;
    protected Future<KafkaProducer<String, String>> producer;


    @Override
    public void init(NimbusData nimbus_data) throws Exception {
        LOGGER.info("initialized metric uploader");
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.metric_context = new ClusterMetricsContext(nimbus_data);

        this.topic_ok = this.async(() -> {
            try (AdminClient admin = AdminClient.create(kafkaConfig())) {
                admin.createTopics(
                        Collections.singleton(new NewTopic(METRIC_KAFKA_TOPIC, 10, (short) 1)),
                        new CreateTopicsOptions()
                ).all().get();
                return true;
            } catch (TopicExistsException e) {
                return true;
            } catch (Throwable throwable) {
                LOGGER.error("fail to ensure topic:" + METRIC_KAFKA_TOPIC, throwable);
                return false;
            }
        });

        this.producer = this.metric_context.getNimbusData().getScheduExec().submit(() -> {
            return new KafkaProducer<String, String>(this.kafkaConfig());
        });
    }

    @Override
    public void cleanup() {
        LOGGER.info("cleanup metric uploader");

        // help gc
        this.metric_context = null;
        this.gson = null;
        try {
            this.producer.get().close();
        } catch (Exception exception) {
            LOGGER.warn("unexpected exception when closing producer");
        } finally {
            this.producer = null;
        }
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
        // no particular events that are interested in
        // note： if handle all events here, it may cause a continuously
        // pushing behavior eg. invoking this sendEvent with identical parameters
        return true;
    }

    protected <T> Future<T> async(Callable<T> runnable) {
        return this.metric_context.getNimbusData().getScheduExec().submit(runnable);
    }

    protected Future<Boolean> upload(String cluster_name, String toplogy_id, Map<String, Object> metric_summary, int metric_cache_index) {
        // reuse schedule thread pool in nimbus data.
        return this.async(() -> {
            boolean ok = false;
            try {
                // get metric and its meta info
                TopologyMetric metric = metric_context.getMetricDataFromCache(metric_cache_index);
                TopologyMetricDataInfo info = metric_context.getMetricDataInfoFromCache(metric_cache_index);

                // build json
                JsonObject root = new JsonObject();
                root.add("metric", this.gson.toJsonTree(metric));
                root.add("info", this.gson.toJsonTree(info));
                root.add("summary", this.gson.toJsonTree(metric_summary));

                //push to kafka
                String json = this.gson.toJson(root);
                LOGGER.info("pushing metric:" + json + " of toplogy:" + toplogy_id + " to cluster:" + cluster_name);

                if (!this.topic_ok.get()) {
                    LOGGER.warn("topic:" + METRIC_KAFKA_TOPIC + " not ready,drop metric");
                    ok = false;
                } else {
                    Arrays.asList(metric.get_componentMetric(), //
                            metric.get_nettyMetric(),
                            metric.get_streamMetric(),
                            metric.get_taskMetric(),
                            metric.get_topologyMetric(),
                            metric.get_workerMetric()
                    ).parallelStream().forEach((each_metric) -> {
                        this.send(each_metric, info, metric_summary);
                    });

                    ok = true;
                }
            } catch (Exception exception) {
                LOGGER.error("fail to upload metric:" + metric_summary + " of topology:" + toplogy_id + " to cluster:" + cluster_name + " failed index:" + metric_cache_index, exception);
                ok = false;
            } finally {
                metric_context.markUploaded(metric_cache_index);
            }

            return ok;
        });
    }

    protected void send(MetricInfo metric, TopologyMetricDataInfo info, Map<String, Object> metric_summary) {
        metric.get_metrics().entrySet().parallelStream().forEach((entry) -> {
            final String name = entry.getKey();
            LOGGER.info("metric info" + info + " metric_summary:" + metric_summary);
            entry.getValue().entrySet().parallelStream().forEach((window) -> {
                int range = window.getKey();
                MetricSnapshot snapshot = window.getValue();

                String key = "";
                String value = "";
                //TODO

                try {
                    this.producer.get().send(new ProducerRecord<>(key, value));
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.warn("fail to send metric:" + name + " message,key:" + key + " value:" + value + " range:" + range + " snapshot:" + snapshot);
                }
            });

        });
    }

    protected Map<String, Object> kafkaConfig() {
        String kafka_servers = (String) this.metric_context.getNimbusData().getConf().get("kafka.servers");

        Map<String, Object> configuration = new TreeMap<>();
        configuration.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_servers);
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_servers);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return configuration;
    }

    public static void main(String args[]) {
        try {
            String[] zookeepers = new String[]{"10.202.77.200", "10.202.77.201", "10.202.77.202"};
            Map<String, Object> conf = Utils.readStormConfig();
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zookeepers));
            conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            /*
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
            */

            /*
            ZkUtils zkutils = ZkUtils.apply(String.join(",", ((List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS))),
                    1000, 1000, false);

            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(
                    zkutils,
                    METRIC_KAFKA_TOPIC,
                    AdminUtils.assignReplicasToBrokers(
                            AdminUtils.getBrokerMetadatas(
                                    zkutils,
                                    RackAwareMode.Safe$.MODULE$,
                                    AdminUtils.getBrokerMetadatas$default$3()
                            ),
                            10,
                            1,
                            AdminUtils.assignReplicasToBrokers$default$4(),
                            AdminUtils.assignReplicasToBrokers$default$5()
                    ),
                    new Properties(),
                    true
            );
            */

            Map<String, Object> configuration = new TreeMap<>();
            configuration.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.202.77.200:9092");
            configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.202.77.200:9092");
            configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.202.77.200:9092");

            KafkaProducer<String, String> producer = new KafkaProducer<>(configuration);
            producer.send(new ProducerRecord<>(METRIC_KAFKA_TOPIC, "test-key", "1")).get();

            //AdminUtils.createTopic(zkutils,"jstrom-metrics",10,1,new Properties(),RackAwareMode.Safe$.MODULE$);

            //LOGGER.info(client.getClusterInfo());
            /*
            AdminClient.create(configuration)
                    .createTopics(
                            Arrays.asList(new NewTopic(METRIC_KAFKA_TOPIC, 10, (short) 1)),
                            new CreateTopicsOptions()
                    ).all().get();
            */
        } catch (Exception e) {
            LOGGER.error("unexpected exception", e);
        } finally {
            LOGGER.info("done");
        }
    }
}
