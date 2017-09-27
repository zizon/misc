package com.sf.misc.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HiveQueryPlaner {

    private static final Log LOGGER = LogFactory.getLog(HiveQueryPlaner.class);

    public static enum RejectReason {
        None, ParseError;
    }

    public static class PlanGenerationFailException extends RuntimeException {
        public PlanGenerationFailException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }

    public static interface FetchPlan {
    }

    public static class VirtualFetchPlan implements FetchPlan {
        public VirtualFetchPlan(String name, List<String> schema, List<List<String>> values) {

        }
    }


    protected static final String DEFAULT_DATABASE = "_default_database_";

    protected String query;
    protected ASTNode root;
    protected Exception parse_excetion;

    protected ConcurrentMap<String, FetchPlan> fetch_plans;

    public HiveQueryPlaner(String query) throws PlanGenerationFailException {
        this.query = query;
        this.parse_excetion = null;
        try {
            this.root = this.trim(new ParseDriver().parse(query));
        } catch (ParseException e) {
            throw new PlanGenerationFailException("fail to parse query:" + query, e);
        }

        this.fetch_plans = new ConcurrentHashMap<>();
    }

    public FetchPlan genQueryPlan() {
        if (this.root.getType() != HiveParser.TOK_QUERY) {
            // skip not query statement
            return null;
        }

        this.genQueryPlan(this.root);
        return null;
    }


    public Exception exception() {
        return this.parse_excetion;
    }

    protected ASTNode trim(ASTNode node) {
        List<ASTNode> new_nodes = new LinkedList<>();
        for (int i = node.getChildCount() - 1; i >= 0; i--) {
            ASTNode child = (ASTNode) node.getChild(i);
            switch (child.getType()) {
                case HiveParser.EOF:
                    node.deleteChild(i);
                    break;
                default:
                    // drill down and replace
                    node.replaceChildren(i, i, trim(child));
            }
        }

        if (node.getToken() == null && node.getChildCount() == 1) {
            node = (ASTNode) node.getChild(0);
        }

        return node;
    }

    protected void consumeIfExists(ASTNode node, int child_token_type, Consumer<ASTNode> consumer) {
        ASTNode candidate = (ASTNode) node.getFirstChildWithType(child_token_type);
        if (candidate != null) {
            consumer.accept(candidate);
            node.deleteChild(candidate.getChildIndex());
        }
    }

    protected FetchPlan genQueryPlan(ASTNode node) {
        // process cte first
        consumeIfExists(node, HiveParser.TOK_CTE, this::genCTEPlan);

        // process query
        consumeIfExists(node, HiveParser.TOK_QUERY, this::genQueryPlan);

        // process from
        consumeIfExists(node, HiveParser.TOK_FROM, this::genFromPlan);

        //todo
        return null;
    }

    protected void genCTEPlan(ASTNode node) {
        ASTNode query = (ASTNode) node.getFirstChildWithType(HiveParser.TOK_SUBQUERY);

        // gen plan
        FetchPlan plan = this.genQueryPlan((ASTNode) query.getChild(0));

        //todo
        //this.fetch_plans.put(query.getChild(1).getText().toLowerCase(), plan);
    }

    protected void genFromPlan(ASTNode node) {
        this.consumeIfExists(node,HiveParser.TOK_VIRTUAL_TABLE,this::genVirtualTablePlan);

        this.consumeIfExists(node,HiveParser.TOK_JOIN,this::genJoinPlan);
    }

    protected void genVirtualTablePlan(ASTNode node) {
        // find table name
        ASTNode virtual_table = (ASTNode) node.getFirstChildWithType(HiveParser.TOK_VIRTUAL_TABREF);
        String table_name = virtual_table.getFirstChildWithType(HiveParser.TOK_TABNAME).getChild(0).getText().toLowerCase();

        // columns
        ASTNode columns = (ASTNode) virtual_table.getFirstChildWithType(HiveParser.TOK_COL_NAME);
        List<String> schema = columns.getChildren().stream().map(
                (child) -> ((ASTNode) child).getText() //
        ).collect(Collectors.toList());

        // values
        ASTNode values = (ASTNode) node.getFirstChildWithType(HiveParser.TOK_VALUES_TABLE);
        List<List<String>> table_vlaues =values.getChildren().stream().map(
                (child) -> child.getChildren().stream().map(
                        (value_node) -> ((ASTNode)value_node).getText()
                ).collect(Collectors.toList())
        ).collect(Collectors.toList());

        this.fetch_plans.put(table_name,new VirtualFetchPlan(table_name,schema,table_vlaues));
    }

    protected  void genJoinPlan(ASTNode node){
        //todo
        LOGGER.info(node.toStringTree());
    }

    @Override
    public String toString() {
        return this.root != null ? this.root.toStringTree() : "None";
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", ".");
        String meta_uri = "thrift://10.202.77.200:9083";
        //String query = "select * from  a join b";
        String query = "with c as  (select * from x  Y) select * from (select * from b) a join  d on a.c = d.c where c!=null";
        // (TOK_CREATETABLE (TOK_TABNAME test) (TOK_LIKETABLE (TOK_TABNAME test2)))
        // (TOK_CREATETABLE (TOK_TABNAME db1 test) (TOK_LIKETABLE (TOK_TABNAME test2)))
        query = "create table db1.test like test2";
        try {
            /*
            //LOGGER.info(Thread.currentThread().getContextClassLoader().getResources(".").nextElement().toString());
            HiveConf configuration = new HiveConf();

            // guide meta store location
            configuration.set("hive.metastore.uris", meta_uri);

            // overwrite ugi.
            // workaround for windows
            configuration.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, HiveMetaSyncer.AnyGroupMappingServiceProvider.class, GroupMappingServiceProvider.class);
            UserGroupInformation.setConfiguration(configuration);


            //configuration.set("hive.exec.scratchdir", Thread.currentThread().getContextClassLoader().getResources(".").nextElement().toString());
            SessionState.start(configuration);
            //SessionState.start(configuration);

            //SessionState.start(configuration);
            //SessionState.start(SessionState.get());

            Context context = new Context(configuration);
            ParseDriver driver = new ParseDriver();
            ASTNode ast = driver.parse(query, context);
            LOGGER.info(ast);
             */
            HiveQueryPlaner planer = new HiveQueryPlaner(query);
            planer.genQueryPlan();

            //LOGGER.info(((ASTNode) root.getChild(1)).getToken() == null);
            //LOGGER.info(((ASTNode)root.getChild(1)).getToken().getClass());


            // is query?
            LOGGER.info(planer);

        } catch (Exception e) {
            LOGGER.error("unexpected exception", e);
        }


    }
}
