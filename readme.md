mvn package && hadoop jar target/`ls target/ | grep "\.jar$"` com.sf.misc.HiveMetaSyncer
