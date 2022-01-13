package cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: keguang
 * @Date: 2022/1/12 17:36
 * @version: v1.0.0
 * @description: flink cdc同步mysql数据到mysql
 */
public class Mysql2MysqlDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);

        String sourceDDL =
                "CREATE TABLE mysql_binlog (\n" +
                        " id Int,\n" +
                        " name STRING,\n" +
                        " primary key (id) not enforced\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '127.0.0.1',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'root',\n" +
                        " 'database-name' = 'bigdata',\n" +
                        " 'table-name' = 'person',\n" +
                        " 'scan.startup.mode' = 'latest-offset'\n" +
                        ")";

        String sinkDDL =
                "CREATE TABLE test_cdc (" +
                        " id Int," +
                        " name STRING," +
                        " primary key (id) not enforced" +
                        ") WITH (" +
                        " 'connector' = 'jdbc'," +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver'," +
                        " 'url' = 'jdbc:mysql://127.0.0.1:3306/bigdata?serverTimezone=UTC&useSSL=false'," +
                        " 'username' = 'root'," +
                        " 'password' = 'root'," +
                        " 'table-name' = 'person1'" +
                        ")";

        String transformDmlSQL =  "insert into test_cdc select * from mysql_binlog";
        TableResult tableResult = tableEnv.executeSql(sourceDDL);
        TableResult sinkResult = tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformDmlSQL);

        senv.execute("Mysql2MysqlDemo");

    }
}
