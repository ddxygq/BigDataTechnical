package cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @Author: keguang
 * @Date: 2023/2/17 15:26
 * @version: v1.0.0
 * @description:
 */
public class Mysq2ClickHouse {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);

        String sourceDDL =
                "CREATE TABLE mysql_user (\n" +
                        " id Int,\n" +
                        " created_at timestamp,\n" +
                        " primary key (id) not enforced\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '192.168.20.250',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'bigdata',\n" +
                        " 'password' = 'bigdata123',\n" +
                        " 'database-name' = 'flash_center',\n" +
                        " 'table-name' = 'user',\n" +
                        " 'scan.startup.mode' = 'latest-offset'\n" +
                        ")";

        tableEnv.executeSql(sourceDDL);

        /*String url = "jdbc:clickhouse://192.168.20.250:8123/test";
        String userName = "default";
        String password = "clickhouse";
        String ckTable = "user";
        // 输出目标表
        String sinkDDL =
                "CREATE TABLE clickhouse_user (\n" +
                        " id INT NOT NULL,\n" +
                        " username STRING,\n" +
                        " primary key (id) not enforced\n" +
                        ") WITH (\n" +
                        " 'connector' = 'clickhouse',\n" +
                        " 'driver' = 'com.mysql.jdbc.Driver',\n" +
                        " 'url' = '" + url + "',\n" +

                        " 'table-name' = '" + ckTable + "'\n" +
                        ")";

        tableEnv.executeSql(sinkDDL);*/

        String transformDmlSQL =  "select * from mysql_user";

        Table tableResult = tableEnv.sqlQuery(transformDmlSQL);
        DataStream<Row> rows = tableEnv.toChangelogStream(tableResult);
        rows.addSink(new RichSinkFunction<Row>() {
            private Connection conn = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                if(conn == null) {
                    Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                    conn = DriverManager.getConnection("jdbc:clickhouse://192.168.20.250:8123/test");
                }

            }

            @Override
            public void close() throws Exception {
                super.close();
                if(conn != null) {
                    conn.close();
                }
            }

            @Override
            public void invoke(Row row, Context context) throws Exception {
                String sql = "";
                PreparedStatement ps = null;

                int id = (int)row.getField("id");
                LocalDateTime createdAt = (LocalDateTime) row.getField("created_at");
                Timestamp createdAtTimestamp = Timestamp.valueOf(createdAt);
                RowKind rowKind = row.getKind();
                byte kind = rowKind.toByteValue();

                if(kind == 0) {
                    sql = "insert into user values(?, ?)";
                    ps = conn.prepareStatement(sql);
                    ps.setInt(1, id);
                    ps.setTimestamp(2, createdAtTimestamp);
                }

                if(kind == 1) {
                    return;
                }

                if(kind == 2) {
                    sql = "alter table user_shade on cluster flash_cluster update created_at = ? where id = ?";
                    ps = conn.prepareStatement(sql);
                    ps.setTimestamp(1, createdAtTimestamp);
                    ps.setInt(2, id);
                }

                if(kind == 3) {
                    sql = "alter table user_shade on cluster flash_cluster delete where id = ?";
                    ps = conn.prepareStatement(sql);
                    ps.setInt(1, id);
                }

                ps.execute();

                if(ps != null) {
                    ps.close();
                }

            }
        });

        senv.execute("Mysq2ClickHouse");

    }
}
