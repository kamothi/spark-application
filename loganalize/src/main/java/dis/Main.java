package dis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Collections;
import java.util.concurrent.TimeoutException;


public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("loganalize")
                .master("spark://localhost:7077")
                .config("spark.es.nodes", "localhost") // Elasticsearch 호스트 설정
                .config("spark.es.port", "9200") // Elasticsearch 포트 설정
                .config("spark.es.nodes.wan.only", "true") // WAN 환경에서만 Elasticsearch에 연결
                .getOrCreate();

        // 카프카 브로커 서버
        String bootstrapServers = "localhost:9092";
        // 입력 토픽
        String topic = "spark";

        // 시작 오프셋 설정
        String startingOffsets = "latest"; // 현재 메시지부터 읽기


        // 스트리밍 처리를 위한 DataFrame 생성
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", startingOffsets) // 시작 오프셋 설정
                .load();



        Dataset<Row> parsedLogs = lines
                .selectExpr("CAST(value AS STRING) as log")
                .filter("log RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}.*$'")
                .selectExpr(
                        "substring(log, 1, 23) as timestamp",
                        "substring(log, 26, 4) as logLevel",
                        "IF(regexp_extract(log, 'User ID: (.*?),', 1) = '', NULL, regexp_extract(log, 'User ID: (.*?),', 1)) as userId",
                        "IF(regexp_extract(log, 'Client IP: (.*?),', 1) = '', NULL, regexp_extract(log, 'Client IP: (.*?),', 1)) as clientIp",
                        "IF(regexp_extract(log, 'Request URL: (.*?),', 1) = '', NULL, regexp_extract(log, 'Request URL: (.*?),', 1)) as requestUrl"
                );


        Dataset<Row> logsWithSQLInjection = parsedLogs
                .withColumn("isSQLInjection", detectSQLInjection(parsedLogs.col("requestUrl")));

        // 유효하지 않은 로그 형식에 맞는 데이터를 필터링
        Dataset<Row> invalidLogs = lines
                .selectExpr("CAST(value AS STRING) as log")
                .filter("NOT log RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}.*$'");



        // 유효한 로그(외부 api 요청으로 발생하는 로그)
        StreamingQuery validQuery = logsWithSQLInjection
                .writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // 각 배치마다 Elasticsearch에 저장
                    if (!batchDF.isEmpty()) {
                        batchDF.show(); // 배치 데이터를 콘솔에 출력
                        batchDF.write().format("org.elasticsearch.spark.sql")
                                .option("es.nodes", "localhost")
                                .option("es.port", "9200")
                                .option("es.resource", "logs_valid")
                                .option("es.index.auto.create", "true")
                                .option("es.mapping.id","timestamp")
                                .option("es.mapping.date.rich", "false")
                                .mode("append")
                                .save();
                    }
                })
                .start();



        // 유효하지 않은 로그(내부 동작으로 발생하는 로그)
        StreamingQuery invalidQuery = invalidLogs
                .writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // 각 배치마다 Elasticsearch에 저장
                    if (!batchDF.isEmpty()) {
                        batchDF.show(); // 배치 데이터를 콘솔에 출력
                        batchDF.write().format("org.elasticsearch.spark.sql")
                                .option("es.nodes", "localhost")
                                .option("es.port", "9200")
                                .option("es.resource", "logs_invalid")
                                .option("es.index.auto.create", "true")
                                .mode("append")
                                .save();
                    }
                })
                .start();

        // 스트리밍 쿼리 실행
        validQuery.awaitTermination();
        invalidQuery.awaitTermination();

    }

    private static Column detectSQLInjection(Column col) {
        // 사용자 입력에서 잠재적인 SQL 인젝션 문자를 판별합니다.
        // 작은따옴표(')의 유무를 확인합니다.
        Column containsSingleQuote = functions.regexp_extract(col, "'", 0).notEqual("");

        // 세미콜론(;)의 유무를 확인합니다.
        Column containsSemicolon = functions.regexp_extract(col, ";", 0).notEqual("");

        // 주석문의 유무를 확인합니다.
        Column containsComment = functions.regexp_extract(col, "--", 0).notEqual("");

        // UNION 등의 SQL 키워드의 유무를 확인합니다.
        Column containsSQLKeyword = functions.regexp_extract(col, "(?i)\\bUNION\\b", 0).notEqual("");

        // SQL 인젝션 여부를 나타내는 열을 만듭니다.
        Column isSQLInjection = containsSingleQuote.or(containsSemicolon).or(containsComment).or(containsSQLKeyword);

        return isSQLInjection;
    }


}
