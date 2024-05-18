package dis;

import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.ArrayList;
import java.util.List;

import java.util.Collections;
import java.util.concurrent.TimeoutException;


public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("loganalize")
                .master("spark://192.168.200.156:7077")
                .config("spark.es.nodes", "192.168.200.156") // Elasticsearch 호스트 설정
                .config("spark.es.port", "9200") // Elasticsearch 포트 설정
                .config("spark.es.nodes.wan.only", "true") // WAN 환경에서만 Elasticsearch에 연결
                .getOrCreate();

        // 카프카 브로커 서버
        String bootstrapServers = "192.168.200.156:9092";
        // 입력 토픽
        String topic = "spark";

        // 시작 오프셋 설정
        String startingOffsets = "latest"; // 현재 메시지부터 읽기

        String checkpointLocation = "./checkpoint"; // 변경 필요

        // 스트리밍 처리를 위한 DataFrame 생성
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", startingOffsets) // 시작 오프셋 설정
                .load();


        // 스트리밍으로 받아온 데이터 전처리
        Dataset<Row> filteredLines = lines
                .selectExpr("CAST(value AS STRING) as log")
                .filter("log RLIKE '^\\\\d{4}.*$'");

        StreamingQuery query = filteredLines
                .writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // 각 배치마다 Elasticsearch에 저장
                    if (!batchDF.isEmpty()) {
                        batchDF.write().format("org.elasticsearch.spark.sql")
                                .option("es.nodes", "localhost")
                                .option("es.port", "9200")
                                .option("es.resource", "logs")
                                .mode("append")
                                .save();
                    }
                })
                .start();

        // 스트리밍 쿼리 실행
        query.awaitTermination();

    }
}
