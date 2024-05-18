package dis;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;


public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("loganalize")
                .master("spark://192.168.200.156:7077")
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


        lines.printSchema();


        // 스트리밍으로 받아온 데이터 전처리
        Dataset<Row> filteredLines = lines
                .selectExpr("CAST(value AS STRING) as log")
                .filter("log RLIKE '^\\\\d{4}.*$'");

        StreamingQuery consoleQuery = filteredLines
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        // 콘솔에 출력하는 예시
        StreamingQuery query = filteredLines
                .writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", "./test")  // CSV 파일이 저장될 경로 지정
                .option("failOnDataLoss", "false")
                .option("checkpointLocation", checkpointLocation)  // 체크포인트 위치 설정
                .start();

        consoleQuery.awaitTermination();
        query.awaitTermination();
    }
}
