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
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // 카프카 브로커 서버
        String bootstrapServers = "localhost:9092";
        // 입력 토픽
        String topic = "spark";

        // 스트리밍 처리를 위한 DataFrame 생성
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        // 콘솔에 출력하는 예시
        StreamingQuery query = lines
                .writeStream()
                .outputMode("append")
                .format("console") // 콘솔에 출력
                .start();

        query.awaitTermination();
    }
}
