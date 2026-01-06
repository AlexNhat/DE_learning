import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {
        Main app = new Main();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
        .appName("CSV to DB")
        .master("local")
        .getOrCreate();

        System.out.println(spark.getClass());
    }
}