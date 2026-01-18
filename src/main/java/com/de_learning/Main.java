package com.de_learning;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.concat;


import com.de_learning.utils.Utils;



public final class Main {
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
            .appName("CSV to DB")
            .master("local")
            .getOrCreate();

        System.out.println(spark.getClass());
    }

    public static void main(String[] args) {
        Main app = new Main();
        app.start();
        System.out.println(Utils.rawDataRoot);
    }
}