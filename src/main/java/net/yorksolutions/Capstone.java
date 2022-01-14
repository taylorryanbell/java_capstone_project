package net.yorksolutions;

/*
 * Capstone Project in Java
 * Author: Taylor Bell
 * For York Solutions
 * Barriers 2 Entry Program
 * 2022.01.13
 */

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;

public class Capstone {

    public static void main(String[] args) {

        // setting pipeline options to use Google Dataflow Runner
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("final-taylor-bell-java-jenkins");
        pipelineOptions.setProject("york-cdf-start");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setRunner(DataflowRunner.class);

        // make pipeline
        Pipeline p = Pipeline.create(pipelineOptions);

        // create table schemas
        TableSchema schema1 = new TableSchema()
            .setFields(Arrays.asList(
                    new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                    new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
                )
            );
        TableSchema schema2 = new TableSchema()
            .setFields(Arrays.asList(
                    new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                    new TableFieldSchema().setName("total_sales_amount").setType("FLOAT").setMode("REQUIRED")
                )
            );

        // read from BigQuery tables
        PCollection<TableRow> rows1 =
            p.apply(
                "Read from BigQuery query 1",
                BigQueryIO.readTableRows()
                    .fromQuery("WITH CTE AS ( " +
                        "SELECT c.CUST_TIER_CODE as cust_tier_code, SKU as sku, COUNT(p.SKU) as total_no_of_product_views " +
                        "FROM `york-cdf-start.final_input_data.product_views` as p " +
                        "JOIN `york-cdf-start.final_input_data.customers` as c ON p.CUSTOMER_ID = c.CUSTOMER_ID " +
                        "GROUP BY sku, cust_tier_code " +
                        "ORDER BY total_no_of_product_views DESC " +
                        ") SELECT cust_tier_code, sku, total_no_of_product_views FROM CTE " +
                        "ORDER BY cust_tier_code, total_no_of_product_views DESC;")
                    .usingStandardSql());
        PCollection<TableRow> rows2 =
            p.apply(
                "Read from BigQuery query 2",
                BigQueryIO.readTableRows()
                    .fromQuery("WITH CTE AS ( " +
                        "SELECT c.CUST_TIER_CODE as cust_tier_code, SKU as sku, SUM(o.ORDER_AMT) as total_sales_amount " +
                        "FROM `york-cdf-start.final_input_data.orders` as o " +
                        "JOIN `york-cdf-start.final_input_data.customers` as c ON o.CUSTOMER_ID = c.CUSTOMER_ID " +
                        "GROUP BY sku, cust_tier_code " +
                        "ORDER BY total_sales_amount DESC " +
                        ") SELECT cust_tier_code, sku, total_sales_amount FROM CTE " +
                        "ORDER BY cust_tier_code, total_sales_amount DESC;")
                    .usingStandardSql());

        // write to BigQuery tables
        rows1.apply(
            "Write to BigQuery 1",
            BigQueryIO.writeTableRows()
                .to(String.format("york-cdf-start:final_taylor_bell.cust_tier_code-sku-total_no_of_product_views"))
                .withSchema(schema1)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        rows2.apply(
            "Write to BigQuery 2",
            BigQueryIO.writeTableRows()
                .to(String.format("york-cdf-start:final_taylor_bell.cust_tier_code-sku-total_sales_amount"))
                .withSchema(schema2)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        // run the pipeline
        p.run().waitUntilFinish();

    }
}
