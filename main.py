import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


def run():
    opt = PipelineOptions(
        temp_location="gs://york_jimt/tmp/",
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york_jimt/staging",
        job_name="jimt-cap-test-run",
        save_main_session=True
    )

    new_schema = {
        'fields': [
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    out_table1 = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="j_thomson_cap_test_run",
        tableId="cap_test_join"
    )

    with beam.Pipeline(runner="DataflowRunner", options=opt) as pipeline:
        # read in BigQuery Tables
        data1 = pipeline | "ReadFromBigQuery1" >> beam.io.ReadFromBigQuery(
            query="SELECT table1.name, table2.last_name FROM `york-cdf-start.bigquerypython.bqtable1` as table1 " \
                  "JOIN `york-cdf-start.bigquerypython.bqtable4` as table2 ON table1.order_id = table2.order_id",
            use_standard_sql=True
        )

        data1 | "Write" >> beam.io.WriteToBigQuery(
            out_table1,
            schema=new_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://york_jimt/tmp"
        )


if __name__ == '__main__':
    run()
