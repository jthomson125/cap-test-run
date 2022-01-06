import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class ChangeData(beam.DoFn):
    def process(self, element):
        # print(element)
        # print(type(element))

        # print(element["name"])
        # element["name"] = (element["name"] + " New")
        # print(element["name"])
        # yield element

        yield element


def run():
    opt = PipelineOptions(
        temp_location="gs://york_jimt/tmp/",
        project="york-cdf-start",
        region="us-central1",
        staging_location="gs://york_jimt/staging",
        job_name="jimt-cap-test-run",
        save_main_session=True
    )

    schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

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
    table1 = "york-cdf-start.bigquerypython.bqtable1"
    table2 = "york-cdf-start.bigquerypython.bqtable4"

    table2 = bigquery.TableReference()

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

        pass


if __name__ == '__main__':
    print("Hello Jenkins")
    run()
    pass
