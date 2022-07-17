import json
from datetime import datetime

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions

from apacheBeamSample.utils import parse_line


def format_output(date, amount):
    return {"date": date, "total_amount": amount}


@beam.ptransform_fn
def FilterTransactions(pcoll: PCollection) -> PCollection:
    return (
        pcoll
        | "Parse file" >> beam.Map(parse_line)
        | "Find all transactions with amount greater than 20"
        >> beam.Filter(lambda x: x["transaction_amount"] > 20)
        | "Exclude transaction made before year 2010"
        >> beam.Filter(
            lambda x: x["timestamp"] >= datetime.strptime("2010 UTC", "%Y %Z"),
        )
        | "Remove irrelevant columns"
        >> beam.Map(
            lambda x: {
                "date": x["timestamp"].date(),
                "transaction_amount": x["transaction_amount"],
            },
        )
        | "Group by date and aggregate transaction amounts"
        >> beam.GroupBy(lambda x: x["date"]).aggregate_field(
            lambda y: y["transaction_amount"], sum, "total_amount"
        )
        | "Generate dictionary"
        >> beam.Map(lambda x: {"date": str(x[0]), "total_amount": str(x[1])})
        | "Format JSONL output" >> beam.Map(json.dumps)
    )


def run(options: PipelineOptions) -> None:
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read input file"
            >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Filter Transactions " >> FilterTransactions()
            | "Write file to local output"
            >> beam.io.WriteToText(
                output_path,
                file_name_suffix=".jsonl.gz",
                compression_type=CompressionTypes.GZIP,
            )
        )


if __name__ == "__main__":
    input_file = (
        "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
    )
    output_path = "output/result"

    pipeline_options = PipelineOptions(
        runner="DirectRunner",
        job_name=f"pipeline-2-{datetime.now()}",
    )

    run(pipeline_options)
