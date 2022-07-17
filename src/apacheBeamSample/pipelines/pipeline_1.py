import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from apacheBeamSample.utils import parse_line


def run(options: PipelineOptions) -> None:
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read input file"
            >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Parse file" >> beam.Map(parse_line)
            | "Find all transactions with amount greater than 20"
            >> beam.Filter(lambda x: x["transaction_amount"] > 20)
            | "Exclude transaction made before year 2010"
            >> beam.Filter(
                lambda x: x["timestamp"]
                >= datetime.strptime("2010 UTC", "%Y %Z"),
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
            >> beam.Map(
                lambda x: {"date": str(x[0]), "total_amount": str(x[1])}
            )
            | "Format JSONL output" >> beam.Map(json.dumps)
            | "Write file to local output"
            >> beam.io.WriteToText(
                output_path,
                file_name_suffix=".csv",
                header="date,total_amount",
            )
        )


if __name__ == "__main__":
    input_file = (
        "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
    )
    output_path = "output/result"

    pipeline_options = PipelineOptions(
        runner="DirectRunner",
        job_name=f"pipeline-1-{datetime.now()}",
    )

    run(pipeline_options)
