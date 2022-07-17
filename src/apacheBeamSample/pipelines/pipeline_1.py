import csv

from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


def parse_line(element):
    for line in csv.DictReader(
        [element],
        fieldnames=[
            "timestamp",
            "origin",
            "destination",
            "transaction_amount",
        ],
    ):
        formatted_line = {
            "timestamp": datetime.strptime(
                line["timestamp"], "%Y-%m-%d %H:%M:%S %Z"
            ),
            "origin": line["origin"],
            "destination": line["destination"],
            "transaction_amount": float(line["transaction_amount"]),
        }
        return formatted_line


def to_strings(element):
    return [str(v) for v in element.values()]


def run(options):
    with beam.Pipeline(options=options) as p:
        lines = (
            p
            | "Read input file"
            >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Parse file" >> beam.Map(parse_line)
            | "Find all transactions with amount greater than 20"
            >> beam.Filter(lambda x: x["transaction_amount"] > 20)
            | "Exclude transaction made before year 2010"
            >> beam.Filter(
                lambda x: x["timestamp"]
                >= datetime.strptime("2010 UTC", "%Y %Z")
            )
            | "Remove irrelevant columns"
            >> beam.Map(
                lambda x: {
                    "timestamp": x["timestamp"].date(),
                    "transaction_amount": x["transaction_amount"],
                }
            )
            | "Create a keyed pCollection"
            >> beam.Map(
                lambda x: (str(x["timestamp"]), x["transaction_amount"])
            )
            | "Combine amounts per dates" >> beam.CombinePerKey(sum)
            | "Generate csv lines" >> beam.Map(lambda x: f"{x[0]},{x[1]}")
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
