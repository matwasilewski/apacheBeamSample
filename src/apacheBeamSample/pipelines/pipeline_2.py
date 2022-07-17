from datetime import datetime

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

from apacheBeamSample.utils import parse_line


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
                "timestamp": x["timestamp"].date(),
                "transaction_amount": x["transaction_amount"],
            },
        )
        | "Create a keyed pCollection"
        >> beam.Map(lambda x: (str(x["timestamp"]), x["transaction_amount"]))
        | "Combine amounts per dates" >> beam.CombinePerKey(sum)
        | "Generate csv lines" >> beam.Map(lambda x: f"{x[0]},{x[1]}")
    )


def run(options: PipelineOptions) -> None:
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read input file"
            >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Lorem ipsum " >> FilterTransactions()
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
        job_name=f"pipeline-2-{datetime.now()}",
    )

    run(pipeline_options)
