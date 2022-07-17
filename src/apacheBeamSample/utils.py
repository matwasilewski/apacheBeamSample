import csv
from datetime import datetime


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
