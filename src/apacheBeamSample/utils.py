import csv
from datetime import datetime
from typing import Dict, List, Optional


def parse_line(
    element: str,
) -> Optional[Dict[str, object]]:
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
                line["timestamp"],
                "%Y-%m-%d %H:%M:%S %Z",
            ),
            "origin": line["origin"],
            "destination": line["destination"],
            "transaction_amount": float(line["transaction_amount"]),
        }
        return formatted_line
    return None


def to_strings(element: Dict) -> List[str]:
    return [str(v) for v in element.values()]
