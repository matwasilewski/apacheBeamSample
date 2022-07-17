import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pytest

from apacheBeamSample.pipelines.pipeline_2 import FilterTransactions


def test_filter_before_2010():
    records = [
        "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,100",
        "2010-01-01 00:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,200",
        "2011-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,400",
    ]
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    "2010-01-01,200.0",
                    "2011-01-01,400.0",
                ]
            ),
        )
