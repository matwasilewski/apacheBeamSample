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


def test_filter_for_amount_greater_than_20():
    records = [
        "2010-01-01 00:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,5",
        "2011-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,10",
        "2012-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,19",
        "2013-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,20",
        "2014-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,21",
        "2015-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,40",
        "2016-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,100",
    ]
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    "2014-01-01,21.0",
                    "2015-01-01,40.0",
                    "2016-01-01,100.0",
                ]
            ),
        )


def test_aggregate_by_date():
    records = [
        "2011-01-01 10:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,25",
        "2011-01-01 11:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,50",
        "2012-01-01 12:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,100",
        "2012-01-01 13:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,200",
        "2012-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,400",
        "2013-02-01 15:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,800",
        "2013-02-01 16:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,1600",
        "2013-03-01 17:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,3200",
        "2013-03-01 18:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,6400",
        "2013-03-02 19:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,12800",
        "2013-03-02 20:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,25600",
    ]
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    "2011-01-01,75.0",
                    "2012-01-01,700.0",
                    "2013-02-01,2400.0",
                    "2013-03-01,9600.0",
                    "2013-03-02,38400.0",
                ]
            ),
        )


def test_filter_and_aggregate():
    records = [
        "2008-01-01 10:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,25",
        "2009-01-01 11:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,50",
        "2010-01-01 12:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,100",
        "2010-01-01 13:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,200",
        "2012-01-01 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,400",
        "2012-02-01 15:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,800",
        "2012-02-01 16:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,1600",
        "2015-03-01 17:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,3200",
        "2015-03-01 18:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,6400",
        "2003-03-02 19:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,12800",
        "2002-03-02 20:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,25600",
    ]
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    "2010-01-01,300.0",
                    "2012-01-01,400.0",
                    "2012-02-01,2400.0",
                    "2015-03-01,9600.0",
                ]
            ),
        )
