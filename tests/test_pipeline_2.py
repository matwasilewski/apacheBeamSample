from typing import List

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from apacheBeamSample.pipelines.pipeline_2 import FilterTransactions


@pytest.fixture
def records_before_2010() -> List[str]:
    with open("resources/records_before_2010.csv") as f:
        return f.readlines()


@pytest.fixture
def records_amount_greater_than_20() -> List[str]:
    with open("resources/records_amount_greater_than_20.csv") as f:
        return f.readlines()


@pytest.fixture
def records_aggregate_by_date() -> List[str]:
    with open("resources/records_aggregate_by_date.csv") as f:
        return f.readlines()


@pytest.fixture
def records_filter_and_aggregate() -> List[str]:
    with open("resources/records_filter_and_aggregate.csv") as f:
        return f.readlines()


def test_filter_before_2010(records_before_2010: List[str]) -> None:
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records_before_2010)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    '{"date": "2010-01-01", "total_amount": "200.0"}',
                    '{"date": "2011-01-01", "total_amount": "400.0"}',
                ],
            ),
        )


def test_filter_for_amount_greater_than_20(
    records_amount_greater_than_20: List[str],
) -> None:
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records_amount_greater_than_20)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    '{"date": "2014-01-01", "total_amount": "21.0"}',
                    '{"date": "2015-01-01", "total_amount": "40.0"}',
                    '{"date": "2016-01-01", "total_amount": "100.0"}',
                ],
            ),
        )


def test_aggregate_by_date(records_aggregate_by_date: List[str]) -> None:
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records_aggregate_by_date)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    '{"date": "2011-01-01", "total_amount": "75.0"}',
                    '{"date": "2012-01-01", "total_amount": "700.0"}',
                    '{"date": "2013-02-01", "total_amount": "2400.0"}',
                    '{"date": "2013-03-01", "total_amount": "9600.0"}',
                    '{"date": "2013-03-02", "total_amount": "38400.0"}',
                ],
            ),
        )


def test_filter_and_aggregate(records_filter_and_aggregate: List[str]) -> None:
    with TestPipeline() as p:
        records_pCollection = p | beam.Create(records_filter_and_aggregate)
        output_pCollection = records_pCollection | FilterTransactions()

        assert_that(
            output_pCollection,
            equal_to(
                [
                    '{"date": "2010-01-01", "total_amount": "300.0"}',
                    '{"date": "2012-01-01", "total_amount": "400.0"}',
                    '{"date": "2012-02-01", "total_amount": "2400.0"}',
                    '{"date": "2015-03-01", "total_amount": "9600.0"}',
                ],
            ),
        )
