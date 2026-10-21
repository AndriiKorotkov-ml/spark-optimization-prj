import pytest

from sales_pipeline.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests", master="local[2]")
    yield session
    session.stop()
