from nyc_taxi_etl.gold.loader import GoldLoader


def test_gold_loader_can_be_created() -> None:
    loader = GoldLoader()

    assert loader is not None


def test_gold_loader_exposes_run_method() -> None:
    loader = GoldLoader()

    assert callable(loader.run)
