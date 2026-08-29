from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nyc_taxi_etl.database import engine

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_kpis() -> dict[str, float]:
    """Load the main dashboard KPI values from the Gold fact table."""

    query = text(
        """
        SELECT
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            (
                SELECT
                    SUM(fare_amount)
                    / NULLIF(SUM(trip_distance), 0)
                FROM gold.fact_taxi_trips
                WHERE trip_distance > 0
            ) AS average_fare_per_mile
        FROM gold.fact_taxi_trips
        """
    )

    with engine.connect() as connection:
        row = connection.execute(query).mappings().one()

    return {
        "total_trips": float(row["total_trips"]),
        "total_revenue": float(row["total_revenue"]),
        "average_fare_per_mile": float(row["average_fare_per_mile"]),
    }


@st.cache_data(ttl=300)
def load_trips_by_hour() -> pd.DataFrame:
    """Load taxi trip counts grouped by pickup hour."""

    query = text(
        """
        SELECT
            pickup_hour,
            COUNT(*) AS trip_count
        FROM gold.fact_taxi_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
        """
    )

    return pd.read_sql(query, engine)


@st.cache_data(ttl=300)
def load_revenue_by_payment_type() -> pd.DataFrame:
    """Load total taxi revenue grouped by payment type."""

    query = text(
        """
        SELECT
            payment.payment_type_name,
            SUM(fact.total_amount) AS total_revenue
        FROM gold.fact_taxi_trips AS fact
        INNER JOIN gold.dim_payment_type AS payment
            ON fact.payment_type_key = payment.payment_type_key
        GROUP BY payment.payment_type_name
        ORDER BY total_revenue DESC
        """
    )

    return pd.read_sql(query, engine)


def main() -> None:
    """Render the NYC Yellow Taxi analytics dashboard."""

    st.title("🚕 NYC Yellow Taxi Analytics")

    st.caption("January–February 2023 | PostgreSQL Gold Layer")

    try:
        kpis = load_kpis()
        trips_by_hour = load_trips_by_hour()
        revenue_by_payment = load_revenue_by_payment_type()

    except SQLAlchemyError as exc:
        st.error("Unable to load dashboard data from PostgreSQL.")
        st.exception(exc)
        return

    # ---------------------------------------------------------
    # KPI section
    # ---------------------------------------------------------

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Trips",
        f"{int(kpis['total_trips']):,}",
    )

    col2.metric(
        "Total Revenue",
        f"${kpis['total_revenue']:,.2f}",
    )

    col3.metric(
        "Average Fare / Mile",
        f"${kpis['average_fare_per_mile']:,.2f}",
    )

    st.divider()

    # ---------------------------------------------------------
    # Charts
    # ---------------------------------------------------------

    left, right = st.columns(2)

    with left:
        st.subheader("Trips by Pickup Hour")

        hourly_chart = trips_by_hour.set_index("pickup_hour")

        st.bar_chart(
            hourly_chart["trip_count"],
            width="stretch",
        )

    with right:
        st.subheader("Revenue by Payment Type")

        payment_chart = revenue_by_payment.set_index("payment_type_name")

        st.bar_chart(
            payment_chart["total_revenue"],
            width="stretch",
        )

    st.divider()

    st.caption("Source: NYC Taxi & Limousine Commission Yellow Taxi Trip Records")


if __name__ == "__main__":
    main()
