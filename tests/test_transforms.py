from datetime import date
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from sales_pipeline import transforms as T
from sales_pipeline.schemas import PRODUCTS, RAW_CUSTOMERS, RAW_ORDERS


def products_df(spark):
    return spark.createDataFrame(
        [
            ("P1", "Mouse Basic", "Electronics", Decimal("10.00"), 5),
            ("P2", "Novel Basic", "Books", Decimal("20.00"), 5),
            ("P3", "Novel Pro", "Books", Decimal("30.00"), 5),
        ],
        PRODUCTS,
    )


def raw_orders_df(spark, rows):
    return spark.createDataFrame([r + (None,) for r in rows], RAW_ORDERS)


def test_clean_orders_rejects_bad_rows_and_dedups(spark):
    raw = raw_orders_df(
        spark,
        [
            ("O1", "C1", "P1", "2", "2025-01-01 10:00:00", "completed"),
            ("O1", "C1", "P1", "2", "2025-01-01 10:00:00", "completed"),
            ("O2", "", "P1", "1", "2025-01-01 10:00:00", "completed"),
            ("O3", "C1", "P1", "-1", "2025-01-01 10:00:00", "completed"),
            ("O4", "C1", "P1", "abc", "2025-01-01 10:00:00", "completed"),
            ("O5", "C1", "P1", "1", "31/02/2025", "completed"),
            ("O6", "C1", "P404", "1", "2025-01-01 10:00:00", "completed"),
            ("O7", "C1", "P1", "1", "2025-01-01 10:00:00", " COMPLETED "),
        ],
    )
    valid, rejected = T.split_orders(T.label_orders(raw, products_df(spark)))

    assert sorted(r.order_id for r in valid.collect()) == ["O1", "O7"]
    reasons = {r.order_id: r.reject_reason for r in rejected.collect()}
    assert reasons == {
        "O2": "missing_customer_id",
        "O3": "non_positive_quantity",
        "O4": "bad_quantity",
        "O5": "bad_timestamp",
        "O6": "unknown_product",
    }
    o7 = valid.filter("order_id = 'O7'").first()
    assert o7.status == "completed"
    assert o7.order_month == "2025-01"


def test_clean_customers_normalises_fields(spark):
    raw = spark.createDataFrame(
        [("C1", "Anna", " Anna@Example.COM ", " ua ", "2025-02-30"),
         ("C1", "Anna", "anna@example.com", "UA", "2025-01-01")],
        RAW_CUSTOMERS,
    )
    rows = T.clean_customers(raw).collect()
    assert len(rows) == 1
    assert rows[0].country == "UA"
    assert rows[0].email == "anna@example.com"


def enriched_df(spark):
    orders = spark.createDataFrame(
        [
            Row(order_id="O1", customer_id="C1", product_id="P1", quantity=3,
                order_ts="2025-01-01 10:00:00", status="completed"),
            Row(order_id="O2", customer_id="C1", product_id="P2", quantity=1,
                order_ts="2025-02-01 10:00:00", status="completed"),
            Row(order_id="O3", customer_id="C2", product_id="P3", quantity=2,
                order_ts="2025-02-05 10:00:00", status="completed"),
            Row(order_id="O4", customer_id="C2", product_id="P2", quantity=9,
                order_ts="2025-02-06 10:00:00", status="cancelled"),
        ]
    ).withColumn("order_ts", F.to_timestamp("order_ts")).withColumns(
        {"order_date": F.to_date("order_ts"), "order_month": F.date_format("order_ts", "yyyy-MM")}
    )
    customers = spark.createDataFrame(
        [("C1", "UA", date(2024, 1, 1)), ("C2", "PL", date(2024, 1, 1))],
        "customer_id string, country string, signup_date date",
    )
    return T.enrich_orders(orders, customers, products_df(spark))


def test_enrich_computes_line_revenue(spark):
    rev = {r.order_id: r.line_revenue for r in enriched_df(spark).collect()}
    assert rev["O1"] == Decimal("30.00")
    assert rev["O3"] == Decimal("60.00")


def test_order_facts_one_row_per_order(spark):
    facts = {r.order_id: r for r in T.order_facts(enriched_df(spark)).collect()}
    assert set(facts) == {"O1", "O2", "O3", "O4"}
    assert facts["O1"].order_total == Decimal("30.00")
    assert facts["O1"].lines == 1


def test_daily_revenue_counts_completed_orders(spark):
    days = {r.order_date: r for r in T.daily_revenue(T.order_facts(enriched_df(spark))).collect()}
    assert date(2025, 2, 6) not in days
    assert days[date(2025, 2, 5)].revenue == Decimal("60.00")
    assert days[date(2025, 2, 5)].orders == 1


def test_label_orders_marks_every_row(spark):
    raw = raw_orders_df(
        spark,
        [
            ("O1", "C1", "P1", "1", "2025-01-01 10:00:00", "completed"),
            ("O2", "C1", "P1", "1", "2025-01-01 10:00:00", "lost"),
        ],
    )
    reasons = {r.order_id: r.reject_reason for r in T.label_orders(raw, products_df(spark)).collect()}
    assert reasons == {"O1": None, "O2": "unknown_status"}


def test_monthly_growth_uses_previous_month(spark):
    books = {
        r.order_month: r
        for r in T.monthly_category_growth(enriched_df(spark)).filter("category = 'Books'").collect()
    }
    assert set(books) == {"2025-02"}
    assert books["2025-02"].revenue == Decimal("80.00")
    assert books["2025-02"].prev_revenue is None


def test_top_products_ranking(spark):
    top = T.top_products_per_category(enriched_df(spark), n=1).collect()
    assert {(r.category, r.product_id) for r in top} == {("Books", "P3"), ("Electronics", "P1")}


def test_running_total(spark):
    rows = T.customer_running_total(T.order_facts(enriched_df(spark))).filter("customer_id = 'C1'") \
        .orderBy("order_seq").collect()
    assert [r.cumulative_spend for r in rows] == [Decimal("30.00"), Decimal("50.00")]
    assert rows[1].days_since_prev_order == 31


def test_rfm_assigns_every_customer_a_segment(spark):
    rfm = {r.customer_id: r for r in T.customer_rfm(T.order_facts(enriched_df(spark))).collect()}
    assert set(rfm) == {"C1", "C2"}
    assert all(r.segment for r in rfm.values())
    assert rfm["C2"].recency_days == 1
    assert rfm["C1"].recency_days == 5


def test_return_rate_by_category(spark):
    rates = {r.category: r for r in T.return_rate_by_category(enriched_df(spark)).collect()}
    assert rates["Books"].orders == 3
    assert rates["Books"].cancelled == 1
    assert rates["Books"].cancel_rate_pct == 33.33
    assert rates["Electronics"].return_rate_pct == 0.0


def test_unknown_customer_gets_placeholder_country(spark):
    orders = spark.createDataFrame(
        [("O1", "C404", "P1", 1, "completed")],
        "order_id string, customer_id string, product_id string, quantity int, status string",
    )
    customers = spark.createDataFrame([], "customer_id string, country string, signup_date date")
    row = T.enrich_orders(orders, customers, products_df(spark)).first()
    assert row.country == "UNKNOWN"
    assert row.line_revenue == Decimal("10.00")


def test_rfm_scores_rank_bigger_spender_higher(spark):
    rows = [(f"C{i}", "UA", f"O{i}", "completed", date(2025, 1, 1 + i), Decimal(10 * (i + 1))) for i in range(8)]
    facts = spark.createDataFrame(
        rows, "customer_id string, country string, order_id string, status string, order_date date, order_total decimal(12,2)"
    )
    scored = {r.customer_id: r for r in T.customer_rfm(facts).collect()}
    assert scored["C7"].m_score > scored["C0"].m_score
    assert scored["C7"].r_score > scored["C0"].r_score
    assert scored["C7"].recency_days == 1
    assert scored["C0"].recency_days == 8
