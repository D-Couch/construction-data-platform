"""Spark and Delta implementation of the local Milestone 1 pipeline."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

FEEDS = ("purchase_order_lines", "scale_tickets", "site_receipts", "invoices", "invoice_lines", "invoice_delivery_allocations")
CONVERSION_TO_KG = {"kg": "1", "metric_tonne": "1000", "us_short_ton": "907.18474"}
KEYS = {
    "purchase_order_lines": ["company_id", "po_id", "po_line_id"],
    "scale_tickets": ["company_id", "scale_system_id", "ticket_id"],
    "site_receipts": ["company_id", "receipt_id"],
    "invoices": ["company_id", "supplier_id", "invoice_id"],
    "invoice_lines": ["company_id", "supplier_id", "invoice_id", "invoice_line_id"],
    "invoice_delivery_allocations": ["company_id", "supplier_id", "invoice_id", "invoice_line_id", "allocation_id"],
}


def create_spark(app_name: str = "construction-data-platform") -> SparkSession:
    project_root = Path(__file__).resolve().parents[2]
    builder = SparkSession.builder.appName(app_name)
    if "DATABRICKS_RUNTIME_VERSION" in __import__("os").environ:
        # Databricks Runtime supplies Spark and Delta; do not override managed compute settings.
        spark = builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        builder = (builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.jars.ivy", str(project_root / ".cache" / "ivy"))
            .config("spark.local.dir", str(project_root / ".cache" / "spark-tmp"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def _input_files(landing: Path) -> list[Path]:
    # Strict allowlist keeps independent expected fixtures out of all pipeline reads.
    paths = []
    for feed in FEEDS:
        paths.extend(sorted(landing.glob(f"{feed}_*.csv")))
    return paths


def _read_files(spark: SparkSession, landing: Path, run_id: str):
    rows = {name: [] for name in FEEDS}
    now = datetime.now(UTC).isoformat()
    for path in _input_files(landing):
        content = path.read_bytes()
        content_hash = hashlib.sha256(content).hexdigest()
        frame = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
        business_columns = frame.columns
        frame = (frame.withColumn("source_file", F.lit(path.name))
            .withColumn("file_content_hash", F.lit(content_hash))
            .withColumn("ingested_at", F.lit(now))
            .withColumn("ingestion_batch_id", F.lit(run_id)))
        frame = frame.withColumn("payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_columns])), 256))
        feed = next(name for name in FEEDS if path.name.startswith(name + "_"))
        rows[feed].append(frame)
    return rows


def _union(frames):
    if not frames:
        return None
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def _write_delta(df, path: Path, mode: str = "overwrite"):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(str(path))


def _read_delta(spark, path: Path):
    return spark.read.format("delta").load(str(path))


def run_pipeline(root: Path, spark: SparkSession | None = None, fail_after_product: str | None = None) -> dict[str, object]:
    root = root.resolve()
    landing, store = root / "landing", root / "tables"
    if not landing.exists():
        raise FileNotFoundError(f"Synthetic feed landing folder does not exist: {landing}")
    owns_spark = spark is None
    spark = spark or create_spark()
    run_id = str(uuid.uuid4())
    started = datetime.now(UTC).isoformat()
    ingest = _read_files(spark, landing, run_id)
    counts = {}
    duplicates = 0
    raw_frames = {}
    for name in FEEDS:
        raw = _union(ingest[name])
        raw_path = store / "bronze" / name
        if raw_path.exists():
            previous = _read_delta(spark, raw_path)
            raw = previous if raw is None else previous.unionByName(raw, allowMissingColumns=True)
        if raw is None:
            continue
        before_retry_dedup = raw.count()
        raw = raw.dropDuplicates(["source_file", "file_content_hash", "payload_hash"])
        deduplicated_count = raw.count()
        duplicates += before_retry_dedup - deduplicated_count
        raw_frames[name] = raw
        _write_delta(raw, raw_path)
        counts[name] = deduplicated_count

    def current(name: str):
        nonlocal duplicates
        df = raw_frames.get(name)
        if df is None:
            return None
        keys = KEYS[name]
        w = Window.partitionBy(*keys).orderBy(F.col("source_revision").cast("long").desc(), F.col("source_updated_at").desc(), F.col("payload_hash").asc())
        technical = df.dropDuplicates([*keys, "source_revision", "payload_hash"])
        duplicates += df.count() - technical.count()
        conflict = technical.groupBy(*keys, "source_revision").agg(F.countDistinct("payload_hash").alias("n")).filter("n > 1")
        if conflict.limit(1).count():
            raise ValueError(f"Conflicting equal revision in {name}; no deterministic winner is publishable")
        history_path = store / "silver" / f"{name}_history"
        _write_delta(technical, history_path)
        latest = technical.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")
        _write_delta(latest, store / "silver" / f"{name}_current")
        return latest

    po, ticket, receipt = (current(n) for n in ("purchase_order_lines", "scale_tickets", "site_receipts"))
    invoices, lines, allocations = (current(n) for n in ("invoices", "invoice_lines", "invoice_delivery_allocations"))
    quarantined = []
    if ticket is not None:
        unit_ok = F.col("weight_unit").isin(*CONVERSION_TO_KG)
        gross = F.expr("try_cast(gross_weight AS DECIMAL(18,6))")
        tare = F.expr("try_cast(tare_weight AS DECIMAL(18,6))")
        numeric_ok = gross.isNotNull() & tare.isNotNull()
        ticket = ticket.withColumn(
            "quarantine_reason",
            F.when(F.col("weight_unit").isNull() | ~unit_ok, "UNKNOWN_WEIGHT_UNIT")
            .when(~numeric_ok, "INVALID_WEIGHT")
            .when(F.try_to_timestamp(F.col("weighed_at")).isNull(), "INVALID_EVENT_TIMESTAMP")
            .when((gross < 0) | (tare < 0), "NEGATIVE_WEIGHT")
            .when(gross < tare, "GROSS_BELOW_TARE"),
        )
        quarantined.append(ticket.filter(F.col("quarantine_reason").isNotNull()).withColumn("feed", F.lit("scale_tickets")))
        ticket = ticket.filter(F.col("quarantine_reason").isNull()).withColumn("net_weight_original", gross - tare)
        # Unit-specific exact conversions to tonnes.
        factor = F.when(F.col("weight_unit") == "kg", F.lit(Decimal("0.001"))).when(F.col("weight_unit") == "us_short_ton", F.lit(Decimal("0.90718474"))).otherwise(F.lit(Decimal(1)))
        ticket = ticket.withColumn("net_metric_tonnes", (F.col("net_weight_original") * factor).cast("decimal(18,6)"))
        _write_delta(ticket, store / "silver" / "scale_tickets_current")

    if po is not None:
        po = po.withColumn("ordered_quantity_decimal", F.expr("try_cast(ordered_quantity AS DECIMAL(18,6))")).withColumn("unit_price_decimal", F.expr("try_cast(unit_price AS DECIMAL(18,6))"))
        _write_delta(po, store / "silver" / "purchase_order_lines_current")
    if receipt is not None:
        receipt = receipt.withColumn("_accepted_fraction_decimal", F.expr("try_cast(accepted_fraction AS DECIMAL(9,6))"))
        receipt = receipt.withColumn(
            "quarantine_reason",
            F.when(F.try_to_timestamp(F.col("received_at")).isNull(), "INVALID_RECEIVED_TIMESTAMP")
            .when(F.col("acceptance_status").isNull() | ~F.col("acceptance_status").isin("accepted", "rejected", "pending"), "INVALID_ACCEPTANCE_STATUS")
            .when(F.col("_accepted_fraction_decimal").isNull() | (F.col("_accepted_fraction_decimal") < 0) | (F.col("_accepted_fraction_decimal") > 1), "INVALID_ACCEPTED_FRACTION")
            .when((F.col("acceptance_status") == "accepted") & (F.col("_accepted_fraction_decimal") != 1), "UNSUPPORTED_FRACTIONAL_ACCEPTANCE"),
        )
        quarantined.append(receipt.filter(F.col("quarantine_reason").isNotNull()).withColumn("feed", F.lit("site_receipts")))
        receipt = receipt.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason", "_accepted_fraction_decimal")
        _write_delta(receipt, store / "silver" / "site_receipts_current")

    if lines is not None:
        line_amount = F.expr("try_cast(line_amount AS DECIMAL(18,2))")
        billed_quantity = F.expr("try_cast(billed_quantity AS DECIMAL(18,6))")
        unit_price = F.expr("try_cast(unit_price AS DECIMAL(18,6))")
        lines = lines.withColumn(
            "quarantine_reason",
            F.when(F.col("quantity_unit").isNull() | ~F.col("quantity_unit").isin(*CONVERSION_TO_KG), "UNKNOWN_INVOICE_QUANTITY_UNIT")
            .when(F.col("price_unit").isNull() | ~F.col("price_unit").isin(*CONVERSION_TO_KG), "UNKNOWN_INVOICE_PRICE_UNIT")
            .when(billed_quantity.isNull() | unit_price.isNull() | line_amount.isNull(), "INVALID_INVOICE_AMOUNT_OR_QUANTITY")
            .when((billed_quantity < 0) | (unit_price < 0) | (line_amount < 0), "NEGATIVE_INVOICE_AMOUNT_OR_QUANTITY"),
        )
        quarantined.append(lines.filter(F.col("quarantine_reason").isNotNull()).withColumn("feed", F.lit("invoice_lines")))
        lines = lines.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
        _write_delta(lines, store / "silver" / "invoice_lines_current")

    if allocations is not None:
        allocated_quantity = F.expr("try_cast(allocated_quantity AS DECIMAL(18,6))")
        allocations = allocations.withColumn(
            "quarantine_reason",
            F.when(F.col("quantity_unit").isNull() | ~F.col("quantity_unit").isin(*CONVERSION_TO_KG), "UNKNOWN_ALLOCATION_UNIT")
            .when(allocated_quantity.isNull(), "INVALID_ALLOCATION_QUANTITY")
            .when(allocated_quantity < 0, "NEGATIVE_ALLOCATION_QUANTITY"),
        )
        quarantined.append(allocations.filter(F.col("quarantine_reason").isNotNull()).withColumn("feed", F.lit("invoice_delivery_allocations")))
        allocations = allocations.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
        _write_delta(allocations, store / "silver" / "invoice_delivery_allocations_current")

    # A missing receipt is a normal late-arrival state. Keep ticket and invoice
    # evidence flowing so reporting can show pending support until it arrives.
    if receipt is None:
        receipt = spark.createDataFrame([], T.StructType([
            T.StructField("company_id", T.StringType()),
            T.StructField("delivery_id", T.StringType()),
            T.StructField("received_at", T.StringType()),
            T.StructField("acceptance_status", T.StringType()),
            T.StructField("accepted_fraction", T.StringType()),
            T.StructField("project_id", T.StringType()),
        ]))

    if ticket is not None:
        if po is None:
            po_schema = T.StructType([
                T.StructField("company_id", T.StringType()),
                T.StructField("po_id", T.StringType()),
                T.StructField("po_line_id", T.StringType()),
                T.StructField("project_id", T.StringType()),
                T.StructField("supplier_id", T.StringType()),
                T.StructField("material_id", T.StringType()),
                T.StructField("unit_price", T.StringType()),
                T.StructField("price_unit", T.StringType()),
                T.StructField("currency", T.StringType()),
                T.StructField("effective_at", T.StringType()),
                T.StructField("source_revision", T.StringType()),
                T.StructField("payload_hash", T.StringType()),
            ])
            po = spark.createDataFrame([], po_schema)
            po_history = po
        else:
            po_history = _read_delta(spark, store / "silver" / "purchase_order_lines_history")
        delivery = ticket.alias("t").join(
            receipt.alias("r"),
            (F.col("t.company_id") == F.col("r.company_id"))
            & (F.col("t.delivery_id") == F.col("r.delivery_id")),
            "left",
        )
        delivery = delivery.select("t.*", F.col("r.delivery_id").alias("receipt_delivery_id"), F.col("r.received_at").alias("received_at"), F.col("r.acceptance_status").alias("acceptance_status"), F.col("r.accepted_fraction").alias("accepted_fraction"), F.col("r.project_id").alias("receipt_project_id"))
        effective_po = po_history.alias("p")
        candidates = delivery.alias("d").join(
            effective_po,
            (F.col("d.company_id") == F.col("p.company_id"))
            & (F.col("d.po_id") == F.col("p.po_id"))
            & (F.col("d.po_line_id") == F.col("p.po_line_id"))
            & (F.try_to_timestamp(F.col("p.effective_at")) <= F.try_to_timestamp(F.col("d.weighed_at"))),
            "left",
        )
        po_rank = Window.partitionBy("d.company_id", "d.delivery_id").orderBy(
            F.try_to_timestamp(F.col("p.effective_at")).desc_nulls_last(),
            F.col("p.source_revision").cast("long").desc_nulls_last(),
            F.col("p.payload_hash").asc_nulls_last(),
        )
        candidates = candidates.withColumn("_po_rank", F.row_number().over(po_rank)).filter("_po_rank = 1")
        delivery = candidates.select(
            "d.*",
            F.col("p.po_id").alias("matched_po_id"),
            F.col("p.po_line_id").alias("matched_po_line_id"),
            F.col("p.project_id").alias("po_project_id"),
            F.col("p.supplier_id").alias("po_supplier_id"),
            F.col("p.material_id").alias("po_material_id"),
            F.col("p.unit_price").cast("decimal(18,6)").alias("po_unit_price"),
            F.col("p.price_unit").alias("po_price_unit"),
            F.col("p.currency").alias("po_currency"),
        )
        delivery = delivery.withColumn("project_id", F.coalesce("receipt_project_id", "po_project_id"))
        delivery = delivery.withColumn("delivery_state", F.when(F.col("receipt_delivery_id").isNull(), "PENDING_RECEIPT").when(F.col("acceptance_status") != "accepted", F.concat(F.lit("RECEIPT_"), F.upper(F.col("acceptance_status")))).when(F.col("matched_po_id").isNull(), "UNRESOLVED_PO").otherwise("ACCEPTED"))
        delivery = delivery.withColumn("accepted_net_quantity", F.when(F.col("delivery_state") == "ACCEPTED", F.col("net_metric_tonnes") * F.col("accepted_fraction").cast("decimal(9,6)")))
        _write_delta(delivery, store / "silver" / "delivery_status")
        daily = delivery.filter(F.col("delivery_state") == "ACCEPTED").withColumn("business_date", F.to_date(F.from_utc_timestamp(F.to_timestamp("received_at"), "America/Chicago"))).groupBy("project_id", "material_id", "business_date").agg(F.sum("accepted_net_quantity").cast("decimal(18,6)").alias("accepted_net_quantity"))
        products = [("daily_delivered_quantities", daily)]
    else:
        delivery = None
        products = []

    exception_rows = None
    if lines is not None and allocations is not None and delivery is not None and po is not None:
        line_keys = ["company_id", "supplier_id", "invoice_id", "invoice_line_id"]

        def kilograms_per_unit(unit_column: str):
            unit = F.col(unit_column)
            return (
                F.when(unit == "kg", F.lit(Decimal(1)))
                .when(unit == "metric_tonne", F.lit(Decimal(1000)))
                .when(unit == "us_short_ton", F.lit(Decimal("907.18474")))
            )

        if invoices is None:
            invoice_headers = spark.createDataFrame([], T.StructType([
                T.StructField("company_id", T.StringType()),
                T.StructField("supplier_id", T.StringType()),
                T.StructField("invoice_id", T.StringType()),
                T.StructField("currency", T.StringType()),
            ]))
        else:
            invoice_headers = invoices.select(*line_keys[:-1], "currency")

        line_basis = lines.join(invoice_headers, line_keys[:-1], "left")
        line_basis = (
            line_basis
            .withColumn("billed_quantity_decimal", F.expr("try_cast(billed_quantity AS DECIMAL(18,6))"))
            .withColumn("unit_price_decimal", F.expr("try_cast(unit_price AS DECIMAL(18,6))"))
            .withColumn("line_amount_decimal", F.expr("try_cast(line_amount AS DECIMAL(18,2))"))
            .withColumn("quantity_unit_kg", kilograms_per_unit("quantity_unit"))
            .withColumn("price_unit_kg", kilograms_per_unit("price_unit"))
            .withColumn(
                "billed_quantity_metric_tonnes",
                (F.col("billed_quantity_decimal") * F.col("quantity_unit_kg") / F.lit(Decimal(1000)))
                .cast("decimal(18,6)"),
            )
            .withColumn(
                "invoice_price_metric_tonne",
                (F.col("unit_price_decimal") * F.lit(Decimal(1000)) / F.col("price_unit_kg"))
                .cast("decimal(18,6)"),
            )
        )
        expected_amount = F.round(
            F.col("billed_quantity_decimal")
            * F.col("quantity_unit_kg")
            / F.col("price_unit_kg")
            * F.col("unit_price_decimal"),
            2,
        )
        line_basis = line_basis.withColumn(
            "amount_mismatch",
            F.abs(F.col("line_amount_decimal") - expected_amount) > F.lit(Decimal("0.01")),
        )

        allocation_quantity = F.expr("try_cast(allocated_quantity AS DECIMAL(18,6))")
        allocation_unit_kg = kilograms_per_unit("quantity_unit")
        allocations_normalized = (
            allocations
            .withColumn("allocated_quantity_decimal", allocation_quantity)
            .withColumn("allocation_unit_kg", allocation_unit_kg)
            .withColumn(
                "allocated_quantity_metric_tonnes",
                (F.col("allocated_quantity_decimal") * F.col("allocation_unit_kg") / F.lit(Decimal(1000)))
                .cast("decimal(18,6)"),
            )
            .withColumn(
                "invalid_allocation_unit",
                F.when(F.col("allocation_unit_kg").isNull() | F.col("allocated_quantity_decimal").isNull(), 1)
                .otherwise(0),
            )
        )
        alloc_line = allocations_normalized.groupBy(*line_keys).agg(
            F.sum("allocated_quantity_metric_tonnes").alias("allocated_quantity_metric_tonnes"),
            F.max("invalid_allocation_unit").alias("invalid_allocation_unit"),
        )

        delivery_context = delivery.select(
            "company_id",
            "delivery_id",
            F.col("po_id").alias("delivery_po_id"),
            F.col("po_line_id").alias("delivery_po_line_id"),
            "po_supplier_id",
            "po_material_id",
            F.col("po_unit_price").alias("po_unit_price_decimal"),
            "po_price_unit",
            "po_currency",
            "delivery_state",
            "accepted_net_quantity",
        )
        alloc_detail = (
            allocations_normalized
            .join(
                line_basis.select(
                    *line_keys,
                    F.col("material_id").alias("line_material_id"),
                    "po_id",
                    "po_line_id",
                    "invoice_price_metric_tonne",
                ),
                line_keys,
                "left",
            )
            .join(delivery_context, ["company_id", "delivery_id"], "left")
            .withColumn(
                "po_price_unit_kg",
                kilograms_per_unit("po_price_unit"),
            )
            .withColumn(
                "po_price_metric_tonne",
                (F.col("po_unit_price_decimal") * F.lit(Decimal(1000)) / F.col("po_price_unit_kg"))
                .cast("decimal(18,6)"),
            )
            .withColumn(
                "price_mismatch",
                F.when(
                    F.col("invoice_price_metric_tonne").isNotNull()
                    & F.col("po_price_metric_tonne").isNotNull()
                    & (F.abs(F.col("invoice_price_metric_tonne") - F.col("po_price_metric_tonne")) > F.lit(Decimal("0.01"))),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "supplier_mismatch",
                F.when(F.col("po_supplier_id").isNotNull() & (F.col("supplier_id") != F.col("po_supplier_id")), 1)
                .otherwise(0),
            )
            .withColumn(
                "material_mismatch",
                F.when(F.col("po_material_id").isNotNull() & (F.col("line_material_id") != F.col("po_material_id")), 1)
                .otherwise(0),
            )
            .withColumn(
                "po_line_mismatch",
                F.when(
                    F.col("delivery_po_id").isNotNull()
                    & (
                        (F.col("po_id") != F.col("delivery_po_id"))
                        | (F.col("po_line_id") != F.col("delivery_po_line_id"))
                    ),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                "unresolved_delivery",
                F.when(F.col("delivery_state").isNull() | (F.col("delivery_state") != "ACCEPTED"), 1)
                .otherwise(0),
            )
        )
        # Collapse repeated allocation rows to one invoice-line/delivery reference
        # before summing receipt facts, preventing quantity fan-out.
        delivery_grain = alloc_detail.groupBy(*line_keys, "delivery_id").agg(
            F.max("accepted_net_quantity").alias("accepted_net_quantity"),
            F.max("unresolved_delivery").alias("unresolved_delivery"),
            F.max("price_mismatch").alias("price_mismatch"),
            F.max("supplier_mismatch").alias("supplier_mismatch"),
            F.max("material_mismatch").alias("material_mismatch"),
            F.max("po_line_mismatch").alias("po_line_mismatch"),
        )
        alloc_ref = delivery_grain.groupBy(*line_keys).agg(
            F.sum("accepted_net_quantity").alias("allocated_receipt_quantity"),
            F.max("unresolved_delivery").alias("unresolved_delivery"),
            F.max("price_mismatch").alias("price_mismatch"),
            F.max("supplier_mismatch").alias("supplier_mismatch"),
            F.max("material_mismatch").alias("material_mismatch"),
            F.max("po_line_mismatch").alias("po_line_mismatch"),
        )
        result = line_basis.join(alloc_line, line_keys, "left").join(alloc_ref, line_keys, "left")
        tol = F.greatest(
            F.lit(Decimal("0.01")),
            F.coalesce(F.col("allocated_receipt_quantity"), F.lit(Decimal(0))) * F.lit(Decimal("0.005")),
        )
        result = (
            result
            .withColumn(
                "quantity_difference",
                F.abs(
                    F.coalesce(F.col("allocated_quantity_metric_tonnes"), F.lit(Decimal(0)))
                    - F.coalesce(F.col("allocated_receipt_quantity"), F.lit(Decimal(0)))
                ),
            )
            .withColumn(
                "allocation_billed_difference",
                F.abs(
                    F.coalesce(F.col("allocated_quantity_metric_tonnes"), F.lit(Decimal(0)))
                    - F.coalesce(F.col("billed_quantity_metric_tonnes"), F.lit(Decimal(0)))
                ),
            )
            .withColumn(
                "exception_reason",
                F.when(F.col("allocated_quantity_metric_tonnes").isNull(), "MISSING_ALLOCATION")
                .when(F.col("unresolved_delivery") == 1, "PENDING_SUPPORTING_DELIVERY")
                .when(F.col("currency").isNull(), "MISSING_INVOICE_HEADER")
                .when(F.col("currency") != "USD", "UNSUPPORTED_CURRENCY")
                .when(F.col("billed_quantity_decimal").isNull() | F.col("unit_price_decimal").isNull() | F.col("line_amount_decimal").isNull(), "INVALID_INVOICE_AMOUNT_OR_QUANTITY")
                .when(F.col("quantity_unit_kg").isNull() | F.col("price_unit_kg").isNull(), "UNSUPPORTED_INVOICE_UNIT")
                .when(F.col("invalid_allocation_unit") == 1, "UNSUPPORTED_ALLOCATION_UNIT")
                .when(F.col("supplier_mismatch") == 1, "SUPPLIER_MISMATCH")
                .when(F.col("material_mismatch") == 1, "MATERIAL_MISMATCH")
                .when(F.col("po_line_mismatch") == 1, "PO_LINE_MISMATCH")
                .when(F.col("amount_mismatch"), "LINE_AMOUNT_MISMATCH")
                .when(F.col("price_mismatch") == 1, "UNIT_PRICE_MISMATCH")
                .when(F.col("allocation_billed_difference") > tol, "ALLOCATION_QUANTITY_MISMATCH")
                .when(F.col("quantity_difference") > tol, "QUANTITY_MISMATCH"),
            )
        )
        exception_rows = result.filter(F.col("exception_reason").isNotNull())
        products.append(("invoice_exceptions", exception_rows))
        _write_delta(result, store / "silver" / "invoice_reconciliation")

    quarantine = _union(quarantined)
    if quarantine is not None:
        _write_delta(quarantine, store / "silver" / "quarantine")
    else:
        quarantine = spark.createDataFrame([], T.StructType([T.StructField("quarantine_reason", T.StringType())]))

    # Stage all products under a unique generation, then atomically replace pointer.
    generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + run_id[:8]
    staged = store / "gold" / "generations" / generation
    product_counts = {}
    for name, product in products:
        _write_delta(product, staged / name)
        product_counts[name] = product.count()
        if fail_after_product == name:
            if owns_spark:
                spark.stop()
            return {"run_id": run_id, "status": "FAILED_INJECTED", "generation": generation, "published": False, "products_written": product_counts}
    if exception_rows is None:
        exception_rows = spark.createDataFrame([], T.StructType([T.StructField("exception_reason", T.StringType())]))
        _write_delta(exception_rows, staged / "invoice_exceptions")
        product_counts["invoice_exceptions"] = 0
    # The pointer is the sole consumer contract; no table under gold/latest is read directly.
    pointer = {"generation": generation, "run_id": run_id, "published_at_utc": datetime.now(UTC).isoformat(), "synthetic": True}
    pointer_path = store / "gold" / "current_generation.json"
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    temp = pointer_path.with_suffix(".tmp")
    temp.write_text(json.dumps(pointer, indent=2) + "\n", encoding="utf-8")
    temp.replace(pointer_path)
    report = {"run_id": run_id, "generation": generation, "status": "SUCCESS", "published": True, "synthetic": True, "started_at_utc": started, "finished_at_utc": datetime.now(UTC).isoformat(), "source_rows_by_feed": counts, "duplicate_business_envelopes_suppressed": duplicates, "quarantine_rows": quarantine.count(), "product_rows": product_counts, "daily_delivered": [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(staged / "daily_delivered_quantities")).collect()] if (staged / "daily_delivered_quantities").exists() else [], "invoice_exceptions": [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(staged / "invoice_exceptions")).collect()], "publication_pointer": str(pointer_path)}
    (root / "evidence" / "runs").mkdir(parents=True, exist_ok=True)
    (root / "evidence" / "runs" / f"{run_id}.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    if owns_spark:
        spark.stop()
    return report


def published_product(root: Path, product: str) -> list[dict[str, object]]:
    root = root.resolve()
    pointer = json.loads((root / "tables" / "gold" / "current_generation.json").read_text())
    path = root / "tables" / "gold" / "generations" / pointer["generation"] / product
    spark = create_spark("construction-data-platform-read")
    try:
        return [r.asDict(recursive=True) for r in _read_delta(spark, path).collect()]
    finally:
        spark.stop()
