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
    spark.sparkContext.setLogLevel("ERROR")
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
        duplicates += before_retry_dedup - raw.count()
        raw_frames[name] = raw
        _write_delta(raw, raw_path)
        counts[name] = raw.count()

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
    _invoices, lines, allocations = (current(n) for n in ("invoices", "invoice_lines", "invoice_delivery_allocations"))
    quarantined = []
    if ticket is not None:
        unit_ok = F.col("weight_unit").isin(*CONVERSION_TO_KG)
        numeric_ok = (F.col("gross_weight").cast("decimal(18,6)").isNotNull() & F.col("tare_weight").cast("decimal(18,6)").isNotNull())
        ticket = ticket.withColumn("quarantine_reason", F.when(~unit_ok, "UNKNOWN_WEIGHT_UNIT").when(~numeric_ok, "INVALID_WEIGHT").when(F.col("gross_weight").cast("decimal(18,6)") < F.col("tare_weight").cast("decimal(18,6)"), "GROSS_BELOW_TARE"))
        quarantined.append(ticket.filter(F.col("quarantine_reason").isNotNull()))
        ticket = ticket.filter(F.col("quarantine_reason").isNull()).withColumn("net_weight_original", (F.col("gross_weight").cast("decimal(18,6)") - F.col("tare_weight").cast("decimal(18,6)"))).withColumn("net_metric_tonnes", (F.col("net_weight_original") * F.lit(Decimal(CONVERSION_TO_KG["metric_tonne"]) / Decimal(CONVERSION_TO_KG["metric_tonne"]))).cast("decimal(18,6)"))
        # Unit-specific exact conversions to tonnes.
        factor = F.when(F.col("weight_unit") == "kg", F.lit(Decimal("0.001"))).when(F.col("weight_unit") == "us_short_ton", F.lit(Decimal("0.90718474"))).otherwise(F.lit(Decimal(1)))
        ticket = ticket.withColumn("net_metric_tonnes", (F.col("net_weight_original") * factor).cast("decimal(18,6)"))
        _write_delta(ticket, store / "silver" / "scale_tickets_current")

    if po is not None:
        po = po.withColumn("ordered_quantity_decimal", F.col("ordered_quantity").cast("decimal(18,6)")).withColumn("unit_price_decimal", F.col("unit_price").cast("decimal(18,6)"))
        _write_delta(po, store / "silver" / "purchase_order_lines_current")
    if receipt is not None:
        _write_delta(receipt, store / "silver" / "site_receipts_current")

    if ticket is not None and receipt is not None:
        if po is None:
            po_schema = T.StructType([T.StructField("company_id", T.StringType()), T.StructField("po_id", T.StringType()), T.StructField("po_line_id", T.StringType()), T.StructField("project_id", T.StringType()), T.StructField("supplier_id", T.StringType()), T.StructField("material_id", T.StringType())])
            po = spark.createDataFrame([], po_schema)
        delivery = ticket.alias("t").join(receipt.alias("r"), F.col("t.delivery_id") == F.col("r.delivery_id"), "left")
        delivery = delivery.select("t.*", F.col("r.delivery_id").alias("receipt_delivery_id"), F.col("r.received_at").alias("received_at"), F.col("r.acceptance_status").alias("acceptance_status"), F.col("r.accepted_fraction").alias("accepted_fraction"), F.col("r.project_id").alias("receipt_project_id"))
        delivery = delivery.alias("d").join(po.alias("p"), (F.col("d.po_id") == F.col("p.po_id")) & (F.col("d.po_line_id") == F.col("p.po_line_id")), "left")
        delivery = delivery.select("d.*", F.col("p.po_id").alias("matched_po_id"), F.col("p.project_id").alias("po_project_id"), F.col("p.supplier_id").alias("po_supplier_id"), F.col("p.material_id").alias("po_material_id"))
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
        alloc_line = allocations.groupBy("company_id", "supplier_id", "invoice_id", "invoice_line_id").agg(F.sum(F.col("allocated_quantity").cast("decimal(18,6)")).alias("allocated_quantity"), F.countDistinct("delivery_id").alias("allocated_delivery_count"))
        line_basis = lines.join(alloc_line, ["company_id", "supplier_id", "invoice_id", "invoice_line_id"], "left")
        line_basis = line_basis.withColumn("billed_quantity_decimal", F.col("billed_quantity").cast("decimal(18,6)")).withColumn("unit_price_decimal", F.col("unit_price").cast("decimal(18,6)")).withColumn("line_amount_decimal", F.col("line_amount").cast("decimal(18,2)"))
        expected_amount = F.round(F.col("billed_quantity_decimal") * F.col("unit_price_decimal"), 2)
        line_basis = line_basis.withColumn("amount_mismatch", F.abs(F.col("line_amount_decimal") - expected_amount) > F.lit(Decimal("0.01")))
        alloc_delivery = allocations.join(delivery.select("delivery_id", "accepted_net_quantity", "delivery_state", "project_id", "material_id", "po_id", "po_line_id"), "delivery_id", "left")
        alloc_ref = alloc_delivery.groupBy("company_id", "supplier_id", "invoice_id", "invoice_line_id").agg(F.sum("accepted_net_quantity").alias("allocated_receipt_quantity"), F.max(F.when((F.col("delivery_state").isNull()) | (F.col("delivery_state") != "ACCEPTED"), 1).otherwise(0)).alias("unresolved_delivery"))
        result = line_basis.join(alloc_ref, ["company_id", "supplier_id", "invoice_id", "invoice_line_id"], "left")
        tol = F.greatest(F.lit(Decimal("0.01")), F.coalesce(F.col("allocated_receipt_quantity"), F.lit(Decimal(0))) * F.lit(Decimal("0.005")))
        result = result.withColumn("quantity_difference", F.abs(F.coalesce(F.col("allocated_quantity"), F.lit(Decimal(0))) - F.coalesce(F.col("allocated_receipt_quantity"), F.lit(Decimal(0)))))
        result = result.withColumn("exception_reason", F.when(F.col("allocated_quantity").isNull(), "MISSING_ALLOCATION").when(F.col("unresolved_delivery") == 1, "PENDING_SUPPORTING_DELIVERY").when(F.col("amount_mismatch"), "LINE_AMOUNT_MISMATCH").when(F.col("quantity_difference") > tol, "QUANTITY_MISMATCH"))
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
    report = {"run_id": run_id, "generation": generation, "status": "SUCCESS", "synthetic": True, "started_at_utc": started, "finished_at_utc": datetime.now(UTC).isoformat(), "source_rows_by_feed": counts, "duplicate_business_envelopes_suppressed": duplicates, "quarantine_rows": quarantine.count(), "product_rows": product_counts, "daily_delivered": [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(staged / "daily_delivered_quantities")).collect()] if (staged / "daily_delivered_quantities").exists() else [], "invoice_exceptions": [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(staged / "invoice_exceptions")).collect()], "publication_pointer": str(pointer_path)}
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
