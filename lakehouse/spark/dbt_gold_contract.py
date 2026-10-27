"""Pure, shared dbt-only Gold identity and output contract."""
import hashlib

SQL_VALIDATION_CONTRACT = "dbt_sql_ozone_v1_learning13_teaching15"
DATASETS = {
    "education.learning_outcomes": ("agg_learning_outcomes_by_period", "learning_outcomes_metrics"),
    "education.teaching_progress": ("fct_teaching_progress", "teaching_progress_metrics"),
}


def identity(dataset, run_id):
    if dataset not in DATASETS or not isinstance(run_id, str) or not run_id or len(run_id) > 1000:
        raise ValueError("Unsupported dataset or invalid run ID")
    digest = hashlib.sha256(f"{dataset}\n{run_id}".encode()).hexdigest()[:24]
    return digest, "dbt_gold_" + digest, DATASETS[dataset][1] + "_" + digest


def outputs(dataset, run_id):
    job, _, alias = identity(dataset, run_id)
    result = {DATASETS[dataset][1]: alias}
    if dataset == "education.learning_outcomes":
        result["learning_outcomes_by_period"] = "learning_outcomes_by_period_" + job
    return result


def require_config(config):
    if config.dataset not in DATASETS:
        raise ValueError("Dataset has no dbt Gold contract")
    selector, table = DATASETS[config.dataset]
    if config.dbt_selector != selector or config.gold_table != "lakehouse.gold." + table:
        raise ValueError("Dataset dbt routing does not match Gold contract")
    if config.silver_table != "lakehouse.silver." + config.dataset.split(".")[-1]:
        raise ValueError("Dataset Silver source does not match Gold contract")
