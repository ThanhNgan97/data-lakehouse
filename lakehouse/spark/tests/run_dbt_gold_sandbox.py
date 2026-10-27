"""Bounded live acceptance, owned Nessie refs only. Never invokes the full DAG."""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import time

import requests
from pyspark.sql import functions as F

import dbt_gold_runtime as gold
from dbt_gold_contract import identity
from api_dataset_registry import get_dataset_config
from env_config import NESSIE_API_URL, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--baseline-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--max-source-rows', type=int, default=10000)
    args = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{10}', args.tag) or not 1 <= args.max_source_rows <= 10000:
        raise ValueError('Invalid acceptance identity or row bound')
    prefix = 'dbt_gold_test_step4_' + args.tag + '_'
    report = {'status': 'running', 'tag': args.tag, 'cases': [], 'owned_refs': [],
              'started_utc_epoch': time.time(), 'dag_triggered': False, 'main_write_requested': False,
              'max_source_rows': args.max_source_rows, 'cleanup': [], 'float_tolerance': {'absolute': 1e-9, 'relative': 1e-12}}
    args.output_dir.mkdir(exist_ok=True)
    spark = None
    jobs = []
    catalogs = set()
    baseline_ref = prefix + 'baseline'

    def save():
        (args.output_dir / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')

    def owned(ref):
        if ref not in report['owned_refs'] or ref == 'main':
            raise RuntimeError('Ref is not owned by this acceptance run')

    def exists(ref):
        response = requests.get(NESSIE_API_URL.rstrip('/') + '/trees/tree/' + ref, timeout=15)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def new_target(ref):
        if not ref.startswith(prefix) or exists(ref):
            raise RuntimeError('Ref already exists or is outside this run')
        gold.api('POST', '/trees/tree', params={'sourceRefName': 'main'},
                 json={'type': 'BRANCH', 'name': ref, 'hash': report['main_before']})
        report['owned_refs'].append(ref)
        save()
        if gold.head(ref) != report['main_before']:
            raise RuntimeError('Sandbox was not created from the pinned head')

    def assert_main():
        if gold.head('main') != report['main_before']:
            raise RuntimeError('Main changed during acceptance; do not overwrite or repair it')

    def sql(statement):
        response = requests.post('http://trino:8080/v1/statement', data=statement.encode(),
                                 headers={'X-Trino-User': 'dbt_step4_acceptance'}, timeout=20)
        response.raise_for_status()
        rows = []
        while True:
            result = response.json()
            if result.get('error'):
                raise RuntimeError('Acceptance catalog query failed: ' + result['error'].get('errorName', 'UNKNOWN'))
            rows.extend(result.get('data', []))
            if not result.get('nextUri'):
                return rows
            response = requests.get(result['nextUri'], timeout=20)
            response.raise_for_status()

    def configure_catalog(name, ref):
        owned(ref)
        options = {'': 'org.apache.iceberg.spark.SparkCatalog', 'catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
                   'uri': NESSIE_API_URL, 'ref': ref, 'warehouse': 's3a://' + S3_BUCKET_NAME + '/iceberg-warehouse',
                   'cache-enabled': 'false', 's3.endpoint': S3_ENDPOINT}
        for key, value in options.items():
            spark.conf.set('spark.sql.catalog.' + name + ('.' + key if key else ''), value)

    def baseline(name):
        path = args.baseline_dir / ('spark_' + name + '_to_gold.py')
        spec = importlib.util.spec_from_file_location('step4_baseline_' + name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        report.setdefault('baseline_sources', {})[name] = {'file': str(path), 'sha256': sha(path)}
        return module

    def compare(expected, actual, keys):
        expected_schema = [(f.name, f.dataType.simpleString()) for f in expected.schema.fields]
        actual_schema = [(f.name, f.dataType.simpleString()) for f in actual.schema.fields]
        if expected_schema != actual_schema:
            raise RuntimeError('Output column order/type differs from reference')
        def index(frame):
            result = {}
            for row in frame.collect():
                data = row.asDict()
                key = tuple(data[k] for k in keys)
                if key in result:
                    raise RuntimeError('Duplicate output business key')
                result[key] = data
            return result
        left, right = index(expected), index(actual)
        if set(left) != set(right):
            raise RuntimeError('Output rows/keys differ from reference')
        for key in left:
            for column, a in left[key].items():
                if column == '_gold_updated_at':
                    continue
                b = right[key][column]
                if a is None or b is None:
                    equal = a is None and b is None
                elif isinstance(a, float) or isinstance(b, float):
                    equal = math.isclose(a, b, abs_tol=1e-9, rel_tol=1e-12)
                else:
                    equal = a == b
                if not equal:
                    raise RuntimeError('Output value mismatch in column ' + column)
        return {'rows': len(left), 'compared_columns': len(expected_schema) - 1,
                'schema_order_types_match': True, 'parity': True}

    def mart_reference(fact):
        totals = fact.groupBy('nam_hoc', 'hoc_ky').agg(
            F.countDistinct('ma_chuong_trinh').alias('so_chuong_trinh'),
            *[F.sum(c).alias(c) for c in ['so_luot_hoc_phan_dat', 'tong_luot_hoc_phan', 'tong_diem_gpa',
                                        'so_sinh_vien_tinh_gpa', 'so_sinh_vien_dung_tien_do', 'so_sinh_vien_danh_gia_tien_do']])
        def rate(numerator, denominator, factor):
            return F.when(F.col(denominator) > 0, F.col(numerator).cast('double') * F.lit(factor) / F.col(denominator).cast('double'))
        return totals.select('nam_hoc', 'hoc_ky', 'so_chuong_trinh', 'so_luot_hoc_phan_dat', 'tong_luot_hoc_phan',
            rate('so_luot_hoc_phan_dat', 'tong_luot_hoc_phan', 100.0).alias('ty_le_qua_hoc_phan'),
            'tong_diem_gpa', 'so_sinh_vien_tinh_gpa', rate('tong_diem_gpa', 'so_sinh_vien_tinh_gpa', 1.0).alias('gpa_trung_binh'),
            'so_sinh_vien_dung_tien_do', 'so_sinh_vien_danh_gia_tien_do',
            rate('so_sinh_vien_dung_tien_do', 'so_sinh_vien_danh_gia_tien_do', 100.0).alias('ty_le_dung_tien_do'),
            F.current_timestamp().alias('_gold_updated_at'))

    def run_gold(name, label):
        config = get_dataset_config('education.' + name)
        target = prefix + label
        new_target(target)
        run_id = 'step4_' + args.tag + '_' + label
        job, ref, _ = identity(config.dataset, run_id)
        if exists(ref):
            raise RuntimeError('Candidate ref collision')
        os.environ['GOLD_TARGET_REF'] = target
        case = {'dataset': config.dataset, 'label': label, 'target': target, 'job': job, 'ref': ref, 'timings': {}}
        report['cases'].append(case)
        jobs.append(job)
        # Register the deterministic work ref before prepare; absence was verified.
        report['owned_refs'].append(ref)
        catalogs.add(ref)
        save()
        for phase, action in [('prepare', gold.prepare), ('build', gold.build), ('validate', gold.validate)]:
            owned(target)
            assert_main()
            start = time.monotonic()
            action(config, run_id)
            case['timings'][phase] = time.monotonic() - start
            save()
        state = gold.load(config, run_id)
        result = gold.worker_result(state)
        if result['source_content'] != report['silver_sources'][name]:
            raise RuntimeError('Worker did not use the pinned Silver snapshot')
        if label == 'changed_candidate':
            marker = 'step4_audit_' + args.tag
            gold.api('POST', '/trees/branch/' + ref + '/commit', params={'expectedHash': state['validated_hash']},
                     json={'commitMeta': {'message': 'step4 owned candidate head-change check'},
                           'operations': [{'type': 'PUT', 'key': {'elements': [marker]},
                                           'content': {'type': 'NAMESPACE', 'elements': [marker], 'properties': {}}}]})
            target_before = gold.head(target)
            try:
                gold.publish(config, run_id)
            except RuntimeError as exc:
                if 'Validated candidate changed' not in str(exc):
                    raise
                case['publish_refused_after_candidate_change'] = True
            else:
                raise RuntimeError('Changed candidate was incorrectly published')
            if gold.head(target) != target_before:
                raise RuntimeError('Refused publication changed the sandbox target')
            case['status'] = 'expected_refusal'
        else:
            start = time.monotonic()
            gold.publish(config, run_id)
            case['timings']['publish'] = time.monotonic() - start
            published = gold.load(config, run_id)
            published_head = gold.head(target)
            # Replay the whole lifecycle with the same identity: no second build/commit.
            gold.prepare(config, run_id)
            gold.build(config, run_id)
            gold.validate(config, run_id)
            gold.publish(config, run_id)
            after = gold.load(config, run_id)
            if gold.head(target) != published_head or after['artifact_sha256'] != published['artifact_sha256']:
                raise RuntimeError('Retry changed build artifact or published target')
            case.update(status='published_in_sandbox', retry_idempotent=True, outputs=published['candidate_contents'])
        case['result_summary'] = result['result_summary']
        case['artifact_sha256'] = result['artifact_sha256']
        case['catalog_cleanup_failed'] = result.get('catalog_cleanup_failed', False)
        save()
        return case

    save()
    try:
        health = requests.get(gold.WORKER + '/health', timeout=5)
        health.raise_for_status()
        report['main_before'] = gold.head('main')
        report['silver_sources'] = {name: gold.content('main', 'silver.' + name, report['main_before'])
                                    for name in ['learning_outcomes', 'teaching_progress']}
        if any(not c or c.get('type') != 'ICEBERG_TABLE' for c in report['silver_sources'].values()):
            raise RuntimeError('Both Silver sources must exist')
        if gold.content('main', 'gold', report['main_before']) is None:
            raise RuntimeError('Gold namespace must already exist')
        new_target(baseline_ref)
        from spark_bronze_to_silver import get_spark_session
        spark = get_spark_session()
        spark.sparkContext.setLogLevel('WARN')
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        configure_catalog('step4_base', baseline_ref)
        report['source_rows'] = {}
        for name in ['learning_outcomes', 'teaching_progress']:
            count = spark.table('step4_base.silver.' + name).count()
            report['source_rows'][name] = count
            if not 0 < count <= args.max_source_rows:
                raise RuntimeError('Silver row count outside the approved bounded acceptance workload')
        save()
        # Run dbt before Spark parity scans/writes; no simultaneous benchmark.
        positive = {name: run_gold(name, name) for name in ['learning_outcomes', 'teaching_progress']}
        for name, case in positive.items():
            owned(baseline_ref)
            configure_catalog('step4_actual_' + name, case['target'])
            module = baseline(name)
            source = spark.table('step4_base.silver.' + name)
            if name == 'learning_outcomes':
                reference = module.transform_learning_outcomes_gold(source.filter(F.col('da_xoa') == F.lit(False)))
            else:
                reference = module.transform_teaching_progress_gold(source)
            alias = name + '_step4_reference_' + args.tag
            table = 'step4_base.gold.' + alias
            reference.writeTo(table).createOrReplace()
            reference = spark.table(table)
            actual = spark.table('step4_actual_' + name + '.gold.' + name + '_metrics')
            case['fact_parity'] = compare(reference, actual, module.BUSINESS_KEY)
            if name == 'learning_outcomes':
                expected = mart_reference(reference)
                actual = spark.table('step4_actual_' + name + '.gold.learning_outcomes_by_period')
                case['mart_parity'] = compare(expected, actual, ('nam_hoc', 'hoc_ky'))
            if gold.content(case['target'], 'silver.' + name) != report['silver_sources'][name]:
                raise RuntimeError('Sandbox Silver source changed')
            save()
        run_gold('learning_outcomes', 'changed_candidate')
        assert_main()
        report['status'] = 'passed'
    except Exception as exc:
        report['status'] = 'failed'
        report['error_type'] = type(exc).__name__
        report['error'] = str(exc)
        print('STEP4_STOP=' + type(exc).__name__, flush=True)
    finally:
        if spark is not None:
            try: spark.stop()
            except Exception: report['spark_stop_error'] = True
        terminal = True
        for job in jobs:
            try:
                response = requests.get(gold.WORKER + '/jobs/' + job, timeout=10)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                if response.json().get('status') not in ('passed', 'failed'):
                    terminal = False
            except Exception:
                terminal = False
        report['all_jobs_terminal'] = terminal
        if terminal:
            for catalog in sorted(catalogs):
                try:
                    present = {row[0] for row in sql('SHOW CATALOGS')}
                    if catalog in present:
                        sql('DROP CATALOG ' + catalog)
                    report['cleanup'].append({'catalog': catalog, 'removed_or_absent': True})
                except Exception as exc:
                    report['cleanup'].append({'catalog': catalog, 'error_type': type(exc).__name__})
                    report['status'] = 'failed'
            for ref in reversed(report['owned_refs']):
                try:
                    owned(ref)
                    if exists(ref):
                        current = gold.head(ref)
                        response = requests.delete(NESSIE_API_URL.rstrip('/') + '/trees/branch/' + ref,
                                                   params={'expectedHash': current}, timeout=30)
                        response.raise_for_status()
                    if exists(ref):
                        raise RuntimeError('Owned ref cleanup readback failed')
                    report['cleanup'].append({'ref': ref, 'removed_or_absent': True})
                except Exception as exc:
                    report['cleanup'].append({'ref': ref, 'error_type': type(exc).__name__})
                    report['status'] = 'failed'
        else:
            report['status'] = 'failed'
            report['cleanup_deferred'] = 'Worker still active or unreachable; retain refs, no automatic retry'
        try:
            report['main_after'] = gold.head('main')
            report['main_unchanged'] = report['main_after'] == report.get('main_before')
            if not report['main_unchanged']:
                report['status'] = 'failed'
        except Exception:
            report['status'] = 'failed'
        report['finished_utc_epoch'] = time.time()
        save()
        print('STEP4_STATUS=' + report['status'], flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
