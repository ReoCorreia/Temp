import argparse
import logging
import importlib
import pkgutil
import yaml
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage
import pandas as pd
import great_expectations as gx
import pyarrow.parquet as pq
import gcsfs
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class ValidateParquetDoFn(beam.DoFn):
    """
    Reads a Parquet file, runs GX rules with custom expectations, and outputs file-level statistics.
    """
    def __init__(self, rules_gcs_path: str, table_name: str):
        self.rules_gcs_path = rules_gcs_path
        self.table_name = table_name
        self.rules_config = None
        self.gx_context = None

    def setup(self):
        """Runs once per worker to load GX rules and custom expectations."""
        try:
            # Load custom expectations first
            self._load_custom_expectations("custom_expectations")
            
            # Initialize GX context
            self.gx_context = gx.get_context(mode="ephemeral")
            
            # Load rules from GCS
            if not self.rules_gcs_path.startswith("gs://"):
                raise ValueError(f"Rules path must be a GCS URI: {self.rules_gcs_path}")

            client = storage.Client()
            bucket_name, blob_name = self.rules_gcs_path.replace("gs://", "").split("/", 1)
            blob = client.bucket(bucket_name).blob(blob_name)

            if not blob.exists():
                raise FileNotFoundError(f"Rules file not found: {self.rules_gcs_path}")

            rules_content = blob.download_as_string().decode('utf-8')
            self.rules_config = yaml.safe_load(rules_content)

            if not self.rules_config or 'tables' not in self.rules_config:
                raise ValueError(f"Invalid rules configuration: missing 'tables' key")

            if self.table_name not in self.rules_config.get('tables', {}):
                raise ValueError(f"Table '{self.table_name}' not found in rules configuration")

            log.info(f"Successfully loaded rules for table '{self.table_name}'")

        except Exception as e:
            log.critical(f"Failed to load rules from {self.rules_gcs_path}: {e}")
            raise

    def _load_custom_expectations(self, package_name: str = "custom_expectations"):
        """Load custom expectations package."""
        try:
            pkg = importlib.import_module(package_name)
        except ModuleNotFoundError:
            log.warning(f"Custom expectations package '{package_name}' not found, skipping")
            return

        if hasattr(pkg, "__path__"):
            for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
                if not ispkg:
                    try:
                        importlib.import_module(f"{package_name}.{modname}")
                        log.info(f"Loaded custom expectation module: {package_name}.{modname}")
                    except Exception as e:
                        log.warning(f"Failed to load module {package_name}.{modname}: {e}")

    def _get_expectation_callable(self, rule_name: str):
        """Get expectation class by rule name."""
        class_name = "".join([part.capitalize() for part in rule_name.split("_")])
        
        for module_name in [
            "custom_expectations",
            "great_expectations.expectations.core",
        ]:
            try:
                module = importlib.import_module(module_name)
                cls = getattr(module, class_name)
                return cls
            except (ModuleNotFoundError, AttributeError):
                continue
        
        raise ImportError(f"Expectation class for '{rule_name}' not found in custom or GE modules.")

    def _apply_expectation(self, batch, rules: Dict, table_name: str) -> List[Dict]:
        """Apply expectations to a batch using the script's logic."""
        results = []
        columns = rules["tables"][table_name]["columns"]
        
        for column, rules_list in columns.items():
            for rule in rules_list:
                try:
                    if isinstance(rule, str):
                        # Single-column rule
                        expectation_class = self._get_expectation_callable(rule)
                        expectation = expectation_class(column=column)
                    elif isinstance(rule, dict):
                        rule_name, supplied_kwargs = list(rule.items())[0]
                        supplied_kwargs = dict(supplied_kwargs or {})
                        multi_keys = {"column_A", "column_B", "column_list", "columns"}
                        
                        if not (multi_keys & supplied_kwargs.keys()):
                            supplied_kwargs["column"] = column
                        
                        expectation_class = self._get_expectation_callable(rule_name)
                        expectation = expectation_class(**supplied_kwargs)
                    else:
                        continue

                    # Validate using batch.validate()
                    validation_result = batch.validate(expectation, result_format="COMPLETE")
                    
                    # Extract results from validation result
                    if hasattr(validation_result, 'results'):
                        for result in validation_result.results:
                            results.append(result)
                    elif isinstance(validation_result, dict):
                        results.append(validation_result)
                    elif hasattr(validation_result, 'success'):
                        results.append(validation_result)
                    else:
                        log.warning(f"Unexpected validation result type: {type(validation_result)}")
                        
                    exp_type = getattr(expectation, "expectation_type", expectation.__class__.__name__)
                    log.info(f"Validated: {column} - {exp_type}")
                    
                except Exception as e:
                    log.error(f"Failed to apply expectation for column {column}, rule {rule}: {e}")
                    continue

        return results

    def _parse_validation_results(self, results: List[Dict]) -> List[Dict]:
        """Parse validation results to match the script's output format."""
        parsed = []
        
        for result in results:
            if isinstance(result, dict):
                config = result.get("expectation_config", {})
                res = result.get("result", {})
                
                parsed.append({
                    "expectation": config.get("expectation_type", "unknown"),
                    "column": config.get("kwargs", {}).get("column", "Unknown"),
                    "success": result.get("success", False),
                    "element_count": res.get("element_count", "N/A"),
                    "unexpected_count": res.get("unexpected_count", "N/A"),
                    "unexpected_percent": res.get("unexpected_percent", "N/A"),
                    "sample_unexpected_values": res.get("partial_unexpected_list", [])[:10],
                    "file_name": ""
                })
            elif hasattr(result, 'expectation_config') and hasattr(result, 'result'):
                config = result.expectation_config
                res = result.result
                
                config_dict = config if isinstance(config, dict) else {
                    "expectation_type": getattr(config, 'expectation_type', 'unknown'),
                    "kwargs": getattr(config, 'kwargs', {})
                }
                res_dict = res if isinstance(res, dict) else {
                    "element_count": getattr(res, 'element_count', 'N/A'),
                    "unexpected_count": getattr(res, 'unexpected_count', 'N/A'),
                    "unexpected_percent": getattr(res, 'unexpected_percent', 'N/A'),
                    "partial_unexpected_list": getattr(res, 'partial_unexpected_list', [])
                }
                
                parsed.append({
                    "expectation": config_dict.get("expectation_type", "unknown"),
                    "column": config_dict.get("kwargs", {}).get("column", "Unknown"),
                    "success": getattr(result, 'success', False) if hasattr(result, 'success') else False,
                    "element_count": res_dict.get("element_count", "N/A"),
                    "unexpected_count": res_dict.get("unexpected_count", "N/A"),
                    "unexpected_percent": res_dict.get("unexpected_percent", "N/A"),
                    "sample_unexpected_values": res_dict.get("partial_unexpected_list", [])[:10],
                    "file_name": ""
                })
            else:
                log.warning(f"Unexpected result format: {type(result)}")
                continue
        
        return parsed

    def process(self, file_metadata):
        """Process a single Parquet file."""
        file_path = file_metadata.path
        log.info(f"Processing file: {file_path}")

        try:
            if not file_path.startswith("gs://"):
                log.error(f"Invalid file path (not GCS): {file_path}")
                return

            fs = gcsfs.GCSFileSystem()
            
            try:
                parquet_file = pq.ParquetFile(fs.open(file_path))
                total_rows = parquet_file.metadata.num_rows
                log.info(f"File {file_path} has {total_rows} total rows")

                if total_rows == 0:
                    log.warning(f"Empty file {file_path}, skipping validation")
                    return

                table = parquet_file.read()
                df = table.to_pandas()
                
                batch = self.gx_context.data_sources.pandas_default.read_dataframe(df)
                
                validation_results = self._apply_expectation(batch, self.rules_config, self.table_name)
                parsed_results = self._parse_validation_results(validation_results)
                
                for result in parsed_results:
                    result["file_name"] = file_path
                
                total_checks = len(parsed_results)
                failed_checks = sum(1 for r in parsed_results if not r.get("success", False))
                passed_checks = total_checks - failed_checks
                
                total_unexpected = sum(
                    r.get("unexpected_count", 0) 
                    for r in parsed_results 
                    if isinstance(r.get("unexpected_count"), (int, float))
                )
                
                log.info(f"File {file_path}: {passed_checks} passed, {failed_checks} failed out of {total_checks} checks")

                yield {
                    'file_path': file_path,
                    'total_records': total_rows,
                    'total_checks': total_checks,
                    'passed_checks': passed_checks,
                    'failed_checks': failed_checks,
                    'total_unexpected_count': total_unexpected,
                    'details': parsed_results
                }

            except Exception as e:
                log.error(f"Failed to read or process Parquet file {file_path}: {e}")
                yield {
                    'file_path': file_path,
                    'total_records': 0,
                    'total_checks': 0,
                    'passed_checks': 0,
                    'failed_checks': 0,
                    'total_unexpected_count': 0,
                    'details': [],
                    'error': str(e)
                }

        except Exception as e:
            log.error(f"Unexpected error processing {file_path}: {e}")
            yield {
                'file_path': file_path,
                'total_records': 0,
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0,
                'total_unexpected_count': 0,
                'details': [],
                'error': str(e)
            }

    def teardown(self):
        """Cleanup resources after processing."""
        try:
            if self.gx_context:
                self.gx_context = None
            log.info("GX context cleaned up successfully")
        except Exception as e:
            log.warning(f"Error during teardown: {e}")


class TableStatsCombineFn(beam.CombineFn):
    """
    Aggregates file-level stats into table-level stats.
    """
    def create_accumulator(self):
        return {
            'files_processed': 0,
            'total_records': 0,
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'total_unexpected_count': 0,
            'details': [],
            'errors': []
        }

    def add_input(self, accumulator, input_element):
        accumulator['files_processed'] += 1
        
        if 'error' in input_element:
            accumulator['errors'].append({
                'file_path': input_element.get('file_path', 'unknown'),
                'error': input_element['error']
            })
        
        accumulator['details'].extend(input_element.get('details', []))
        
        for key in ['total_records', 'total_checks', 'passed_checks', 'failed_checks', 'total_unexpected_count']:
            accumulator[key] += input_element.get(key, 0)
        
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged['files_processed'] += acc['files_processed']
            merged['details'].extend(acc['details'])
            merged['errors'].extend(acc.get('errors', []))
            for key in ['total_records', 'total_checks', 'passed_checks', 'failed_checks', 'total_unexpected_count']:
                merged[key] += acc[key]
        return merged

    def extract_output(self, accumulator):
        total_checks = accumulator['total_checks']
        passed_checks = accumulator['passed_checks']
        total_records = accumulator['total_records']
        
        if total_checks == 0:
            log.warning("No checks performed - all files may have failed or been empty")
            accumulator['pass_rate_pct'] = 0.0
            accumulator['dq_status'] = 'NO_DATA'
        else:
            accumulator['pass_rate_pct'] = (passed_checks / total_checks * 100) if total_checks > 0 else 0.0
            accumulator['dq_status'] = 'PASSED' if accumulator['pass_rate_pct'] >= 95.0 else 'FAILED'
        
        if total_records > 0:
            accumulator['unexpected_record_rate'] = (accumulator['total_unexpected_count'] / total_records * 100)
        else:
            accumulator['unexpected_record_rate'] = 0.0

        log.info(
            f"Final aggregation: {passed_checks}/{total_checks} checks passed "
            f"({accumulator['pass_rate_pct']:.2f}%), "
            f"{accumulator['total_unexpected_count']}/{total_records} unexpected records, "
            f"status: {accumulator['dq_status']}"
        )
        
        return json.dumps(accumulator, default=str)


def run(argv=None):
    parser = argparse.ArgumentParser(description="Data Quality Check Pipeline using Great Expectations")
    parser.add_argument('--input_pattern', required=True, help='GCS glob pattern for table files (e.g., gs://bucket/path/*.parquet)')
    parser.add_argument('--rules_yaml', required=True, help='GCS path to rules YAML file')
    parser.add_argument('--output_stats', required=True, help='GCS path for final table stats JSON')
    parser.add_argument('--table_name', required=True, help='Table name for rules lookup')
    parser.add_argument('--dq_run_id', default='', help='Run ID for tracking (optional)')

    known_args, pipeline_args = parser.parse_known_args(argv)

    if not known_args.input_pattern.startswith("gs://"):
        raise ValueError("input_pattern must be a GCS URI starting with 'gs://'")
    if not known_args.rules_yaml.startswith("gs://"):
        raise ValueError("rules_yaml must be a GCS URI starting with 'gs://'")
    if not known_args.output_stats.startswith("gs://"):
        raise ValueError("output_stats must be a GCS URI starting with 'gs://'")

    log.info(f"Starting DQ check pipeline with run_id: {known_args.dq_run_id or 'auto-generated'}")
    log.info(f"Input pattern: {known_args.input_pattern}")
    log.info(f"Rules YAML: {known_args.rules_yaml}")
    log.info(f"Table name: {known_args.table_name}")
    log.info(f"Output path: {known_args.output_stats}")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    try:
        with beam.Pipeline(options=pipeline_options) as p:
            file_matches = (
                p
                | 'MatchParquetFiles' >> beam.io.fileio.MatchFiles(known_args.input_pattern)
            )

            validation_results = (
                file_matches
                | 'ValidateFiles' >> beam.ParDo(ValidateParquetDoFn(known_args.rules_yaml, known_args.table_name))
            )

            aggregated_stats = (
                validation_results
                | 'AggregateTableStats' >> beam.CombineGlobally(TableStatsCombineFn())
            )

            def add_metadata(stats_str, run_id, table_name):
                stats_dict = json.loads(stats_str)
                if run_id:
                    stats_dict['dq_run_id'] = run_id
                stats_dict['table_name'] = table_name
                return json.dumps(stats_dict, default=str)

            final_output = (
                aggregated_stats
                | 'AddRunId' >> beam.Map(add_metadata, known_args.dq_run_id, known_args.table_name)
                | 'WriteStats' >> beam.io.WriteToText(
                    known_args.output_stats, 
                    shard_name_template='', 
                    num_shards=1
                )
            )

            log.info("Pipeline execution completed successfully")

    except Exception as e:
        log.critical(f"Pipeline execution failed: {e}")
        raise


if __name__ == '__main__':
    run()
