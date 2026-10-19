
"""
Test_All_Contracts.py

Validate all 10 API data contracts.

Run from the project root:
    python Contract/Test_All_Contracts.py

Install dependency:
    pip install jsonschema

Optional sample data:
    Contract/samples/Alpha.json
    Contract/samples/Finnhub.json
    Contract/samples/Coingecko.json
    Contract/samples/EODHD.json
    Contract/samples/FMP.json
    Contract/samples/FRED.json
    Contract/samples/Marketstack.json
    Contract/samples/Massive.json
    Contract/samples/Nvda.json
    Contract/samples/Tiingo.json

Each sample file may contain a single JSON object
or an array of JSON objects.
"""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


CONTRACT_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = CONTRACT_DIR / "samples"

CONTRACTS = {
    "Alpha": "Alpha_schema.json",
    "Finnhub": "Finnhub_schema.json",
    "Coingecko": "Coingecko_schema.json",
    "EODHD": "EODHD_schema.json",
    "FMP": "FMP_schema.json",
    "FRED": "FRED_schema.json",
    "Marketstack": "Marketstack_schema.json",
    "Massive": "Massive_schema.json",
    "Nvda": "Nvda_schema.json",
    "Tiingo": "Tiingo_schema.json",
}


def load_json(path):
    """Read a JSON file."""
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def test_contract(provider, filename):
    """Validate one provider's schema and optional sample data."""

    schema_path = CONTRACT_DIR / "Schema" / filename
    sample_path = SAMPLE_DIR / f"{provider}.json"

    print(f"\n{'=' * 60}")
    print(f"TESTING CONTRACT: {provider}")
    print(f"{'=' * 60}")

    if not schema_path.is_file():
        print(f"FAIL: Schema file not found: {schema_path.name}")
        return False

    try:
        schema = load_json(schema_path)
    except (json.JSONDecodeError, OSError) as error:
        print(f"FAIL: Cannot read schema: {error}")
        return False

    try:
        Draft202012Validator.check_schema(schema)
    except Exception as error:
        print(f"FAIL: Invalid JSON Schema: {error}")
        return False

    print("PASS: Schema file exists")
    print("PASS: Valid JSON")
    print("PASS: Valid JSON Schema Draft 2020-12")

    if not isinstance(schema, dict):
        print("FAIL: Schema must be a JSON object")
        return False

    required_keys = [
        "$schema",
        "title",
        "type",
        "properties",
        "required",
    ]

    missing_keys = [
        key for key in required_keys
        if key not in schema
    ]

    if missing_keys:
        print(f"FAIL: Missing schema keys: {missing_keys}")
        return False

    if schema["type"] != "object":
        print("FAIL: Root schema type must be object")
        return False

    if not isinstance(schema["properties"], dict):
        print("FAIL: Properties must be an object")
        return False

    if not isinstance(schema["required"], list):
        print("FAIL: Required must be an array")
        return False

    unknown_required = [
        field for field in schema["required"]
        if field not in schema["properties"]
    ]

    if unknown_required:
        print(
            "FAIL: Required fields missing from properties: "
            f"{unknown_required}"
        )
        return False

    print("PASS: Contract structure is valid")
    print(f"INFO: Fields defined: {len(schema['properties'])}")
    print(f"INFO: Required fields: {len(schema['required'])}")

    if not sample_path.is_file():
        print(
            "SKIP: Sample data validation "
            f"({sample_path.name} not found)"
        )
        return True

    try:
        records = load_json(sample_path)
    except (json.JSONDecodeError, OSError) as error:
        print(f"FAIL: Cannot read sample data: {error}")
        return False

    if isinstance(records, dict):
        records = [records]
    elif not isinstance(records, list):
        print("FAIL: Sample must contain an object or array")
        return False

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    errors_found = 0

    for record_number, record in enumerate(records, start=1):
        errors = list(validator.iter_errors(record))

        if errors:
            errors_found += len(errors)

            print(f"FAIL: Sample record {record_number}")

            for error in errors:
                field_path = ".".join(
                    str(part) for part in error.absolute_path
                ) or "(root)"

                print(f"  Field: {field_path}")
                print(f"  Error: {error.message}")

    if errors_found:
        print(f"FAIL: {errors_found} validation error(s)")
        return False

    print(f"PASS: {len(records)} sample record(s) validated")
    return True


def main():
    print("\nALL API DATA CONTRACT TESTS")
    print(f"Contract directory: {CONTRACT_DIR}")

    passed = 0
    failed = 0

    for provider, filename in CONTRACTS.items():
        if test_contract(provider, filename):
            passed += 1
        else:
            failed += 1

    print(f"\n{'=' * 60}")
    print("FINAL CONTRACT TEST SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total contracts: {len(CONTRACTS)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed == 0:
        print("SUCCESS: All contract schema checks passed")
        print(
            "NOTE: Record validation runs only when "
            "sample JSON files are provided."
        )
        return 0

    print("FAILURE: One or more contracts failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
