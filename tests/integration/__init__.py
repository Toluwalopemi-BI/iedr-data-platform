"""IEDR Integration Tests — Require a running Databricks cluster.

These tests validate the full DLT pipeline end-to-end:
1. Load sample data into landing zone
2. Trigger DLT pipeline
3. Verify row counts and schema in Gold/Platinum tables

Run with: pytest tests/integration/ --env=test
"""
