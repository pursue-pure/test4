"""
m1_tester.py — M1 数据交换黑盒校验脚本
用法: python m1_tester.py <parquet_path>
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

REQUIRED_COLS = {"user_id", "item_id", "behavior_type", "timestamp"}
VALID_BEHAVIORS = {"pv", "cart", "fav", "buy"}
MIN_ROWS = 10_000_000


def check(label: str, passed: bool, detail: str = "") -> bool:
    icon = "✅" if passed else "❌"
    msg = f"  {icon}  {label}"
    if detail:
        msg += f"  →  {detail}"
    print(msg)
    return passed


def run_tests(path: str) -> None:
    print("=" * 60)
    print("  M1 数据校验报告")
    print(f"  文件: {path}")
    print("=" * 60)

    p = Path(path)
    passed_count = 0
    total = 0

    total += 1
    if check("文件存在且可访问", p.exists(), str(p.resolve())):
        passed_count += 1
    else:
        print("\n❌ 文件不存在，终止校验。")
        _summary(passed_count, total)
        return

    try:
        df = pl.read_parquet(path)
    except Exception as e:
        total += 1
        check("文件可读取", False, str(e))
        _summary(passed_count, total)
        return

    total += 1
    if check("文件可读取", True, f"{len(df):,} 行"):
        passed_count += 1

    total += 1
    if check("行数 > 0", len(df) > 0, f"实际行数: {len(df):,}"):
        passed_count += 1

    total += 1
    if check(f"行数 >= {MIN_ROWS:,}", len(df) >= MIN_ROWS, f"实际: {len(df):,}"):
        passed_count += 1

    total += 1
    actual_cols = set(df.columns)
    missing = REQUIRED_COLS - actual_cols
    if check("必要字段完整", len(missing) == 0,
             f"缺少: {missing}" if missing else f"字段: {sorted(actual_cols)}"):
        passed_count += 1
    else:
        print("\n❌ 缺少必要字段，跳过后续校验。")
        _summary(passed_count, total)
        return

    total += 1
    actual_behaviors = set(df["behavior_type"].unique().to_list())
    invalid = actual_behaviors - VALID_BEHAVIORS
    if check("behavior_type 值域合法", len(invalid) == 0,
             f"非法值: {invalid}" if invalid else f"值域: {sorted(actual_behaviors)}"):
        passed_count += 1

    total += 1
    null_rows = df.filter(
        pl.all_horizontal([pl.col(c).is_null() for c in REQUIRED_COLS])
    ).height
    if check("无全空行", null_rows == 0, f"全空行数: {null_rows}"):
        passed_count += 1

    total += 1
    neg_ts = df.filter(pl.col("timestamp") < 0).height
    if check("timestamp 无负值", neg_ts == 0, f"负值行数: {neg_ts}"):
        passed_count += 1

    total += 1
    user_stats = (
        df.group_by("user_id")
        .agg([
            (pl.col("behavior_type") == "buy").sum().alias("buy_cnt"),
            (pl.col("behavior_type") == "pv").sum().alias("pv_cnt"),
        ])
    )
    violators = user_stats.filter(pl.col("buy_cnt") > pl.col("pv_cnt")).height
    if check("业务逻辑: buy_count <= pv_count (每用户)",
             violators == 0, f"违规用户数: {violators}"):
        passed_count += 1

    total += 1
    sample_size = min(100_000, len(df))
    sample = df.sample(n=sample_size, seed=42)
    dedup_keys = ["user_id", "item_id", "behavior_type", "timestamp"]
    dup_in_sample = sample.height - sample.unique(subset=dedup_keys).height
    if check(f"抽样去重验证 ({sample_size:,} 行)",
             dup_in_sample == 0, f"抽样重复行: {dup_in_sample}"):
        passed_count += 1

    print()
    _summary(passed_count, total)


def _summary(passed: int, total: int) -> None:
    print("=" * 60)
    all_pass = passed == total
    icon = "✅" if all_pass else "❌"
    print(f"  {icon}  校验结果: {passed}/{total} 项通过")
    if all_pass:
        print("  数据质量符合 M1 基准要求！")
    else:
        print(f"  {total - passed} 项未通过，请根据上方报错回溯修正。")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python m1_tester.py <parquet_path>")
        sys.exit(1)
    run_tests(sys.argv[1])
