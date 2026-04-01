"""
benchmark.py — Pandas vs Polars Eager vs Polars Lazy 性能基准对比
运行方式: python benchmark.py
"""

from __future__ import annotations

import time
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl

matplotlib.rcParams["font.family"] = ["SimHei", "Arial Unicode MS", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

_ROOT = Path(__file__).parent.parent  # test4/ -> test/
SOURCE_GLOB = str(_ROOT / "test2" / "clean_data_partitioned" / "behavior_type=*" / "00000000.parquet")
OUTPUT_CHART = Path(__file__).parent / "output" / "benchmark_chart.png"
REPEAT = 3  # 每种方案重复次数，取最小值


def _all_parquet_files() -> list[str]:
    base = _ROOT / "test2" / "clean_data_partitioned"
    files = sorted(base.glob("behavior_type=*/00000000.parquet"))
    return [f.as_posix() for f in files]


def bench_pandas(files: list[str]) -> float:
    """Pandas: read_parquet + value_counts（全量加载到内存）。"""
    times = []
    for _ in range(REPEAT):
        t0 = time.perf_counter()
        frames = [pd.read_parquet(f) for f in files]
        df = pd.concat(frames, ignore_index=True)
        _ = df["behavior_type"].value_counts()
        times.append(time.perf_counter() - t0)
    return min(times)


def bench_polars_eager(files: list[str]) -> float:
    """Polars Eager: read_parquet + group_by（多次 collect）。"""
    times = []
    for _ in range(REPEAT):
        t0 = time.perf_counter()
        frames = [pl.read_parquet(f) for f in files]
        df = pl.concat(frames)
        _ = df.group_by("behavior_type").agg(pl.len().alias("count")).sort("count", descending=True)
        times.append(time.perf_counter() - t0)
    return min(times)


def bench_polars_lazy(files: list[str]) -> float:
    """Polars Lazy: scan_parquet + lazy group_by（单次 collect，predicate pushdown）。"""
    times = []
    for _ in range(REPEAT):
        t0 = time.perf_counter()
        lf = pl.scan_parquet(files)
        _ = (
            lf.group_by("behavior_type")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect()
        )
        times.append(time.perf_counter() - t0)
    return min(times)


def print_table(results: list[tuple[str, float, float]]) -> None:
    sep = "─" * 56
    print("\n" + "=" * 56)
    print("  性能基准对比结果（取 3 次最小值）")
    print("=" * 56)
    print(f"  {'方案':<20} {'耗时(s)':>10} {'相对Pandas加速比':>14}")
    print(sep)
    for name, elapsed, speedup in results:
        print(f"  {name:<20} {elapsed:>10.3f} {speedup:>13.2f}x")
    print("=" * 56 + "\n")


def write_chart(results: list[tuple[str, float, float]]) -> None:
    OUTPUT_CHART.parent.mkdir(parents=True, exist_ok=True)
    names = [r[0] for r in results]
    times = [r[1] for r in results]
    speedups = [r[2] for r in results]
    colors = ["#C44E52", "#DD8452", "#4C72B0"]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(names, times, color=colors, width=0.5, edgecolor="white", linewidth=1.2)

    for bar, t, sp in zip(bars, times, speedups):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(times) * 0.02,
            f"{t:.3f}s\n({sp:.1f}x)",
            ha="center", va="bottom", fontsize=11, fontweight="bold",
        )

    ax.set_title("Pandas vs Polars Eager vs Polars Lazy\n行为类型分布统计耗时对比", fontsize=14, fontweight="bold")
    ax.set_ylabel("耗时（秒）", fontsize=12)
    ax.set_xlabel("处理方案", fontsize=12)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(0, max(times) * 1.25)

    note = "数据集: 1亿行电商行为日志（分区 Parquet）\n重复 3 次取最小值"
    ax.text(0.98, 0.97, note, transform=ax.transAxes,
            ha="right", va="top", fontsize=9, color="gray",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7))

    plt.tight_layout()
    fig.savefig(OUTPUT_CHART.as_posix(), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"图表已保存: {OUTPUT_CHART.resolve()}")


def main() -> None:
    files = _all_parquet_files()
    if not files:
        print("未找到分区 Parquet 文件，请确认路径: ../test2/clean_data_partitioned/")
        return

    print(f"找到 {len(files)} 个分区文件，开始基准测试（每方案重复 {REPEAT} 次）...\n")

    print("  [1/3] Pandas Eager ...")
    t_pandas = bench_pandas(files)
    print(f"        完成: {t_pandas:.3f}s")

    print("  [2/3] Polars Eager ...")
    t_polars_eager = bench_polars_eager(files)
    print(f"        完成: {t_polars_eager:.3f}s")

    print("  [3/3] Polars Lazy ...")
    t_polars_lazy = bench_polars_lazy(files)
    print(f"        完成: {t_polars_lazy:.3f}s")

    results = [
        ("Pandas Eager",    t_pandas,       1.0),
        ("Polars Eager",    t_polars_eager, t_pandas / t_polars_eager if t_polars_eager else 0),
        ("Polars Lazy",     t_polars_lazy,  t_pandas / t_polars_lazy  if t_polars_lazy  else 0),
    ]

    print_table(results)
    write_chart(results)


if __name__ == "__main__":
    main()
