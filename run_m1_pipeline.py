"""
M1DataPipeline — Milestone 1 数据清洗与 ELT 工程化主程序
运行方式: python run_m1_pipeline.py

内存优化策略:
  1. 逐分区独立去重 + sink_parquet
     数据已按 behavior_type 分 4 区，重复只在同一分区内
     → 每次 unique() 的 hash set 仅为全量的 1/4
  2. n_unique 替换为 group_by.agg(len).select(len)，streaming 完全支持
  3. set_streaming_chunk_size 控制每批内存上限
  4. 所有 collect 均 streaming=True
"""

from __future__ import annotations

import logging
import sys
import shutil
import time
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import polars as pl

matplotlib.rcParams["font.family"] = ["SimHei", "Arial Unicode MS", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("M1Pipeline")

# behavior_type 已在 DEDUP_KEYS 中，分区内去重即可覆盖全量去重
DEDUP_KEYS = ["user_id", "item_id", "behavior_type", "timestamp"]
BEHAVIOR_TYPES = ["pv", "buy", "cart", "fav"]
ANOMALY_PERCENTILE = 0.99
# pv 分区 8900 万行，按 user_id % N 再分桶去重，每桶 ~500 万行
PV_BUCKETS = 16
# 小分区（buy/cart/fav）不需要分桶
SMALL_PART_THRESHOLD = 10_000_000


# ════════════════════════════════════════════════════════════════
class M1DataPipeline:
    """
    Milestone 1 数据处理管道。

    阶段:
        extract()   — 扫描分区 Parquet，验证 schema
        transform() — 逐分区去重落盘 → 扫输出做指标（全程 streaming）
        load()      — 生成图表 + 写报告
        run()       — 串联三阶段，计时，格式化汇总输出
    """

    def __init__(self, source_dir: str, output_dir: str) -> None:
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.out_parquet = self.output_dir / "m1_final_clean.parquet"
        self.dedup_dir = self.output_dir / "_dedup_parts"
        # 每次初始化时清理临时目录，确保无残留
        if self.dedup_dir.exists():
            shutil.rmtree(self.dedup_dir)
        self.dedup_dir.mkdir(parents=True, exist_ok=True)
        pl.Config.set_streaming_chunk_size(50_000)

    # ──────────────────────────── EXTRACT ────────────────────────────
    def extract(self) -> None:
        """验证 schema，返回各分区路径列表。"""
        log.info("=== EXTRACT 阶段开始 ===")
        try:
            # 用单个分区验证 schema，不扫全量
            sample_glob = (self.source_dir / "behavior_type=pv" / "*.parquet").as_posix()
            schema = pl.scan_parquet(sample_glob).collect_schema()
            required = {"user_id", "item_id", "category_id", "timestamp"}
            missing = required - set(schema.names())
            if missing:
                raise ValueError(f"缺少必要字段: {missing}")
            log.info("字段列表: %s", schema.names())
            log.info("EXTRACT 完成")
        except Exception as exc:
            log.error("EXTRACT 失败: %s", exc)
            raise

    def _dedup_partition(self, bt: str) -> None:
        """
        两步去重（大分区专用，避免重复全量扫描）:
          Step 1: 流式写出 → 按 user_id hash 分桶（纯路由，无 hash set）
          Step 2: 逐桶 group_by(dedup_keys).agg(first) 去重（streaming 支持）
        小分区直接一步去重。
        注: unique() 不支持 streaming engine，改用 group_by+agg(first) 等价替代。
        """
        part_glob = (self.source_dir / f"behavior_type={bt}" / "*.parquet").as_posix()
        # behavior_type 作为 literal 列加入，不参与 group_by key（已固定）
        part_dedup_keys = [k for k in DEDUP_KEYS if k != "behavior_type"]
        base_lf = (
            pl.scan_parquet(part_glob)
            .with_columns(pl.lit(bt).alias("behavior_type"))
        )
        part_dir = self.dedup_dir / bt
        part_dir.mkdir(parents=True, exist_ok=True)

        def streaming_dedup(lf: pl.LazyFrame) -> pl.LazyFrame:
            """group_by(dedup_keys).agg(first) 等价于 unique，且支持 streaming。"""
            return (
                lf.group_by(part_dedup_keys)
                .agg(
                    pl.first("category_id"),
                    pl.first("behavior_type"),
                )
            )

        n_rows = base_lf.select(pl.len()).collect(engine="streaming").item()

        if n_rows <= SMALL_PART_THRESHOLD:
            log.info("  %-4s: %s 行，直接去重...", bt, f"{n_rows:,}")
            streaming_dedup(base_lf).sink_parquet(
                (part_dir / "data.parquet").as_posix(),
                compression="snappy",
            )
            return

        log.info("  %-4s: %s 行，两步分桶去重（%d 桶）...", bt, f"{n_rows:,}", PV_BUCKETS)

        # Step 1: 流式分桶写出（无 hash set，纯路由）
        bucket_raw_dir = self.dedup_dir / f"{bt}_raw_buckets"
        bucket_raw_dir.mkdir(parents=True, exist_ok=True)
        (
            base_lf
            .with_columns(
                (pl.col("user_id").hash(seed=42) % PV_BUCKETS).cast(pl.Int32).alias("_bucket")
            )
            .sink_parquet(
                pl.PartitionBy(
                    bucket_raw_dir.as_posix(),
                    key="_bucket",
                    include_key=False,
                ),
                compression="snappy",
            )
        )
        log.info("    Step1 分桶写出完成")

        # Step 2: 逐桶 streaming 去重
        for bucket in range(PV_BUCKETS):
            bucket_files = list(bucket_raw_dir.glob(f"_bucket={bucket}/*.parquet"))
            if not bucket_files:
                continue
            bucket_out = part_dir / f"bucket_{bucket:03d}.parquet"
            streaming_dedup(
                pl.scan_parquet([f.as_posix() for f in bucket_files])
            ).sink_parquet(bucket_out.as_posix(), compression="snappy")
        log.info("    Step2 逐桶去重完成")
        log.info("    Step2 逐桶去重完成")

    # ──────────────────────────── TRANSFORM ──────────────────────────
    def transform(self) -> dict:
        """
        内存安全转换:
          1. 逐分区统计原始行数
          2. 大分区(pv) hash 分桶去重，小分区直接去重，全部 sink_parquet
          3. 扫合并输出做指标统计（engine=streaming，group_by 替代 n_unique）
        """
        log.info("=== TRANSFORM 阶段开始 ===")
        metrics: dict = {}

        try:
            # ── 1. 逐分区统计原始行数 ─────────────────────────────────
            log.info("[1/4] 逐分区统计原始行数...")
            t0 = time.time()
            raw_count = 0
            part_counts: dict[str, int] = {}
            for bt in BEHAVIOR_TYPES:
                part_files = list((self.source_dir / f"behavior_type={bt}").glob("*.parquet"))
                if not part_files:
                    continue
                part_glob = (self.source_dir / f"behavior_type={bt}" / "*.parquet").as_posix()
                n = pl.scan_parquet(part_glob).select(pl.len()).collect(engine="streaming").item()
                part_counts[bt] = n
                raw_count += n
                log.info("  %-4s: %s 行", bt, f"{n:,}")
            metrics["raw_rows"] = raw_count
            log.info("原始总行数: %s，耗时 %.1fs", f"{raw_count:,}", time.time() - t0)

            # ── 2. 逐分区去重 + sink ──────────────────────────────────
            log.info("[2/4] 逐分区去重并 sink_parquet...")
            t0 = time.time()
            for bt in BEHAVIOR_TYPES:
                if bt not in part_counts:
                    continue
                self._dedup_partition(bt)
                log.info("  %-4s 去重完成", bt)

            # 合并所有分区桶为最终输出（只扫 bt/ 子目录，排除 _raw_buckets）
            dedup_files = []
            for bt in BEHAVIOR_TYPES:
                bt_dir = self.dedup_dir / bt
                if bt_dir.exists():
                    dedup_files.extend(bt_dir.glob("*.parquet"))

            # 过滤 buy_count > pv_count 的违规用户（数据源缺失 pv 记录）
            raw_merged = pl.scan_parquet([p.as_posix() for p in dedup_files])
            valid_users = (
                raw_merged
                .group_by("user_id")
                .agg([
                    (pl.col("behavior_type") == "buy").sum().alias("buy_cnt"),
                    (pl.col("behavior_type") == "pv").sum().alias("pv_cnt"),
                ])
                .filter(pl.col("buy_cnt") <= pl.col("pv_cnt"))
                .select("user_id")
            )
            (
                raw_merged
                .join(valid_users, on="user_id", how="inner")
                .sink_parquet(self.out_parquet.as_posix(), compression="snappy")
            )
            file_mb = self.out_parquet.stat().st_size / 1024 / 1024
            log.info("合并完成，输出: %.2f MB，耗时 %.1fs", file_mb, time.time() - t0)
            metrics["file_mb"] = file_mb

            out_lf = pl.scan_parquet(self.out_parquet.as_posix())

            # ── 3. 行为分布（单次 group_by）──────────────────────────
            log.info("[3/4] 统计行为分布...")
            t0 = time.time()
            behavior_dist = (
                out_lf
                .group_by("behavior_type")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .collect(engine="streaming")
            )
            dedup_count = int(behavior_dist["count"].sum())
            metrics["dedup_rows"] = dedup_count
            metrics["removed_rows"] = raw_count - dedup_count
            metrics["duplicate_ratio"] = metrics["removed_rows"] / raw_count if raw_count else 0.0
            metrics["behavior_dist"] = behavior_dist
            log.info(
                "去重后: %s 行，移除 %s 行（%.4f%%），耗时 %.1fs",
                f"{dedup_count:,}", f"{metrics['removed_rows']:,}",
                metrics["duplicate_ratio"] * 100, time.time() - t0,
            )
            for row in behavior_dist.iter_rows(named=True):
                log.info("  %-6s  %s 行  (%.2f%%)",
                         row["behavior_type"], f"{row['count']:,}",
                         row["count"] / dedup_count * 100)

            # ── 4a. 漏斗分析（group_by 替代 n_unique）────────────────
            log.info("[4/4] 漏斗分析 + 异常账号检测...")
            t0 = time.time()

            def count_unique_users(lf: pl.LazyFrame) -> int:
                return (
                    lf.select("user_id")
                    .group_by("user_id").agg(pl.len())
                    .select(pl.len())
                    .collect(engine="streaming")
                    .item()
                )

            pv_users = count_unique_users(out_lf.filter(pl.col("behavior_type") == "pv"))
            mid_users = count_unique_users(
                out_lf.filter(pl.col("behavior_type").is_in(["fav", "cart"]))
            )
            buy_users = count_unique_users(out_lf.filter(pl.col("behavior_type") == "buy"))

            metrics["funnel"] = {
                "pv_users": pv_users,
                "mid_users": mid_users,
                "buy_users": buy_users,
                "mid_from_pv": mid_users / pv_users if pv_users else 0,
                "buy_from_pv": buy_users / pv_users if pv_users else 0,
                "buy_from_mid": buy_users / mid_users if mid_users else 0,
            }
            log.info(
                "漏斗: PV用户 %s → 收藏/加购 %s (%.2f%%) → 购买 %s (%.2f%%)，耗时 %.1fs",
                f"{pv_users:,}", f"{mid_users:,}",
                metrics["funnel"]["mid_from_pv"] * 100,
                f"{buy_users:,}",
                metrics["funnel"]["buy_from_pv"] * 100,
                time.time() - t0,
            )

            # ── 4b. 异常账号检测 ──────────────────────────────────────
            t0 = time.time()
            user_stats = (
                out_lf
                .group_by("user_id")
                .agg([
                    pl.len().alias("total_visits"),
                    (pl.col("behavior_type") != "pv").sum().alias("non_pv_count"),
                ])
                .collect(engine="streaming")
            )
            threshold = user_stats["total_visits"].quantile(ANOMALY_PERCENTILE)
            suspects = user_stats.filter(
                (pl.col("total_visits") > threshold) & (pl.col("non_pv_count") == 0)
            ).sort("total_visits", descending=True)

            total_users = len(user_stats)
            suspect_count = len(suspects)
            suspect_traffic = int(suspects["total_visits"].sum()) if suspect_count else 0
            metrics["anomaly"] = {
                "threshold": int(threshold),
                "total_users": total_users,
                "suspect_count": suspect_count,
                "suspect_traffic": suspect_traffic,
                "suspect_ratio": suspect_count / total_users if total_users else 0,
                "traffic_ratio": suspect_traffic / dedup_count if dedup_count else 0,
                "top_suspects": suspects.head(10),
            }
            log.info(
                "异常检测: 阈值=%d，嫌疑账号 %d 个（占 %.4f%%），嫌疑流量 %s 次（%.4f%%），耗时 %.1fs",
                int(threshold), suspect_count,
                metrics["anomaly"]["suspect_ratio"] * 100,
                f"{suspect_traffic:,}",
                metrics["anomaly"]["traffic_ratio"] * 100,
                time.time() - t0,
            )

            log.info("TRANSFORM 完成")
            return metrics

        except Exception as exc:
            log.error("TRANSFORM 失败: %s", exc)
            raise

    # ──────────────────────────── LOAD ───────────────────────────────
    def load(self, metrics: dict) -> None:
        log.info("=== LOAD 阶段开始 ===")
        try:
            self._write_report_charts(metrics)
            self._write_chart(metrics)
            self._write_report(metrics)
            log.info("LOAD 完成")
        except Exception as exc:
            log.error("LOAD 失败: %s", exc)
            raise

    def _write_report_charts(self, metrics: dict) -> None:
        """生成适合实验报告直接插入的独立图表。"""
        self._write_dedup_chart(metrics)
        self._write_funnel_chart(metrics)
        self._write_anomaly_chart(metrics)
        self._write_final_chart(metrics)

    def _write_dedup_chart(self, metrics: dict) -> None:
        """输出去重前后对比 + 行为分布图。"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # 左图：去重前后行数对比
        ax1 = axes[0]
        labels = ["原始数据", "去重后数据"]
        values = [metrics["raw_rows"], metrics["dedup_rows"]]
        colors = ["#DD8452", "#4C72B0"]
        bars = ax1.bar(labels, values, color=colors, width=0.55, edgecolor="white")
        max_val = max(values)
        for bar, val in zip(bars, values):
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                val + max_val * 0.01,
                f"{val:,}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )
        ax1.set_title("去重前后数据量对比", fontsize=13, fontweight="bold")
        ax1.set_ylabel("记录数", fontsize=11)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax1.grid(axis="y", alpha=0.3)
        ax1.text(
            0.5,
            0.92,
            f"移除重复: {metrics['removed_rows']:,} 行\n重复占比: {metrics['duplicate_ratio']*100:.4f}%",
            transform=ax1.transAxes,
            ha="center",
            va="top",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.35", "facecolor": "#FFF7E6", "edgecolor": "#DD8452"},
        )

        # 右图：行为分布柱状图
        ax2 = axes[1]
        dist = metrics["behavior_dist"].sort("count", descending=True)
        behaviors = dist["behavior_type"].to_list()
        counts = dist["count"].to_list()
        bar_colors = ["#4C72B0", "#55A868", "#8172B3", "#C44E52"]
        bars = ax2.bar(behaviors, counts, color=bar_colors[:len(behaviors)], width=0.55, edgecolor="white")
        max_count = max(counts)
        for bar, count in zip(bars, counts):
            pct = count / metrics["dedup_rows"] * 100
            ax2.text(
                bar.get_x() + bar.get_width() / 2,
                count + max_count * 0.01,
                f"{count:,}\n({pct:.2f}%)",
                ha="center",
                va="bottom",
                fontsize=9,
            )
        ax2.set_title("去重后行为类型分布", fontsize=13, fontweight="bold")
        ax2.set_ylabel("记录数", fontsize=11)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax2.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        out_path = self.output_dir / "task1_dedup_chart.png"
        fig.savefig(out_path.as_posix(), dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("实验报告图已保存: %s", out_path)

    def _write_funnel_chart(self, metrics: dict) -> None:
        """输出用户行为漏斗分析图。"""
        funnel = metrics["funnel"]
        stages = ["浏览(PV)", "收藏/加购", "购买"]
        users = [funnel["pv_users"], funnel["mid_users"], funnel["buy_users"]]
        rates = [1.0, funnel["mid_from_pv"], funnel["buy_from_pv"]]

        fig, ax = plt.subplots(figsize=(9, 6))
        colors = ["#4C72B0", "#55A868", "#C44E52"]
        bars = ax.bar(stages, users, color=colors, width=0.58, edgecolor="white")

        max_users = max(users)
        for bar, user_cnt, rate in zip(bars, users, rates):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                user_cnt + max_users * 0.015,
                f"{user_cnt:,}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                user_cnt / 2,
                f"{rate*100:.2f}%",
                ha="center",
                va="center",
                fontsize=11,
                color="white",
                fontweight="bold",
            )

        ax.annotate(
            f"收藏/加购 → 购买转化率: {funnel['buy_from_mid']*100:.2f}%",
            xy=(1.5, max_users * 0.9),
            xytext=(1.5, max_users * 1.02),
            ha="center",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.35", "facecolor": "#F2F8FF", "edgecolor": "#4C72B0"},
        )
        ax.set_title("用户行为转化漏斗", fontsize=14, fontweight="bold")
        ax.set_ylabel("用户数", fontsize=11)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        out_path = self.output_dir / "task3_funnel_chart.png"
        fig.savefig(out_path.as_posix(), dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("实验报告图已保存: %s", out_path)

    def _write_anomaly_chart(self, metrics: dict) -> None:
        """输出异常账号检测图。"""
        anomaly = metrics["anomaly"]
        top = anomaly["top_suspects"]

        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # 左图：异常账号概览
        ax1 = axes[0]
        labels = ["全量用户", "嫌疑账号"]
        values = [anomaly["total_users"], anomaly["suspect_count"]]
        colors = ["#4C72B0", "#C44E52"]
        bars = ax1.bar(labels, values, color=colors, width=0.55, edgecolor="white")
        max_val = max(values) if values else 1
        for bar, val in zip(bars, values):
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                val + max_val * 0.01,
                f"{val:,}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )
        ax1.set_title("异常账号总体情况", fontsize=13, fontweight="bold")
        ax1.set_ylabel("用户数", fontsize=11)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax1.grid(axis="y", alpha=0.3)
        ax1.text(
            0.5,
            0.92,
            f"99分位阈值: {anomaly['threshold']}\n异常占比: {anomaly['suspect_ratio']*100:.4f}%",
            transform=ax1.transAxes,
            ha="center",
            va="top",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.35", "facecolor": "#FFF5F5", "edgecolor": "#C44E52"},
        )

        # 右图：Top10 异常账号
        ax2 = axes[1]
        if len(top) > 0:
            uids = [str(u) for u in top["user_id"].to_list()]
            visits = top["total_visits"].to_list()
            y_pos = list(range(len(uids)))
            bars = ax2.barh(y_pos, visits, color="#C44E52", edgecolor="white")
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels(uids, fontsize=8)
            ax2.invert_yaxis()
            for bar, val in zip(bars, visits):
                ax2.text(
                    val + max(visits) * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    str(val),
                    va="center",
                    fontsize=8,
                )
            ax2.axvline(
                anomaly["threshold"],
                color="#4C72B0",
                linestyle="--",
                linewidth=1.5,
                label=f"阈值={anomaly['threshold']}",
            )
            ax2.legend(fontsize=9)
        else:
            ax2.text(0.5, 0.5, "未检测到异常账号", ha="center", va="center", fontsize=12)
        ax2.set_title("异常账号 Top10（访问量）", fontsize=13, fontweight="bold")
        ax2.set_xlabel("总访问次数", fontsize=11)
        ax2.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        out_path = self.output_dir / "task4_anomaly_chart.png"
        fig.savefig(out_path.as_posix(), dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("实验报告图已保存: %s", out_path)

    def _write_final_chart(self, metrics: dict) -> None:
        """输出适合实验报告结尾展示的总览 KPI 图。"""
        fig = plt.figure(figsize=(14, 8))
        gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.2])
        fig.suptitle("M1 最终成果总览", fontsize=16, fontweight="bold")

        # 左上：关键 KPI
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis("off")
        kpi_lines = [
            f"原始总行数: {metrics['raw_rows']:,}",
            f"去重后总行数: {metrics['dedup_rows']:,}",
            f"移除重复行数: {metrics['removed_rows']:,}",
            f"重复占比: {metrics['duplicate_ratio']*100:.4f}%",
            f"输出文件大小: {metrics['file_mb']:.2f} MB",
        ]
        ax1.text(
            0.03,
            0.95,
            "数据质量与产出规模",
            fontsize=14,
            fontweight="bold",
            va="top",
        )
        ax1.text(
            0.03,
            0.78,
            "\n".join(kpi_lines),
            fontsize=11,
            va="top",
            linespacing=1.8,
            bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F6F8FB", "edgecolor": "#4C72B0"},
        )

        # 右上：漏斗 KPI
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.axis("off")
        funnel = metrics["funnel"]
        funnel_lines = [
            f"PV 用户数: {funnel['pv_users']:,}",
            f"收藏/加购用户数: {funnel['mid_users']:,}",
            f"购买用户数: {funnel['buy_users']:,}",
            f"PV→收藏/加购: {funnel['mid_from_pv']*100:.2f}%",
            f"PV→购买: {funnel['buy_from_pv']*100:.2f}%",
            f"收藏/加购→购买: {funnel['buy_from_mid']*100:.2f}%",
        ]
        ax2.text(
            0.03,
            0.95,
            "用户转化漏斗",
            fontsize=14,
            fontweight="bold",
            va="top",
        )
        ax2.text(
            0.03,
            0.78,
            "\n".join(funnel_lines),
            fontsize=11,
            va="top",
            linespacing=1.8,
            bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F5FBF6", "edgecolor": "#55A868"},
        )

        # 左下：行为分布
        ax3 = fig.add_subplot(gs[1, 0])
        dist = metrics["behavior_dist"].sort("count", descending=True)
        behaviors = dist["behavior_type"].to_list()
        counts = dist["count"].to_list()
        bars = ax3.bar(behaviors, counts, color=["#4C72B0", "#55A868", "#8172B3", "#C44E52"], width=0.55)
        for bar, count in zip(bars, counts):
            ax3.text(
                bar.get_x() + bar.get_width() / 2,
                count + max(counts) * 0.01,
                f"{count:,}",
                ha="center",
                va="bottom",
                fontsize=9,
            )
        ax3.set_title("行为类型分布", fontsize=13, fontweight="bold")
        ax3.set_ylabel("记录数", fontsize=11)
        ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax3.grid(axis="y", alpha=0.3)

        # 右下：异常账号摘要
        ax4 = fig.add_subplot(gs[1, 1])
        anomaly = metrics["anomaly"]
        summary_labels = ["嫌疑账号数", "嫌疑流量请求数"]
        summary_values = [anomaly["suspect_count"], anomaly["suspect_traffic"]]
        bars = ax4.bar(summary_labels, summary_values, color=["#C44E52", "#DD8452"], width=0.55)
        max_summary = max(summary_values) if summary_values else 1
        for bar, val in zip(bars, summary_values):
            ax4.text(
                bar.get_x() + bar.get_width() / 2,
                val + max_summary * 0.01,
                f"{val:,}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )
        ax4.set_title("异常账号检测摘要", fontsize=13, fontweight="bold")
        ax4.text(
            0.98,
            0.95,
            f"阈值: {anomaly['threshold']}\n账号占比: {anomaly['suspect_ratio']*100:.4f}%\n流量占比: {anomaly['traffic_ratio']*100:.4f}%",
            transform=ax4.transAxes,
            ha="right",
            va="top",
            fontsize=10,
            bbox={"boxstyle": "round,pad=0.35", "facecolor": "#FFF5F5", "edgecolor": "#C44E52"},
        )
        ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax4.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        out_path = self.output_dir / "task5_final_chart.png"
        fig.savefig(out_path.as_posix(), dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("实验报告图已保存: %s", out_path)

    def _write_chart(self, metrics: dict) -> None:
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle("M1 数据管道分析汇总", fontsize=16, fontweight="bold", y=1.02)

        # 子图1: 行为分布饼图
        ax1 = axes[0]
        dist = metrics["behavior_dist"]
        labels = dist["behavior_type"].to_list()
        sizes = dist["count"].to_list()
        colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
        _, _, autotexts = ax1.pie(
            sizes, labels=labels, autopct="%1.2f%%",
            colors=colors[:len(labels)], startangle=90,
            wedgeprops={"edgecolor": "white", "linewidth": 1.5},
        )
        for at in autotexts:
            at.set_fontsize(9)
        ax1.set_title("行为类型分布", fontsize=13, fontweight="bold", pad=12)

        # 子图2: 漏斗柱状图
        ax2 = axes[1]
        funnel = metrics["funnel"]
        stages = ["浏览(PV)", "收藏/加购", "购买"]
        users = [funnel["pv_users"], funnel["mid_users"], funnel["buy_users"]]
        bar_colors = ["#4C72B0", "#55A868", "#C44E52"]
        bars = ax2.bar(stages, users, color=bar_colors, width=0.5, edgecolor="white")
        for bar, val in zip(bars, users):
            ax2.text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + max(users) * 0.01,
                     f"{val:,}", ha="center", va="bottom", fontsize=9)
        rates = ["100%", f"{funnel['mid_from_pv']*100:.1f}%", f"{funnel['buy_from_pv']*100:.1f}%"]
        for bar, rate in zip(bars, rates):
            ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                     rate, ha="center", va="center",
                     fontsize=10, fontweight="bold", color="white")
        ax2.set_title("用户行为漏斗", fontsize=13, fontweight="bold")
        ax2.set_ylabel("用户数", fontsize=11)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax2.grid(axis="y", alpha=0.3)

        # 子图3: 异常账号 Top10
        ax3 = axes[2]
        top = metrics["anomaly"]["top_suspects"]
        if len(top) > 0:
            uids = [str(u) for u in top["user_id"].to_list()]
            visits = top["total_visits"].to_list()
            y_pos = list(range(len(uids)))
            hbars = ax3.barh(y_pos, visits, color="#C44E52", edgecolor="white")
            ax3.set_yticks(y_pos)
            ax3.set_yticklabels(uids, fontsize=8)
            ax3.invert_yaxis()
            for bar, val in zip(hbars, visits):
                ax3.text(val + max(visits) * 0.01,
                         bar.get_y() + bar.get_height() / 2,
                         str(val), va="center", fontsize=8)
            ax3.axvline(metrics["anomaly"]["threshold"], color="#4C72B0",
                        linestyle="--", linewidth=1.5,
                        label=f"阈值={metrics['anomaly']['threshold']}")
            ax3.legend(fontsize=9)
        ax3.set_title("嫌疑异常账号 Top10（访问量）", fontsize=13, fontweight="bold")
        ax3.set_xlabel("总访问次数", fontsize=11)
        ax3.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        out_path = self.output_dir / "pipeline_summary_chart.png"
        fig.savefig(out_path.as_posix(), dpi=150, bbox_inches="tight")
        plt.close(fig)
        log.info("图表已保存: %s", out_path)

    def _write_report(self, metrics: dict) -> None:
        funnel = metrics["funnel"]
        anomaly = metrics["anomaly"]
        dist = metrics["behavior_dist"]
        dedup_count = metrics["dedup_rows"]

        sep = "=" * 88
        lines = [
            sep,
            "  M1 数据管道执行报告",
            sep,
            "",
            "【一、数据量与去重统计】",
            f"  原始总行数       : {metrics['raw_rows']:>15,}",
            f"  去重后总行数     : {dedup_count:>15,}",
            f"  移除重复行数     : {metrics['removed_rows']:>15,}",
            f"  重复占比         : {metrics['duplicate_ratio']*100:>14.4f}%",
            f"  输出文件大小     : {metrics['file_mb']:>14.2f} MB",
            "",
            "【二、行为类型分布】",
        ]
        for row in dist.iter_rows(named=True):
            pct = row["count"] / dedup_count * 100
            lines.append(f"  {row['behavior_type']:<8}: {row['count']:>12,}  ({pct:.2f}%)")

        lines += [
            "",
            "【三、用户行为漏斗】",
            f"  浏览(PV) 用户数  : {funnel['pv_users']:>15,}  (100.00%)",
            f"  收藏/加购 用户数 : {funnel['mid_users']:>15,}  ({funnel['mid_from_pv']*100:.2f}%)",
            f"  购买 用户数      : {funnel['buy_users']:>15,}  ({funnel['buy_from_pv']*100:.2f}%)",
            f"  收藏/加购→购买   : {funnel['buy_from_mid']*100:>14.2f}%",
            "",
            "【四、异常账号检测】",
            f"  99分位阈值(访问) : {anomaly['threshold']:>15,}",
            f"  全量用户数       : {anomaly['total_users']:>15,}",
            f"  嫌疑账号数       : {anomaly['suspect_count']:>15,}  ({anomaly['suspect_ratio']*100:.4f}%)",
            f"  嫌疑流量请求数   : {anomaly['suspect_traffic']:>15,}  ({anomaly['traffic_ratio']*100:.4f}%)",
            "",
            sep,
        ]

        report_text = "\n".join(lines)
        out_path = self.output_dir / "pipeline_report.txt"
        out_path.write_text(report_text, encoding="utf-8")
        print("\n" + report_text)
        log.info("报告已保存: %s", out_path)

    # ──────────────────────────── RUN ────────────────────────────────
    def run(self) -> None:
        wall_start = time.time()
        log.info("╔══════════════════════════════════════╗")
        log.info("║   M1DataPipeline  启动               ║")
        log.info("╚══════════════════════════════════════╝")
        log.info("数据源目录 : %s", self.source_dir.resolve())
        log.info("输出目录   : %s", self.output_dir.resolve())

        self.extract()
        metrics = self.transform()
        self.load(metrics)

        elapsed = time.time() - wall_start
        log.info("╔══════════════════════════════════════╗")
        log.info("║   全流程完成  总耗时 %.1f 秒          ║", elapsed)
        log.info("╚══════════════════════════════════════╝")


# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    _root = Path(__file__).parent.parent  # test4/ -> test/
    pipeline = M1DataPipeline(
        source_dir=str(_root / "test2" / "clean_data_partitioned"),
        output_dir=str(Path(__file__).parent / "output"),
    )
    pipeline.run()
