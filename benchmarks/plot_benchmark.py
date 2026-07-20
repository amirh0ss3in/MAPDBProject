#!/usr/bin/env python3
"""Render the 0.5x/1x/2x throughput-vs-backlog benchmark figure from the raw
probe logs. Reads backlog_r{05,10,20}.log, writes benchmark.png."""
import re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# --- validated palette (dataviz skill reference instance, light mode) ---
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"
BLUE = "#2a78d6"     # producer throughput
GREEN = "#008300"    # Spark-side throughput
ORANGE = "#eb6834"   # backlog column

CHUNK_BYTES = 2 * 1 * 1024 * 1024  # 1MB/channel x2 (i+q) at --chunk-mb default of 1.0
MA_WINDOW = 5  # ~10s at the probe's 2s sampling interval

plt.rcParams["font.family"] = ["DejaVu Sans", "sans-serif"]
plt.rcParams["axes.edgecolor"] = AXIS
plt.rcParams["text.color"] = INK
plt.rcParams["axes.labelcolor"] = INK_SECONDARY
plt.rcParams["xtick.color"] = INK_MUTED
plt.rcParams["ytick.color"] = INK_MUTED

RUNS = [
    ("benchmark_8mbps.log", "0.5x — target 8 MB/s", 8.0),
    ("benchmark_16mbps.log", "1.0x — target 16 MB/s", 16.0),
    ("benchmark_32mbps.log", "2.0x — target 32 MB/s", 32.0),
]

LINE_RE = re.compile(r"t=([\d.]+)s end=(\d+) committed=(\d+) lag=(\d+)")


def parse(path):
    ts, ends, comms, lags = [], [], [], []
    with open(path) as f:
        for line in f:
            m = LINE_RE.match(line)
            if m:
                ts.append(float(m.group(1)))
                ends.append(int(m.group(2)))
                comms.append(int(m.group(3)))
                lags.append(int(m.group(4)))
    return np.array(ts), np.array(ends), np.array(comms), np.array(lags)


TAIL_BUFFER_S = 12.0  # confirm drain-to-zero without dragging in the rest of the idle log


def trim_to_active(ts, ends, comms, lags):
    """Drop the leading dead time before the producer actually starts sending
    (Spark cluster cold-start) and the long idle tail after it's done, keeping just
    enough of the tail to confirm backlog actually returns to and stays at zero."""
    diffs = np.diff(ends)
    nonzero = np.flatnonzero(diffs > 0)
    start = nonzero[0] if len(nonzero) else 0
    ts, ends, comms, lags = ts[start:] - ts[start], ends[start:], comms[start:], lags[start:]

    active = np.flatnonzero(lags > 0)
    last_active_t = ts[active[-1]] if len(active) else ts[-1]
    keep = ts <= last_active_t + TAIL_BUFFER_S
    return ts[keep], ends[keep], comms[keep], lags[keep]


def moving_average(x, window):
    if len(x) < window:
        return x.astype(float)
    pad = window // 2
    xp = np.pad(x.astype(float), (pad, pad), mode="edge")
    kernel = np.ones(window) / window
    return np.convolve(xp, kernel, mode="valid")[: len(x)]


def throughput_series(ts, ends):
    dt = np.diff(ts)
    dend = np.diff(ends)
    thr = (dend * CHUNK_BYTES) / dt / 1_000_000  # decimal MB/s
    t_mid = (ts[1:] + ts[:-1]) / 2
    return t_mid, thr


fig, axes = plt.subplots(3, 2, figsize=(11, 8.5), facecolor=SURFACE)

for row, (fname, label, target_mb_s) in enumerate(RUNS):
    ts, ends, comms, lags = parse(fname)
    ts, ends, comms, lags = trim_to_active(ts, ends, comms, lags)

    t_thr, thr = throughput_series(ts, ends)
    thr_ma = moving_average(thr, MA_WINDOW)
    # Spark-side throughput: rate at which the quax-processor group's committed
    # offset advances — i.e. chunks Spark has confirmed processing, not just received.
    t_proc, proc = throughput_series(ts, comms)
    proc_ma = moving_average(proc, MA_WINDOW)
    lag_ma = moving_average(lags, MA_WINDOW)

    ax_thr, ax_bl = axes[row, 0], axes[row, 1]

    for ax in (ax_thr, ax_bl):
        ax.set_facecolor(SURFACE)
        ax.grid(True, color=GRID, linewidth=0.7, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(AXIS)
        ax.spines["bottom"].set_color(AXIS)
        ax.tick_params(length=0)

    # --- throughput panel ---
    ax_thr.plot(t_thr, thr, color=BLUE, alpha=0.35, linewidth=1.0, marker="o",
                markersize=3, zorder=2, label="Producer (raw)")
    ax_thr.plot(t_thr, thr_ma, color=BLUE, linewidth=2.0, zorder=3,
                label=f"Producer ({MA_WINDOW}-sample moving avg)")
    ax_thr.plot(t_proc, proc_ma, color=GREEN, linewidth=2.0, zorder=3,
                label=f"Spark processed ({MA_WINDOW}-sample moving avg)")
    ax_thr.axhline(target_mb_s, color=INK_SECONDARY, linewidth=1.2, linestyle=(0, (4, 3)),
                   zorder=1, label="Target")
    ax_thr.set_ylim(0, target_mb_s * 1.6)
    ax_thr.set_ylabel("MB/s" if row == 1 else "")

    # --- backlog panel ---
    ax_bl.plot(ts, lags, color=ORANGE, alpha=0.35, linewidth=1.0, marker="o",
               markersize=3, zorder=2, label="Backlog (raw)")
    ax_bl.plot(ts, lag_ma, color=ORANGE, linewidth=2.0, zorder=3,
               label=f"Backlog ({MA_WINDOW}-sample moving avg)")
    ax_bl.set_ylim(bottom=0)
    ax_bl.set_ylabel("Chunks behind" if row == 1 else "")

    ax_thr.set_title(label, loc="left", color=INK, fontsize=11, fontweight="bold", pad=8)

    if row == 2:
        ax_thr.set_xlabel("Time since producer started (s)")
        ax_bl.set_xlabel("Time since producer started (s)")

axes[0, 0].set_title("Throughput: producer vs Spark", loc="right", color=INK_MUTED, fontsize=9, style="italic")
axes[0, 1].set_title("Processing backlog: is Spark keeping up?", loc="right", color=INK_MUTED, fontsize=9, style="italic")

handles, labels = axes[1, 0].get_legend_handles_labels()
handles2, labels2 = axes[1, 1].get_legend_handles_labels()
fig.legend(handles + handles2, labels + labels2, loc="lower center", ncol=3,
           frameon=False, fontsize=9, bbox_to_anchor=(0.5, -0.02))

fig.suptitle("QUAX pipeline benchmark: sustaining 0.5x / 1x / 2x the 16 MB/s DAQ target",
             fontsize=13, fontweight="bold", color=INK, y=0.995)
fig.tight_layout(rect=[0, 0.04, 1, 0.97])
fig.savefig("benchmark.png", dpi=200, facecolor=SURFACE, bbox_inches="tight")
print("saved benchmark.png")
