#!/usr/bin/env python3
"""Generate StockEx Developer's Guide PDF with flow diagrams."""

import os
import sys
import textwrap
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

from fpdf import FPDF

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(OUT_DIR, "StockEx_Developer_Guide.pdf")

# ── Colours ───────────────────────────────────────────────────────────────────
C_PRIMARY   = "#1a237e"
C_SECONDARY = "#283593"
C_ACCENT    = "#42a5f5"
C_KAFKA     = "#e65100"
C_SERVICE   = "#1565c0"
C_DB        = "#2e7d32"
C_AI        = "#6a1b9a"
C_RL        = "#00695c"
C_LLM       = "#bf360c"
C_BG        = "#f5f5f5"
C_LIGHT     = "#e3f2fd"


def hex_to_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


# ── Diagram helpers ───────────────────────────────────────────────────────────

def save_fig_to_bytes(fig) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def draw_box(ax, x, y, w, h, label, color=C_SERVICE, fontsize=8, sublabel=None):
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02",
                         facecolor=color, edgecolor="#333", linewidth=0.8, alpha=0.85)
    ax.add_patch(box)
    if sublabel:
        ax.text(x + w/2, y + h*0.62, label, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", color="white")
        ax.text(x + w/2, y + h*0.32, sublabel, ha="center", va="center",
                fontsize=fontsize-1.5, color="#ddd")
    else:
        ax.text(x + w/2, y + h/2, label, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", color="white")


def draw_arrow(ax, x1, y1, x2, y2, color="#555", style="->"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color, lw=1.2))


def draw_arrow_label(ax, x1, y1, x2, y2, label, color="#555"):
    draw_arrow(ax, x1, y1, x2, y2, color)
    mx, my = (x1+x2)/2, (y1+y2)/2
    ax.text(mx, my + 0.02, label, ha="center", va="bottom", fontsize=5.5, color=color)


# ── Diagram 1: System Architecture ────────────────────────────────────────────

def diagram_architecture():
    fig, ax = plt.subplots(1, 1, figsize=(10, 6.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("System Architecture — Single Container", fontsize=12,
                 fontweight="bold", color=C_PRIMARY, pad=15)

    # nginx
    draw_box(ax, 0.30, 0.90, 0.40, 0.07, "nginx :7860", "#546e7a", 9, "Reverse Proxy")

    # Row 1: input services
    draw_box(ax, 0.02, 0.74, 0.18, 0.10, "MD Feeder", C_SERVICE, 8, "Regime Engine")
    draw_box(ax, 0.22, 0.74, 0.15, 0.10, "FIX OEG", C_SERVICE, 8, ":5001")
    draw_box(ax, 0.39, 0.74, 0.15, 0.10, "FIX UI", C_SERVICE, 8, ":5002")
    draw_box(ax, 0.56, 0.74, 0.15, 0.10, "Frontend", C_SERVICE, 8, ":5003")
    draw_box(ax, 0.73, 0.74, 0.18, 0.10, "AI Analyst", C_AI, 8, "LLM")

    # Kafka
    draw_box(ax, 0.10, 0.55, 0.80, 0.10, "Apache Kafka (KRaft)", C_KAFKA, 10,
             "orders  |  trades  |  snapshots  |  control  |  ai_insights")

    # Row 2: core services
    draw_box(ax, 0.05, 0.32, 0.20, 0.12, "Matcher", C_SERVICE, 9, ":6000  SQLite")
    draw_box(ax, 0.30, 0.32, 0.25, 0.12, "Dashboard", C_SERVICE, 9, ":5000  SSE + OHLCV")
    draw_box(ax, 0.60, 0.32, 0.28, 0.12, "Clearing House", C_RL, 9, ":5004  RL + LLM Members")

    # Databases
    draw_box(ax, 0.07, 0.17, 0.16, 0.07, "matcher.db", C_DB, 7, "SQLite")
    draw_box(ax, 0.32, 0.17, 0.16, 0.07, "OHLCV.db", C_DB, 7, "SQLite")
    draw_box(ax, 0.64, 0.17, 0.20, 0.07, "clearing_house.db", C_DB, 7, "SQLite")

    # Browser
    draw_box(ax, 0.30, 0.03, 0.40, 0.07, "Browser", "#37474f", 10, "SSE / REST")

    # Arrows: nginx → services
    for x in [0.11, 0.295, 0.465, 0.635]:
        draw_arrow(ax, 0.50, 0.90, x + 0.05, 0.84, "#78909c")

    # Arrows: services → kafka
    for x in [0.11, 0.295, 0.465, 0.635, 0.82]:
        draw_arrow(ax, x + 0.05, 0.74, x + 0.05, 0.65, C_KAFKA)

    # Arrows: kafka → consumers
    draw_arrow(ax, 0.25, 0.55, 0.15, 0.44, C_KAFKA)
    draw_arrow(ax, 0.50, 0.55, 0.42, 0.44, C_KAFKA)
    draw_arrow(ax, 0.70, 0.55, 0.74, 0.44, C_KAFKA)

    # Arrows: services → DBs
    draw_arrow(ax, 0.15, 0.32, 0.15, 0.24, C_DB)
    draw_arrow(ax, 0.42, 0.32, 0.40, 0.24, C_DB)
    draw_arrow(ax, 0.74, 0.32, 0.74, 0.24, C_DB)

    # Dashboard → browser
    draw_arrow(ax, 0.42, 0.32, 0.50, 0.10, "#37474f")

    return save_fig_to_bytes(fig)


# ── Diagram 2: Message Flow ──────────────────────────────────────────────────

def diagram_message_flow():
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("Kafka Message Flow", fontsize=12, fontweight="bold",
                 color=C_PRIMARY, pad=15)

    # Producers (left)
    draw_box(ax, 0.02, 0.80, 0.16, 0.08, "MD Feeder", C_SERVICE, 7)
    draw_box(ax, 0.02, 0.65, 0.16, 0.08, "FIX OEG", C_SERVICE, 7)
    draw_box(ax, 0.02, 0.50, 0.16, 0.08, "Frontend", C_SERVICE, 7)
    draw_box(ax, 0.02, 0.35, 0.16, 0.08, "Clearing House", C_RL, 7)
    draw_box(ax, 0.02, 0.20, 0.16, 0.08, "Dashboard", C_SERVICE, 7)

    # Kafka topics (center)
    topics = ["orders", "trades", "snapshots", "control", "ai_insights"]
    colors = ["#e65100", "#d84315", "#bf360c", "#ff6f00", "#f57c00"]
    for i, (t, c) in enumerate(zip(topics, colors)):
        y = 0.82 - i * 0.155
        draw_box(ax, 0.35, y, 0.16, 0.07, t, c, 8)

    # Consumers (right)
    draw_box(ax, 0.68, 0.80, 0.16, 0.08, "Matcher", C_SERVICE, 7)
    draw_box(ax, 0.68, 0.65, 0.16, 0.08, "Dashboard", C_SERVICE, 7)
    draw_box(ax, 0.68, 0.50, 0.16, 0.08, "Clearing House", C_RL, 7)
    draw_box(ax, 0.68, 0.35, 0.16, 0.08, "MD Feeder", C_SERVICE, 7)
    draw_box(ax, 0.68, 0.20, 0.16, 0.08, "AI Analyst", C_AI, 7)

    # Producer arrows
    draw_arrow_label(ax, 0.18, 0.84, 0.35, 0.855, "orders+snap", C_KAFKA)
    draw_arrow_label(ax, 0.18, 0.69, 0.35, 0.855, "orders", C_KAFKA)
    draw_arrow_label(ax, 0.18, 0.54, 0.35, 0.855, "orders", C_KAFKA)
    draw_arrow_label(ax, 0.18, 0.39, 0.35, 0.855, "orders", C_KAFKA)
    draw_arrow(ax, 0.18, 0.24, 0.35, 0.545, "#ff6f00")

    # Matcher → trades
    draw_arrow(ax, 0.68, 0.82, 0.51, 0.72, "#d84315")

    # Consumer arrows
    draw_arrow(ax, 0.51, 0.855, 0.68, 0.84, C_KAFKA)  # orders → matcher
    draw_arrow(ax, 0.51, 0.82, 0.68, 0.69, C_KAFKA)    # orders → dashboard
    draw_arrow(ax, 0.51, 0.70, 0.68, 0.69, "#d84315")  # trades → dashboard
    draw_arrow(ax, 0.51, 0.70, 0.68, 0.54, "#d84315")  # trades → CH
    draw_arrow(ax, 0.51, 0.545, 0.68, 0.39, "#ff6f00")  # control → MDF
    draw_arrow(ax, 0.51, 0.545, 0.68, 0.54, "#ff6f00")  # control → CH
    draw_arrow(ax, 0.51, 0.39, 0.68, 0.24, "#f57c00")   # insights → analyst

    # Labels
    ax.text(0.10, 0.95, "Producers", ha="center", fontsize=9, fontweight="bold", color="#555")
    ax.text(0.43, 0.95, "Kafka Topics", ha="center", fontsize=9, fontweight="bold", color=C_KAFKA)
    ax.text(0.76, 0.95, "Consumers", ha="center", fontsize=9, fontweight="bold", color="#555")

    return save_fig_to_bytes(fig)


# ── Diagram 3: AI Strategy Decision Flow ─────────────────────────────────────

def diagram_ai_strategy():
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("AI Trading Strategy — Decision Flow", fontsize=12,
                 fontweight="bold", color=C_PRIMARY, pad=15)

    # Strategy selector
    draw_box(ax, 0.30, 0.88, 0.40, 0.07, "CH_AI_STRATEGY", "#37474f", 9,
             "hybrid (default) | rl | llm")

    # Hybrid split
    draw_box(ax, 0.10, 0.72, 0.35, 0.08, "RL Path (USR01-05)", C_RL, 8)
    draw_box(ax, 0.55, 0.72, 0.35, 0.08, "LLM Path (USR06-10)", C_LLM, 8)

    # RL pipeline
    draw_box(ax, 0.02, 0.56, 0.20, 0.07, "Kafka Trades", C_KAFKA, 7)
    draw_box(ax, 0.02, 0.44, 0.20, 0.07, "OHLCV Bars", C_RL, 7, "60-bar window")
    draw_box(ax, 0.02, 0.32, 0.20, 0.07, "50 Indicators", C_RL, 7, "SMA/EMA/MACD/RSI/BB")
    draw_box(ax, 0.02, 0.20, 0.20, 0.07, "Scaler", C_RL, 7, "StandardScaler")
    draw_box(ax, 0.02, 0.08, 0.20, 0.07, "PPO Neural Net", C_RL, 7, "3008-dim → action")

    # RL output
    draw_box(ax, 0.28, 0.08, 0.18, 0.07, "Hold/Buy/Sell\n+ size", "#004d40", 7)

    # LLM pipeline
    draw_box(ax, 0.58, 0.56, 0.20, 0.07, "Build Prompt", C_LLM, 7, "member state + BBOs")
    draw_box(ax, 0.58, 0.44, 0.20, 0.07, "Groq API", C_LLM, 7, "free tier")
    draw_box(ax, 0.58, 0.32, 0.20, 0.07, "HuggingFace", C_LLM, 7, "fallback #1")
    draw_box(ax, 0.58, 0.20, 0.20, 0.07, "Ollama", C_LLM, 7, "fallback #2")
    draw_box(ax, 0.58, 0.08, 0.20, 0.07, "Parse JSON", C_LLM, 7, "→ order dict")

    # Rule-based fallback
    draw_box(ax, 0.82, 0.08, 0.16, 0.07, "Rule-Based\nFallback", "#455a64", 7)

    # Arrows - strategy selector
    draw_arrow(ax, 0.40, 0.88, 0.27, 0.80, C_RL)
    draw_arrow(ax, 0.60, 0.88, 0.72, 0.80, C_LLM)

    # RL pipeline arrows
    draw_arrow(ax, 0.12, 0.56, 0.12, 0.51, C_RL)
    draw_arrow(ax, 0.12, 0.44, 0.12, 0.39, C_RL)
    draw_arrow(ax, 0.12, 0.32, 0.12, 0.27, C_RL)
    draw_arrow(ax, 0.12, 0.20, 0.12, 0.15, C_RL)
    draw_arrow(ax, 0.22, 0.11, 0.28, 0.11, C_RL)

    # LLM pipeline arrows
    draw_arrow(ax, 0.68, 0.56, 0.68, 0.51, C_LLM)
    draw_arrow(ax, 0.68, 0.44, 0.68, 0.39, C_LLM)
    draw_arrow(ax, 0.68, 0.32, 0.68, 0.27, C_LLM)
    draw_arrow(ax, 0.68, 0.20, 0.68, 0.15, C_LLM)

    # Fallback arrows
    draw_arrow(ax, 0.46, 0.11, 0.58, 0.11, "#888")
    ax.text(0.52, 0.13, "fail?", ha="center", fontsize=6, color="#888")
    draw_arrow(ax, 0.78, 0.11, 0.82, 0.11, "#888")
    ax.text(0.80, 0.13, "fail?", ha="center", fontsize=6, color="#888")

    # Submit
    draw_box(ax, 0.35, 0.00, 0.30, 0.05, "→ Submit Order to Kafka 'orders'", C_KAFKA, 7)

    return save_fig_to_bytes(fig)


# ── Diagram 4: MDF Regime Engine ─────────────────────────────────────────────

def diagram_mdf_regimes():
    fig, ax = plt.subplots(1, 1, figsize=(10, 4.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("MD Feeder — Regime-Based Price Dynamics", fontsize=12,
                 fontweight="bold", color=C_PRIMARY, pad=15)

    # Regimes
    regimes = [
        ("trending_up",   0.04, "#1b5e20", "Positive drift\n+ momentum\nRSI ↑, MACD > 0"),
        ("trending_down", 0.22, "#b71c1c", "Negative drift\n+ momentum\nRSI ↓, MACD < 0"),
        ("mean_revert",   0.40, "#1565c0", "Pull to start\nprice\nRSI → 50"),
        ("volatile",      0.58, "#e65100", "Wide swings\nexpanded spread\nBB width ↑"),
        ("calm",          0.76, "#37474f", "Tight range\nnarrow spread\nBB width ↓"),
    ]
    for name, x, color, desc in regimes:
        draw_box(ax, x, 0.55, 0.16, 0.10, name, color, 7)
        ax.text(x + 0.08, 0.48, desc, ha="center", va="top", fontsize=6,
                color="#333", linespacing=1.4)

    # Transition
    draw_box(ax, 0.25, 0.20, 0.50, 0.07, "Auto-rotate every 15-50 ticks", "#546e7a", 8,
             "Bias: mean_revert when price deviates > 15% from start")

    # Arrows from regimes to transition
    for name, x, _, _ in regimes:
        draw_arrow(ax, x + 0.08, 0.55, 0.50, 0.27, "#999")

    # Output
    draw_box(ax, 0.15, 0.03, 0.70, 0.08, "Snapshot with Indicators: SMA(5,20), EMA(12,26), MACD, RSI(14), BB position, regime",
             C_KAFKA, 7)
    draw_arrow(ax, 0.50, 0.20, 0.50, 0.11, C_KAFKA)

    return save_fig_to_bytes(fig)


# ── Diagram 5: RL Observation Space ──────────────────────────────────────────

def diagram_rl_observation():
    fig, ax = plt.subplots(1, 1, figsize=(10, 4))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("RL Agent — Observation Space (3,008 dimensions)", fontsize=12,
                 fontweight="bold", color=C_PRIMARY, pad=15)

    # 60 bars
    draw_box(ax, 0.02, 0.65, 0.20, 0.12, "60 OHLCV\nBars", C_RL, 8)

    # 20 base features
    draw_box(ax, 0.28, 0.75, 0.30, 0.12,
             "20 Base Features", C_RL, 8,
             "Close, Vol, SMA(5,10,20,50)\nEMA(12,26), RSI, MACD, Signal\nBB(upper,lower,width,pos)...")

    # 30 lag features
    draw_box(ax, 0.28, 0.55, 0.30, 0.12,
             "30 Lag Features", C_RL, 8,
             "Close, Vol, PriceChg, RSI\nMACD, Volatility\n@ lags 1,2,3,5,10")

    # = 50 per bar
    draw_box(ax, 0.65, 0.65, 0.14, 0.12, "50 features\n× 60 bars\n= 3,000", "#004d40", 8)

    # Portfolio
    draw_box(ax, 0.65, 0.40, 0.14, 0.15, "8 Portfolio\nFeatures", "#004d40", 8,
             "capital, qty, net_worth\nreturns, held_value...")

    # Total
    draw_box(ax, 0.84, 0.55, 0.14, 0.15, "3,008-dim\nObservation\nVector", C_PRIMARY, 9)

    # Arrows
    draw_arrow(ax, 0.22, 0.71, 0.28, 0.81, C_RL)
    draw_arrow(ax, 0.22, 0.71, 0.28, 0.61, C_RL)
    draw_arrow(ax, 0.58, 0.81, 0.65, 0.75, "#004d40")
    draw_arrow(ax, 0.58, 0.61, 0.65, 0.67, "#004d40")
    draw_arrow(ax, 0.79, 0.71, 0.84, 0.66, C_PRIMARY)
    draw_arrow(ax, 0.79, 0.47, 0.84, 0.58, C_PRIMARY)

    # PPO
    draw_box(ax, 0.30, 0.15, 0.40, 0.12, "PPO MlpPolicy (~2.5 MB)", C_PRIMARY, 9,
             "Forward pass → [action_type (0-2), position_size (0-1)]")
    draw_arrow(ax, 0.91, 0.55, 0.70, 0.27, C_PRIMARY)

    # Output
    draw_box(ax, 0.30, 0.00, 0.13, 0.07, "0: Hold", "#78909c", 7)
    draw_box(ax, 0.45, 0.00, 0.11, 0.07, "1: Buy", "#1b5e20", 7)
    draw_box(ax, 0.58, 0.00, 0.11, 0.07, "2: Sell", "#b71c1c", 7)
    draw_arrow(ax, 0.50, 0.15, 0.37, 0.07, "#78909c")
    draw_arrow(ax, 0.50, 0.15, 0.50, 0.07, "#1b5e20")
    draw_arrow(ax, 0.50, 0.15, 0.63, 0.07, "#b71c1c")

    return save_fig_to_bytes(fig)


# ── Diagram 6: Clearing House Lifecycle ──────────────────────────────────────

def diagram_ch_lifecycle():
    fig, ax = plt.subplots(1, 1, figsize=(10, 4.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("Clearing House — Session Lifecycle", fontsize=12,
                 fontweight="bold", color=C_PRIMARY, pad=15)

    # Timeline
    steps = [
        (0.05, "Start of Day", "#1b5e20",
         "Dashboard sends\n'start' to control\ntopic"),
        (0.22, "MDF Generates\nOrders", C_SERVICE,
         "Regime engine\npopulates books\nwith depth"),
        (0.39, "CH AI Trades", C_RL,
         "Every 45s per member\nRL / LLM / hybrid\n→ orders to Kafka"),
        (0.56, "Trade Attribution", C_KAFKA,
         "Trades topic:\ndetect USRxx- prefix\n→ record to CH DB"),
        (0.73, "End of Day", "#b71c1c",
         "Dashboard POSTs\n/ch/eod\nSettlement + P&L"),
    ]
    for x, label, color, desc in steps:
        draw_box(ax, x, 0.60, 0.18, 0.12, label, color, 7)
        ax.text(x + 0.09, 0.52, desc, ha="center", va="top", fontsize=6,
                color="#333", linespacing=1.4)
        if x < 0.73:
            draw_arrow(ax, x + 0.18, 0.66, x + 0.21, 0.66, color)

    # Bottom: obligation check
    draw_box(ax, 0.15, 0.08, 0.70, 0.10, "Daily Obligation Check: ≥ 20 securities traded per member", "#e65100", 8,
             "Members failing obligation flagged in EOD settlement report")

    draw_arrow(ax, 0.50, 0.60, 0.50, 0.18, "#e65100")

    return save_fig_to_bytes(fig)


# ── PDF Builder ───────────────────────────────────────────────────────────────

def _ascii(text: str) -> str:
    """Replace Unicode chars with ASCII equivalents for fpdf core fonts."""
    return (text
            .replace("\u2014", "--")   # em-dash
            .replace("\u2013", "-")    # en-dash
            .replace("\u2019", "'")    # right single quote
            .replace("\u201c", '"')    # left double quote
            .replace("\u201d", '"')    # right double quote
            .replace("\u2192", "->")   # →
            .replace("\u2190", "<-")   # ←
            .replace("\u2191", "^")    # ↑
            .replace("\u2193", "v")    # ↓
            .replace("\u2265", ">=")   # ≥
            .replace("\u2264", "<=")   # ≤
            .replace("\u00d7", "x")    # ×
            )


class DevGuidePDF(FPDF):

    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 5, "StockEx Developer's Guide", align="L")
            self.cell(0, 5, f"Page {self.page_no()}", align="R")
            self.ln(8)

    def chapter_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*hex_to_rgb(C_PRIMARY))
        self.cell(0, 12, _ascii(title), new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*hex_to_rgb(C_ACCENT))
        self.set_line_width(0.6)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def section_title(self, title):
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(*hex_to_rgb(C_SECONDARY))
        self.cell(0, 9, _ascii(title), new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*hex_to_rgb(C_SECONDARY))
        self.cell(0, 7, _ascii(title), new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(33, 33, 33)
        self.multi_cell(0, 5, _ascii(text))
        self.ln(2)

    def code_block(self, text):
        self.set_font("Courier", "", 8)
        self.set_fill_color(240, 240, 240)
        self.set_text_color(50, 50, 50)
        for line in text.strip().split("\n"):
            self.cell(0, 4.5, "  " + _ascii(line), new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(3)

    def table(self, headers, rows, col_widths=None):
        if col_widths is None:
            w = (self.w - self.l_margin - self.r_margin) / len(headers)
            col_widths = [w] * len(headers)
        # Header
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(*hex_to_rgb(C_PRIMARY))
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 6, _ascii(h), border=1, fill=True, align="C")
        self.ln()
        # Rows
        self.set_font("Helvetica", "", 8)
        self.set_text_color(33, 33, 33)
        for j, row in enumerate(rows):
            fill = j % 2 == 0
            if fill:
                self.set_fill_color(245, 245, 245)
            for i, cell in enumerate(row):
                self.cell(col_widths[i], 5.5, _ascii(str(cell)), border=1, fill=fill,
                          align="C" if i > 0 else "L")
            self.ln()
        self.ln(3)

    _diagram_counter = 0

    def add_diagram(self, img_bytes, w=180):
        DevGuidePDF._diagram_counter += 1
        tmp = os.path.join(OUT_DIR, f"_tmp_diagram_{DevGuidePDF._diagram_counter}.png")
        with open(tmp, "wb") as f:
            f.write(img_bytes)
        self.image(tmp, x=(self.w - w) / 2, w=w)
        os.remove(tmp)
        self.ln(5)


def build_pdf():
    os.makedirs(OUT_DIR, exist_ok=True)
    pdf = DevGuidePDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    # ── Cover page ────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.ln(50)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(*hex_to_rgb(C_PRIMARY))
    pdf.cell(0, 15, "StockEx", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 18)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, "Developer's Guide", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7, "Kafka-based Stock Exchange Simulator", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "with AI-Powered Clearing House Members", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(150, 150, 150)
    pdf.cell(0, 6, "Version 2.0  |  March 2026", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "github.com/Bonum/StockEx", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── Table of Contents ────────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("Table of Contents")
    toc = [
        "1. System Architecture",
        "2. Kafka Message Flow",
        "3. Services Reference",
        "4. Market Data Feeder — Regime Engine",
        "5. Clearing House — AI Trading Members",
        "6. RL Agent — Neural Network Details",
        "7. LLM Integration",
        "8. Session Lifecycle",
        "9. Database Schema",
        "10. Configuration Reference",
        "11. Deployment",
    ]
    for item in toc:
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(33, 33, 33)
        pdf.cell(0, 7, _ascii(item), new_x="LMARGIN", new_y="NEXT")

    # ── 1. System Architecture ────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("1. System Architecture")
    pdf.body_text(
        "StockEx runs as a single Docker container on HuggingFace Spaces (port 7860) "
        "or as multiple containers via docker-compose. All services communicate through "
        "Apache Kafka (KRaft mode, no ZooKeeper) with five topics: orders, trades, "
        "snapshots, control, and ai_insights.\n\n"
        "nginx acts as the reverse proxy, routing paths to the appropriate Flask service. "
        "Three SQLite databases persist matcher state, OHLCV history, and clearing house data."
    )
    pdf.add_diagram(diagram_architecture())

    # ── 2. Kafka Message Flow ─────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("2. Kafka Message Flow")
    pdf.body_text(
        "All order flow and control signals pass through Kafka topics. Producers send "
        "messages (orders, snapshots, control signals) and multiple consumers process them "
        "independently. This decoupled architecture allows services to be added or removed "
        "without affecting others."
    )
    pdf.add_diagram(diagram_message_flow())

    pdf.section_title("Order Message Format")
    pdf.code_block("""\
{
  "cl_ord_id": "USR01-1709876543210-1",
  "symbol":    "ALPHA",
  "side":      "BUY",
  "quantity":  100,
  "price":     10.50,
  "ord_type":  "LIMIT",
  "time_in_force": "DAY",
  "timestamp": 1709876543.123,
  "source":    "CLRH"
}""")

    pdf.section_title("Trade Message Format")
    pdf.code_block("""\
{
  "trade_id":  12345,
  "symbol":    "ALPHA",
  "price":     10.50,
  "quantity":  100,
  "buy_id":    "USR01-1709876543210-1",
  "sell_id":   "MDF-1709876543200-42",
  "timestamp": 1709876543.456
}""")

    pdf.section_title("Snapshot with Indicators")
    pdf.code_block("""\
{
  "symbol": "ALPHA", "best_bid": 24.85, "best_ask": 25.05,
  "bid_size": 200, "ask_size": 150, "timestamp": 1709876543.789,
  "source": "MDF",
  "indicators": {
    "sma_5": 24.92, "sma_20": 24.78,
    "ema_12": 24.88, "ema_26": 24.75,
    "macd": 0.13, "rsi_14": 62.5,
    "bb_pos": 0.72, "regime": "trending_up"
  }
}""")

    # ── 3. Services Reference ─────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("3. Services Reference")
    pdf.table(
        ["Service", "Port", "Key File", "Description"],
        [
            ["nginx", "7860", "nginx.conf", "Reverse proxy"],
            ["Dashboard", "5000", "dashboard/dashboard.py", "SSE streaming, session control, charts"],
            ["Matcher", "6000", "matcher/matcher.py", "Price-time matching, SQLite persistence"],
            ["MD Feeder", "-", "md_feeder/mdf_simulator.py", "Regime-based order generation"],
            ["Clearing House", "5004", "clearing_house/app.py", "AI members, RL/LLM trading"],
            ["AI Analyst", "-", "ai_analyst/ai_analyst.py", "LLM market commentary"],
            ["FIX OEG", "5001", "fix_oeg/fix_oeg_server.py", "FIX 4.4 acceptor"],
            ["FIX UI", "5002", "fix-ui-client/fix-ui-client.py", "Browser FIX client"],
            ["Frontend", "5003", "frontend/frontend.py", "Order entry form"],
        ],
        [35, 15, 55, 85],
    )

    pdf.section_title("Startup Order (entrypoint.sh)")
    pdf.body_text(
        "Services start sequentially with sleep delays to allow initialization:\n"
        "1. Kafka (KRaft) — wait for broker ready (up to 30 retries)\n"
        "2. Create Kafka topics (orders, trades, snapshots, control, ai_insights)\n"
        "3. Matcher (:6000) — wait 8s\n"
        "4. MD Feeder — background\n"
        "5. FIX OEG (:5001) — wait 6s\n"
        "6. FIX UI (:5002) — wait 3s\n"
        "7. AI Analyst — wait 1s\n"
        "8. Frontend (:5003) — wait 2s\n"
        "9. Clearing House (:5004) — wait 3s\n"
        "10. nginx (:7860)\n"
        "11. Dashboard (:5000) — exec (foreground process)"
    )

    # ── 4. MDF Regime Engine ──────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("4. Market Data Feeder — Regime Engine")
    pdf.body_text(
        "The MD Feeder drives price evolution through a PriceDynamics class that simulates "
        "five market regimes. Each symbol independently cycles through regimes, producing "
        "realistic technical indicator patterns that the RL and LLM strategies can exploit.\n\n"
        "Price dynamics use drift + noise + momentum, with adaptive volatility that expands "
        "in volatile regimes and contracts in calm periods. When price deviates more than 15% "
        "from the session start price, the engine biases regime transitions toward mean reversion."
    )
    pdf.add_diagram(diagram_mdf_regimes())

    pdf.section_title("Regime Parameters")
    pdf.table(
        ["Regime", "Drift", "Noise", "Spread", "Aggr. Prob", "Indicator Effect"],
        [
            ["trending_up", "+0.2-0.8 * vol", "0.5 * vol", "0.8-1.5x", "20%", "MACD > 0, RSI rising"],
            ["trending_down", "-0.2-0.8 * vol", "0.5 * vol", "0.8-1.5x", "20%", "MACD < 0, RSI falling"],
            ["mean_revert", "3% pull to start", "0.3 * vol", "1.0x", "20%", "RSI oscillates ~50"],
            ["volatile", "random", "1.5 * vol", "1.5-2.5x", "35%", "BB width expands"],
            ["calm", "near zero", "0.2 * vol", "0.6-1.0x", "20%", "BB width contracts"],
        ],
        [30, 28, 22, 22, 20, 68],
    )

    pdf.section_title("Order Book Depth")
    pdf.body_text(
        "Each cycle places 3 levels of resting bids and asks per symbol (6 passive orders), "
        "ensuring the order book always has visible depth. In trending regimes, the passive side "
        "gets thicker depth (1.5x quantity) to model institutional support/resistance.\n\n"
        "Aggressive orders (20-35% probability) cross the spread to generate trades. "
        "The aggressive side is biased by indicators: sell when RSI > 70 + BB overbought, "
        "buy when RSI < 30 + BB oversold, and follow MACD direction in trending regimes."
    )

    # ── 5. Clearing House ────────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("5. Clearing House — AI Trading Members")
    pdf.body_text(
        "The Clearing House service manages 10 simulated trading members (USR01-USR10), each "
        "starting with EUR 100,000 capital and a daily obligation to trade at least 20 securities. "
        "Members not controlled by a human user are traded by the AI engine."
    )
    pdf.add_diagram(diagram_ai_strategy())

    pdf.section_title("Strategy Selection")
    pdf.table(
        ["Mode", "Env Var Value", "Behavior"],
        [
            ["Hybrid (default)", "hybrid", "USR01-05 = RL, USR06-10 = LLM"],
            ["RL only", "rl", "All members use PPO neural network"],
            ["LLM only", "llm", "All members use Groq/HF/Ollama"],
        ],
        [35, 35, 120],
    )
    pdf.body_text(
        "Strategy is set via CH_AI_STRATEGY env var and can be switched at runtime:\n"
        "  POST /ch/api/strategy {\"strategy\": \"rl\"}\n\n"
        "Every strategy falls back: RL → LLM → rule-based. The rule-based fallback uses "
        "portfolio balance heuristics (buy when holdings < 20% of net worth, sell when > 60%)."
    )

    pdf.section_title("Trade Attribution")
    pdf.body_text(
        "The trade consumer thread monitors the Kafka 'trades' topic. When a trade's buy_id "
        "or sell_id matches the pattern USRxx-*, the trade is attributed to that clearing house "
        "member. This updates their capital, holdings, and daily trade count in the CH database."
    )

    # ── 6. RL Agent ───────────────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("6. RL Agent — Neural Network Details")
    pdf.body_text(
        "The RL strategy uses Adilbai/stock-trading-rl-agent, a PPO (Proximal Policy Optimization) "
        "model trained with Stable-Baselines3 on 500,000 timesteps of historical stock data "
        "(AAPL, MSFT, GOOGL, AMZN, TSLA)."
    )

    pdf.section_title("Model Specifications")
    pdf.table(
        ["Property", "Value"],
        [
            ["Algorithm", "PPO (Proximal Policy Optimization)"],
            ["Policy network", "MlpPolicy (Multi-Layer Perceptron)"],
            ["Model file size", "~2.5 MB (final_model.zip)"],
            ["Scaler file size", "~50 KB (scaler.pkl, StandardScaler)"],
            ["Observation space", "3,008 dimensions"],
            ["Action space", "2D: action_type (0-2) + position_size (0-1)"],
            ["Training steps", "500,000"],
            ["Learning rate", "0.0003"],
            ["Gamma (discount)", "0.99"],
            ["Batch size", "64"],
            ["License", "MIT (free, runs locally)"],
            ["Storage location", "/app/data/rl_model/ (downloaded on first use)"],
        ],
        [50, 140],
    )

    pdf.add_diagram(diagram_rl_observation())

    pdf.section_title("Observation Space Breakdown")
    pdf.body_text(
        "The 3,008-dimensional observation vector consists of:\n\n"
        "Market state (3,000 dims): 60 time bars x 50 features per bar\n"
        "  - 20 base features: Close, Volume, SMA(5,10,20,50), EMA(12,26), RSI(14), "
        "MACD, Signal, Histogram, BB(upper,lower,width,position), Volatility(20), "
        "Price changes, H/L ratio, Volume ratio\n"
        "  - 30 lag features: Close, Volume, Price change, RSI, MACD, Volatility "
        "at lags 1, 2, 3, 5, 10\n\n"
        "Portfolio state (8 dims): capital, held_qty, net_worth, returns, held_value, "
        "has_position (binary), capital_ratio, holdings_ratio"
    )

    pdf.section_title("Price History Management")
    pdf.body_text(
        "The RL module (ch_rl_trader.py) maintains a rolling buffer of OHLCV bars per symbol:\n"
        "  - Trades from Kafka feed into a current-bar accumulator (feed_trade())\n"
        "  - Bars finalize every CH_RL_BAR_INTERVAL seconds (default 60s)\n"
        "  - On startup, bars are seeded from BBO reference prices with small noise\n"
        "  - Minimum CH_RL_MIN_BARS (default 30) required before RL predictions start\n"
        "  - Buffer holds up to 120 bars (deque with maxlen)\n\n"
        "The shipped scaler.pkl was trained on real stock data. When it fails on synthetic "
        "StockEx data (shape mismatch), the code falls back to manual z-score normalization."
    )

    pdf.section_title("Inference vs LLM Comparison")
    pdf.table(
        ["Aspect", "RL (PPO)", "LLM"],
        [
            ["Input", "3,008-dim float vector", "Text prompt (~500 chars)"],
            ["Processing", "NN forward pass (<1ms)", "API call (2-30s)"],
            ["Output", "[action_type, position_size]", "JSON text to parse"],
            ["Internet required", "No (local model)", "Yes (Groq/HF) or Ollama"],
            ["Cost", "Free (CPU inference)", "Free tier or self-hosted"],
            ["Determinism", "Configurable", "Temperature-dependent"],
        ],
        [35, 65, 90],
    )

    # ── 7. LLM Integration ───────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("7. LLM Integration")
    pdf.body_text(
        "The LLM strategy sends a text prompt containing the member's state (capital, holdings, "
        "obligation) and current market BBOs, requesting a single JSON trade decision."
    )

    pdf.section_title("Provider Fallback Chain")
    pdf.table(
        ["Priority (HF Spaces)", "Priority (Local)", "Provider", "Env Vars"],
        [
            ["1st", "2nd", "Groq", "GROQ_API_KEY, GROQ_MODEL"],
            ["-", "3rd", "HuggingFace Inference", "HF_TOKEN, HF_MODEL"],
            ["2nd", "1st", "Ollama (local)", "OLLAMA_HOST, OLLAMA_MODEL"],
        ],
        [35, 30, 45, 80],
    )
    pdf.body_text(
        "On HuggingFace Spaces (detected via SPACE_ID env var), Groq is preferred (free tier, fast). "
        "Locally, Ollama with a fine-tuned model is preferred. The HF Inference endpoint is used "
        "as a fallback with the custom fine-tuned model RayMelius/stockex-ch-trader.\n\n"
        "If all LLM providers fail, the system falls back to rule-based trading."
    )

    pdf.section_title("Prompt Structure")
    pdf.code_block("""\
You are simulating clearing house member USR01 making ONE trading decision.

Member state:
  Available capital: EUR 95,230.00
  Securities obligation remaining today: 15 more to trade
  Current holdings:
    ALPHA: 200 shares @ avg cost 24.90

Current market (Bid/Ask):
  ALPHA: Bid 24.85 / Ask 25.05
  NBG: Bid 17.95 / Ask 18.15
  ...

Rules:
- Do not spend more than your available capital
- Do not sell more shares than you hold
- Choose a realistic price close to the BBO mid-price
- Quantity should be between 10 and 200

Respond ONLY with valid JSON:
Example: {"symbol": "ALPHA", "side": "BUY", "quantity": 50, "price": 5.95}""")

    # ── 8. Session Lifecycle ──────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("8. Session Lifecycle")
    pdf.add_diagram(diagram_ch_lifecycle())

    pdf.section_title("Control Messages")
    pdf.table(
        ["Action", "Dashboard Trigger", "Effect on MDF", "Effect on CH AI"],
        [
            ["start", "Start of Day button", "running=True, reload prices", "suspended=False"],
            ["stop", "End of Day button", "running=False", "suspended=True"],
            ["suspend", "Suspend button", "order gen paused", "suspended=True"],
            ["resume", "Resume button", "order gen resumed", "suspended=False"],
        ],
        [25, 40, 55, 70],
    )

    pdf.section_title("Manual vs Automatic Mode")
    pdf.body_text(
        "The Dashboard supports two session modes:\n\n"
        "Automatic: A background scheduler reads market_schedule.txt and automatically "
        "triggers Start of Day / End of Day based on configured times and timezone.\n\n"
        "Manual: The scheduler is disabled. Sessions are started/stopped exclusively "
        "via the Dashboard buttons. All trading activity (MDF, CH AI, FIX) still operates "
        "normally once a session is started."
    )

    pdf.section_title("EOD Settlement")
    pdf.body_text(
        "When End of Day is triggered, the Dashboard POSTs to /ch/eod. The Clearing House:\n"
        "1. Calculates unrealized P&L for each member (current market value vs avg cost)\n"
        "2. Checks daily obligation (>= 20 securities traded)\n"
        "3. Records settlement entry in the database\n"
        "4. Logs members who failed their obligation"
    )

    # ── 9. Database Schema ────────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("9. Database Schema")

    pdf.section_title("matcher.db (Matcher Service)")
    pdf.body_text("Stores all orders and trades for the matching engine.")
    pdf.code_block("""\
orders:     order_id, symbol, side, price, quantity, remaining_qty,
            status, cl_ord_id, timestamp
trades:     trade_id, symbol, price, quantity, buy_order_id,
            sell_order_id, timestamp""")

    pdf.section_title("clearing_house.db (Clearing House)")
    pdf.body_text("Stores member accounts, trade attribution, and settlement history.")
    pdf.code_block("""\
members:      member_id (PK), password_hash, capital
holdings:     member_id, symbol, quantity, avg_cost
              (composite PK: member_id + symbol)
trade_log:    id, member_id, symbol, side, quantity, price,
              order_id, timestamp
settlements:  id, member_id, trading_date, opening_capital,
              closing_capital, realized_pnl, unrealized_pnl,
              obligation_met
ai_decisions: id, member_id, raw_response, parsed_order,
              source, timestamp""")

    pdf.section_title("Dashboard OHLCV (In-Memory + Periodic SQLite)")
    pdf.body_text(
        "The Dashboard aggregates trades into 1-minute OHLCV buckets for candlestick charts. "
        "Data is held in memory with periodic persistence to SQLite for history."
    )

    # ── 10. Configuration Reference ───────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("10. Configuration Reference")

    pdf.section_title("Core Settings (shared/config.py)")
    pdf.table(
        ["Variable", "Default", "Description"],
        [
            ["KAFKA_BOOTSTRAP", "kafka:9092", "Kafka broker address"],
            ["MATCHER_URL", "http://matcher:6000", "Matcher REST API"],
            ["SECURITIES_FILE", "/app/data/securities.txt", "Symbol definitions"],
            ["TICK_SIZE", "0.05", "Minimum price increment"],
            ["ORDERS_PER_MIN", "8", "MDF order rate per symbol"],
            ["CH_DAILY_OBLIGATION", "20", "Min securities per member/day"],
            ["CH_STARTING_CAPITAL", "100,000", "Initial member capital (EUR)"],
        ],
        [45, 55, 90],
    )

    pdf.section_title("AI Strategy Settings")
    pdf.table(
        ["Variable", "Default", "Description"],
        [
            ["CH_AI_STRATEGY", "hybrid", "llm, rl, or hybrid"],
            ["CH_AI_INTERVAL", "45", "Seconds between AI cycles"],
            ["CH_RL_MODEL_REPO", "Adilbai/stock-trading-rl-agent", "HF model repo"],
            ["CH_RL_BAR_INTERVAL", "60", "Seconds per OHLCV bar"],
            ["CH_RL_MIN_BARS", "30", "Min bars before RL starts"],
            ["GROQ_API_KEY", "-", "Groq API key"],
            ["GROQ_MODEL", "llama-3.1-8b-instant", "Groq model name"],
            ["HF_TOKEN", "-", "HuggingFace API token"],
            ["HF_MODEL", "RayMelius/stockex-ch-trader", "HF model for CH"],
            ["OLLAMA_HOST", "-", "Ollama server URL"],
            ["OLLAMA_MODEL", "llama3.1:8b", "Ollama model name"],
        ],
        [50, 55, 85],
    )

    # ── 11. Deployment ────────────────────────────────────────────────────
    pdf.add_page()
    pdf.chapter_title("11. Deployment")

    pdf.section_title("HuggingFace Spaces (Single Container)")
    pdf.body_text(
        "The Dockerfile builds a single container with Kafka, all Python services, and nginx. "
        "HuggingFace Spaces exposes port 7860. The entrypoint.sh starts services sequentially.\n\n"
        "Key dependencies added for RL: stable-baselines3 (pulls PyTorch ~200MB), "
        "numpy (<2.0), pandas, scikit-learn, huggingface_hub.\n\n"
        "The RL model is downloaded from HuggingFace Hub on first use and cached at "
        "/app/data/rl_model/."
    )

    pdf.section_title("Docker Compose (Multi-Container)")
    pdf.body_text(
        "docker-compose.yml defines separate containers for each service. The clearing_house "
        "service gets its own Dockerfile with the RL dependencies. Volumes persist SQLite databases.\n\n"
        "Set CH_AI_STRATEGY in the environment section or .env file."
    )

    pdf.section_title("Local Development")
    pdf.code_block("""\
# Prerequisites: Python 3.11+, Kafka on localhost:9092
pip install kafka-python Flask requests quickfix \\
            stable-baselines3 huggingface_hub pandas scikit-learn

export PYTHONPATH=$(pwd)
export KAFKA_BOOTSTRAP=localhost:9092
export MATCHER_URL=http://localhost:6000
export CH_AI_STRATEGY=hybrid

# Terminal 1-4: matcher, md_feeder, dashboard, clearing_house""")

    pdf.section_title("CI/CD Pipeline")
    pdf.body_text(
        "GitHub Actions workflows:\n"
        "  - ci.yml: Run matcher unit tests + Docker build check on every push\n"
        "  - deploy-hf.yml: Auto-deploy to HuggingFace Spaces on push to main (after tests pass)\n\n"
        "The container log prints the StockEx version at startup for deployment verification."
    )

    # ── Save ──────────────────────────────────────────────────────────────
    pdf.output(OUT_FILE)
    print(f"PDF generated: {OUT_FILE}")
    return OUT_FILE


if __name__ == "__main__":
    build_pdf()
