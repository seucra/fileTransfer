"""
Dark modern theme colors and styles for RFTP Tkinter GUI
"""

BG_MAIN = "#121620"
BG_CARD = "#1a2130"
BG_CARD_BORDER = "#2d3a50"
BG_INPUT = "#1e2638"
BG_LOG = "#0e121a"

TEXT_PRIMARY = "#e6ebf5"
TEXT_MUTED = "#8e9bb0"
TEXT_WHITE = "#ffffff"

COLOR_ACCENT = "#4c8cf5"
COLOR_SUCCESS = "#48c774"
COLOR_WARNING = "#ffb84d"
COLOR_DANGER = "#f55a5a"

FONT_FAMILY = "Segoe UI" if "nt" in __import__("os").name else "DejaVu Sans"
FONT_MONO = "Consolas" if "nt" in __import__("os").name else "Monospace"
