import os
import queue
import socket
import threading
import time
import tkinter as tk
from tkinter import filedialog, ttk
from typing import Optional

from rftp.network.receiver import ReceiverConfig, run_receiver
from rftp.network.sender import SenderConfig, TransferEvent, run_sender
from rftp.protocol.stats import TransferStats, format_file_size
from rftp.ui.theme import (
    BG_CARD,
    BG_CARD_BORDER,
    BG_INPUT,
    BG_LOG,
    BG_MAIN,
    COLOR_ACCENT,
    COLOR_DANGER,
    COLOR_SUCCESS,
    COLOR_WARNING,
    FONT_FAMILY,
    FONT_MONO,
    TEXT_MUTED,
    TEXT_PRIMARY,
    TEXT_WHITE,
)


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


class TransferApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Reliable UDP File Transfer (RFTP)")
        self.root.geometry("780x640")
        self.root.minsize(700, 560)
        self.root.configure(bg=BG_MAIN)

        self.mode = tk.StringVar(value="SENDER")  # "SENDER" or "RECEIVER"
        self.state = "IDLE"  # "IDLE", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"
        self.local_ip = get_local_ip()

        # Sender state
        self.file_path_var = tk.StringVar(value="")
        self.target_ip_var = tk.StringVar(value=self.local_ip)
        self.sender_port_var = tk.StringVar(value="9999")
        self.window_size_var = tk.IntVar(value=32)
        self.sender_loss_var = tk.IntVar(value=0)

        # Receiver state
        default_dl = os.path.abspath("./downloads")
        self.dest_dir_var = tk.StringVar(value=default_dl)
        self.recv_port_var = tk.StringVar(value="9999")
        self.recv_loss_var = tk.IntVar(value=0)

        # Active worker handles
        self.worker_thread: Optional[threading.Thread] = None
        self.cancel_event = threading.Event()
        self.event_queue = queue.Queue()
        self.stats: Optional[TransferStats] = None

        self._setup_styles()
        self._build_ui()
        self._start_poll_loop()

    def _setup_styles(self):
        style = ttk.Style(self.root)
        style.theme_use("clam")

        style.configure(".", background=BG_MAIN, foreground=TEXT_PRIMARY, font=(FONT_FAMILY, 10))
        style.configure(
            "Card.TFrame", background=BG_CARD, relief="groove", borderwidth=1
        )
        style.configure("Accent.TButton", background=COLOR_ACCENT, foreground=TEXT_WHITE, font=(FONT_FAMILY, 10, "bold"))
        style.map("Accent.TButton", background=[("active", "#3b73cf"), ("disabled", "#334155")])
        style.configure("Danger.TButton", background=COLOR_DANGER, foreground=TEXT_WHITE, font=(FONT_FAMILY, 10, "bold"))
        style.map("Danger.TButton", background=[("active", "#d94343"), ("disabled", "#334155")])
        style.configure("Horizontal.TProgressbar", background=COLOR_ACCENT, troughcolor=BG_INPUT)

    def _build_ui(self):
        # 1. Header Bar
        header = tk.Frame(self.root, bg=BG_MAIN)
        header.pack(fill="x", padx=16, pady=(12, 6))

        title = tk.Label(
            header,
            text="Reliable UDP File Transfer",
            font=(FONT_FAMILY, 16, "bold"),
            fg=COLOR_ACCENT,
            bg=BG_MAIN,
        )
        title.pack(side="left")

        ip_container = tk.Frame(header, bg=BG_MAIN)
        ip_container.pack(side="right")

        self.ip_lbl = tk.Label(
            ip_container,
            text=f"Local IP: {self.local_ip}",
            font=(FONT_MONO, 10),
            fg=TEXT_MUTED,
            bg=BG_MAIN,
        )
        self.ip_lbl.pack(side="left", padx=6)

        refresh_btn = tk.Button(
            ip_container,
            text="↻",
            command=self._refresh_ip,
            font=(FONT_FAMILY, 10, "bold"),
            bg=BG_CARD,
            fg=TEXT_PRIMARY,
            bd=1,
            relief="groove",
            padx=4,
        )
        refresh_btn.pack(side="left")

        # 2. Mode Selector Bar
        mode_frame = tk.Frame(self.root, bg=BG_MAIN)
        mode_frame.pack(fill="x", padx=16, pady=4)

        self.btn_mode_sender = tk.Button(
            mode_frame,
            text="  Sender Mode  ",
            command=lambda: self._set_mode("SENDER"),
            font=(FONT_FAMILY, 11, "bold"),
            bg=COLOR_ACCENT,
            fg=TEXT_WHITE,
            bd=0,
            padx=10,
            pady=4,
        )
        self.btn_mode_sender.pack(side="left", padx=(0, 8))

        self.btn_mode_recv = tk.Button(
            mode_frame,
            text="  Receiver Mode  ",
            command=lambda: self._set_mode("RECEIVER"),
            font=(FONT_FAMILY, 11, "bold"),
            bg=BG_CARD,
            fg=TEXT_MUTED,
            bd=0,
            padx=10,
            pady=4,
        )
        self.btn_mode_recv.pack(side="left")

        self.badge_lbl = tk.Label(
            mode_frame,
            text="IDLE",
            font=(FONT_FAMILY, 10, "bold"),
            fg=TEXT_MUTED,
            bg=BG_MAIN,
        )
        self.badge_lbl.pack(side="right", padx=4)

        # 3. Main Config Card
        self.config_card = tk.Frame(self.root, bg=BG_CARD, bd=1, relief="solid")
        self.config_card.pack(fill="x", padx=16, pady=8)

        self.sender_panel = tk.Frame(self.config_card, bg=BG_CARD)
        self.receiver_panel = tk.Frame(self.config_card, bg=BG_CARD)

        self._build_sender_panel()
        self._build_receiver_panel()
        self.sender_panel.pack(fill="x", padx=12, pady=12)

        # 4. Metrics & Progress Card
        metrics_card = tk.Frame(self.root, bg=BG_CARD, bd=1, relief="solid")
        metrics_card.pack(fill="x", padx=16, pady=4)

        self.progress_bar = ttk.Progressbar(
            metrics_card, orient="horizontal", mode="determinate", style="Horizontal.TProgressbar"
        )
        self.progress_bar.pack(fill="x", padx=12, pady=(10, 6))

        m_grid = tk.Frame(metrics_card, bg=BG_CARD)
        m_grid.pack(fill="x", padx=12, pady=(0, 10))

        self.val_speed = self._create_metric_box(m_grid, 0, "Speed", "0 B/s", COLOR_ACCENT)
        self.val_bytes = self._create_metric_box(m_grid, 1, "Transferred", "0 B / 0 B", TEXT_PRIMARY)
        self.val_chunks = self._create_metric_box(
            m_grid, 2, "Chunks (Retrans / Drops)", "0/0 (0 / 0)", TEXT_PRIMARY
        )
        self.val_rtt = self._create_metric_box(m_grid, 3, "RTT / ETA", "-- / --", TEXT_PRIMARY)

        # 5. Log & Trace Console
        log_header = tk.Frame(self.root, bg=BG_MAIN)
        log_header.pack(fill="x", padx=16, pady=(6, 2))

        tk.Label(
            log_header,
            text="Protocol Event Log & Trace",
            font=(FONT_FAMILY, 10, "bold"),
            fg=TEXT_PRIMARY,
            bg=BG_MAIN,
        ).pack(side="left")

        clear_btn = tk.Button(
            log_header,
            text="Clear",
            command=self._clear_logs,
            font=(FONT_FAMILY, 9),
            bg=BG_CARD,
            fg=TEXT_MUTED,
            bd=0,
            padx=6,
        )
        clear_btn.pack(side="right")

        log_frame = tk.Frame(self.root, bg=BG_LOG, bd=1, relief="solid")
        log_frame.pack(fill="both", expand=True, padx=16, pady=(0, 12))

        self.log_text = tk.Text(
            log_frame,
            bg=BG_LOG,
            fg=TEXT_PRIMARY,
            insertbackground=TEXT_WHITE,
            font=(FONT_MONO, 9),
            bd=0,
            wrap="word",
        )
        self.log_text.pack(side="left", fill="both", expand=True, padx=8, pady=6)
        log_scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=log_scroll.set)

        self._add_log("RFTP Application ready. Choose Sender or Receiver mode.")

    def _build_sender_panel(self):
        # Row 1: File Picker
        r1 = tk.Frame(self.sender_panel, bg=BG_CARD)
        r1.pack(fill="x", pady=3)
        tk.Label(r1, text="File to Send:", width=13, anchor="w", bg=BG_CARD, fg=TEXT_MUTED).pack(
            side="left"
        )
        self.entry_file = tk.Entry(
            r1, textvariable=self.file_path_var, bg=BG_INPUT, fg=TEXT_PRIMARY, insertbackground=TEXT_WHITE
        )
        self.entry_file.pack(side="left", fill="x", expand=True, padx=6)
        tk.Button(
            r1,
            text="Browse...",
            command=self._browse_file,
            bg=BG_MAIN,
            fg=TEXT_PRIMARY,
            bd=1,
            padx=8,
        ).pack(side="right")

        # Row 2: Target IP & Port
        r2 = tk.Frame(self.sender_panel, bg=BG_CARD)
        r2.pack(fill="x", pady=3)
        tk.Label(r2, text="Receiver IP:", width=13, anchor="w", bg=BG_CARD, fg=TEXT_MUTED).pack(
            side="left"
        )
        tk.Entry(
            r2,
            textvariable=self.target_ip_var,
            width=16,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
            insertbackground=TEXT_WHITE,
        ).pack(side="left", padx=(0, 12))

        tk.Label(r2, text="Port:", bg=BG_CARD, fg=TEXT_MUTED).pack(side="left")
        tk.Entry(
            r2,
            textvariable=self.sender_port_var,
            width=7,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
            insertbackground=TEXT_WHITE,
        ).pack(side="left", padx=6)

        # Window size & loss sliders
        tk.Label(r2, text="Window:", bg=BG_CARD, fg=TEXT_MUTED).pack(side="left", padx=(12, 2))
        w_spin = tk.Spinbox(
            r2,
            from_=1,
            to=64,
            textvariable=self.window_size_var,
            width=4,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
        )
        w_spin.pack(side="left", padx=2)

        tk.Label(r2, text="Loss %:", bg=BG_CARD, fg=TEXT_MUTED).pack(side="left", padx=(12, 2))
        l_spin = tk.Spinbox(
            r2,
            from_=0,
            to=50,
            textvariable=self.sender_loss_var,
            width=4,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
        )
        l_spin.pack(side="left", padx=2)

        # Row 3: Action Buttons
        r3 = tk.Frame(self.sender_panel, bg=BG_CARD)
        r3.pack(fill="x", pady=(8, 0))

        self.btn_sender_action = tk.Button(
            r3,
            text="  Start Transfer  ",
            command=self._toggle_sender,
            bg=COLOR_ACCENT,
            fg=TEXT_WHITE,
            font=(FONT_FAMILY, 10, "bold"),
            bd=0,
            padx=12,
            pady=4,
        )
        self.btn_sender_action.pack(side="left")

        self.status_msg_lbl = tk.Label(r3, text="Ready", bg=BG_CARD, fg=TEXT_MUTED)
        self.status_msg_lbl.pack(side="left", padx=12)

    def _build_receiver_panel(self):
        # Row 1: Save Dir
        r1 = tk.Frame(self.receiver_panel, bg=BG_CARD)
        r1.pack(fill="x", pady=3)
        tk.Label(r1, text="Save Directory:", width=13, anchor="w", bg=BG_CARD, fg=TEXT_MUTED).pack(
            side="left"
        )
        self.entry_dest = tk.Entry(
            r1, textvariable=self.dest_dir_var, bg=BG_INPUT, fg=TEXT_PRIMARY, insertbackground=TEXT_WHITE
        )
        self.entry_dest.pack(side="left", fill="x", expand=True, padx=6)
        tk.Button(
            r1,
            text="Browse...",
            command=self._browse_dest,
            bg=BG_MAIN,
            fg=TEXT_PRIMARY,
            bd=1,
            padx=8,
        ).pack(side="right")

        # Row 2: Listen Port & Loss %
        r2 = tk.Frame(self.receiver_panel, bg=BG_CARD)
        r2.pack(fill="x", pady=3)
        tk.Label(r2, text="Listening Port:", width=13, anchor="w", bg=BG_CARD, fg=TEXT_MUTED).pack(
            side="left"
        )
        tk.Entry(
            r2,
            textvariable=self.recv_port_var,
            width=7,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
            insertbackground=TEXT_WHITE,
        ).pack(side="left", padx=(0, 16))

        tk.Label(r2, text="Simulated Loss %:", bg=BG_CARD, fg=TEXT_MUTED).pack(side="left")
        l_spin = tk.Spinbox(
            r2,
            from_=0,
            to=50,
            textvariable=self.recv_loss_var,
            width=4,
            bg=BG_INPUT,
            fg=TEXT_PRIMARY,
        )
        l_spin.pack(side="left", padx=4)

        # Row 3: Action Buttons
        r3 = tk.Frame(self.receiver_panel, bg=BG_CARD)
        r3.pack(fill="x", pady=(8, 0))

        self.btn_recv_action = tk.Button(
            r3,
            text="  Start Listening  ",
            command=self._toggle_receiver,
            bg=COLOR_ACCENT,
            fg=TEXT_WHITE,
            font=(FONT_FAMILY, 10, "bold"),
            bd=0,
            padx=12,
            pady=4,
        )
        self.btn_recv_action.pack(side="left")

        self.recv_status_lbl = tk.Label(r3, text="Idle", bg=BG_CARD, fg=TEXT_MUTED)
        self.recv_status_lbl.pack(side="left", padx=12)

    def _create_metric_box(self, parent, col, title, initial_val, color):
        box = tk.Frame(parent, bg=BG_CARD)
        box.grid(row=0, column=col, sticky="ew", padx=8)
        parent.columnconfigure(col, weight=1)

        tk.Label(box, text=title, font=(FONT_FAMILY, 8), fg=TEXT_MUTED, bg=BG_CARD).pack(anchor="w")
        val_lbl = tk.Label(
            box, text=initial_val, font=(FONT_FAMILY, 11, "bold"), fg=color, bg=BG_CARD
        )
        val_lbl.pack(anchor="w")
        return val_lbl

    def _set_mode(self, mode: str):
        if self.state == "RUNNING":
            return
        self.mode.set(mode)
        if mode == "SENDER":
            self.btn_mode_sender.configure(bg=COLOR_ACCENT, fg=TEXT_WHITE)
            self.btn_mode_recv.configure(bg=BG_CARD, fg=TEXT_MUTED)
            self.receiver_panel.pack_forget()
            self.sender_panel.pack(fill="x", padx=12, pady=12)
        else:
            self.btn_mode_recv.configure(bg=COLOR_ACCENT, fg=TEXT_WHITE)
            self.btn_mode_sender.configure(bg=BG_CARD, fg=TEXT_MUTED)
            self.sender_panel.pack_forget()
            self.receiver_panel.pack(fill="x", padx=12, pady=12)

    def _refresh_ip(self):
        self.local_ip = get_local_ip()
        self.ip_lbl.configure(text=f"Local IP: {self.local_ip}")
        self._add_log(f"Refreshed local IP address: {self.local_ip}")

    def _browse_file(self):
        filename = filedialog.askopenfilename()
        if filename:
            self.file_path_var.set(filename)

    def _browse_dest(self):
        folder = filedialog.askdirectory()
        if folder:
            self.dest_dir_var.set(folder)

    def _clear_logs(self):
        self.log_text.delete("1.0", "end")

    def _add_log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{ts}] {msg}\n")
        self.log_text.see("end")

    def _update_badge(self, state: str):
        self.state = state
        colors = {
            "IDLE": TEXT_MUTED,
            "RUNNING": COLOR_ACCENT,
            "COMPLETED": COLOR_SUCCESS,
            "FAILED": COLOR_DANGER,
            "CANCELLED": COLOR_WARNING,
        }
        self.badge_lbl.configure(text=state, fg=colors.get(state, TEXT_MUTED))

    def _toggle_sender(self):
        if self.state == "RUNNING":
            self.cancel_event.set()
            self._update_badge("CANCELLED")
            self.btn_sender_action.configure(text="  Start Transfer  ", bg=COLOR_ACCENT)
            return

        file_path = self.file_path_var.get().strip()
        if not file_path or not os.path.isfile(file_path):
            self._add_log("ERROR: Please select a valid file to send.")
            self._update_badge("FAILED")
            return

        try:
            port = int(self.sender_port_var.get().strip())
        except ValueError:
            self._add_log("ERROR: Invalid port number.")
            return

        target_ip = self.target_ip_var.get().strip()

        config = SenderConfig(
            file_path=file_path,
            target_addr=(target_ip, port),
            window_size=self.window_size_var.get(),
            simulated_loss_percent=self.sender_loss_var.get(),
        )

        self.cancel_event.clear()
        self._update_badge("RUNNING")
        self.btn_sender_action.configure(text="  Cancel Transfer  ", bg=COLOR_DANGER)
        self.progress_bar["value"] = 0

        self.worker_thread = threading.Thread(
            target=run_sender, args=(config, self.cancel_event, self.event_queue), daemon=True
        )
        self.worker_thread.start()

    def _toggle_receiver(self):
        if self.state == "RUNNING":
            self.cancel_event.set()
            self._update_badge("CANCELLED")
            self.btn_recv_action.configure(text="  Start Listening  ", bg=COLOR_ACCENT)
            return

        try:
            port = int(self.recv_port_var.get().strip())
        except ValueError:
            self._add_log("ERROR: Invalid listening port.")
            return

        config = ReceiverConfig(
            bind_addr=("0.0.0.0", port),
            destination_dir=self.dest_dir_var.get().strip(),
            simulated_loss_percent=self.recv_loss_var.get(),
        )

        self.cancel_event.clear()
        self._update_badge("RUNNING")
        self.btn_recv_action.configure(text="  Stop Listening  ", bg=COLOR_DANGER)
        self.progress_bar["value"] = 0

        self.worker_thread = threading.Thread(
            target=run_receiver, args=(config, self.cancel_event, self.event_queue), daemon=True
        )
        self.worker_thread.start()

    def _start_poll_loop(self):
        def poll():
            while not self.event_queue.empty():
                try:
                    ev: TransferEvent = self.event_queue.get_nowait()
                    if ev.event_type == "LOG":
                        self._add_log(ev.message)
                    elif ev.event_type == "PROGRESS" and ev.stats:
                        st = ev.stats
                        self.progress_bar["value"] = st.progress_percent()
                        self.val_speed.configure(text=st.formatted_speed())
                        self.val_bytes.configure(text=st.formatted_bytes())
                        self.val_chunks.configure(
                            text=f"{st.chunks_transferred}/{st.total_chunks} ({st.retransmissions} / {st.corrupt_packets_dropped + st.duplicate_packets_dropped})"
                        )
                        rtt_s = f"{st.current_rtt * 1000.0:.1f}ms" if st.current_rtt else "--"
                        eta_s = f"{st.estimated_remaining():.0f}s" if st.estimated_remaining() else "--"
                        self.val_rtt.configure(text=f"{rtt_s} / {eta_s}")
                    elif ev.event_type == "SUCCESS":
                        self._update_badge("COMPLETED")
                        self._add_log(f"SUCCESS: {ev.message}")
                        self.btn_sender_action.configure(text="  Start Transfer  ", bg=COLOR_ACCENT)
                        self.btn_recv_action.configure(text="  Start Listening  ", bg=COLOR_ACCENT)
                    elif ev.event_type == "ERROR":
                        self._update_badge("FAILED")
                        self._add_log(f"ERROR: {ev.message}")
                        self.btn_sender_action.configure(text="  Start Transfer  ", bg=COLOR_ACCENT)
                        self.btn_recv_action.configure(text="  Start Listening  ", bg=COLOR_ACCENT)
                    elif ev.event_type == "CANCELLED":
                        self._update_badge("CANCELLED")
                        self._add_log("Cancelled event confirmed.")
                        self.btn_sender_action.configure(text="  Start Transfer  ", bg=COLOR_ACCENT)
                        self.btn_recv_action.configure(text="  Start Listening  ", bg=COLOR_ACCENT)
                except queue.Empty:
                    break

            self.root.after(50, poll)

        self.root.after(50, poll)


def launch_gui():
    root = tk.Tk()
    app = TransferApp(root)
    root.mainloop()
