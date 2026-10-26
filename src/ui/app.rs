use crate::network::receiver::{run_receiver, ReceiverConfig};
use crate::network::sender::{run_sender, SenderConfig, TransferEvent};
use crate::protocol::stats::{
    chunk_byte_range, AckKind, ChunkSnapshot, ChunkState, ProtocolEventKind,
    ProtocolTelemetryEvent, TransferStats, WindowPerspective,
};
use crate::ui::theme::{
    configure_theme, COLOR_CARD_BG, COLOR_CARD_BORDER, COLOR_DANGER, COLOR_INFO,
    COLOR_PINNED_HIGHLIGHT, COLOR_PRIMARY, COLOR_STATE_ACKED_BG, COLOR_STATE_ACKED_BORDER,
    COLOR_STATE_INFLIGHT_BG, COLOR_STATE_INFLIGHT_BORDER, COLOR_STATE_PENDING_BG,
    COLOR_STATE_PENDING_BORDER, COLOR_STATE_RETRANS_BG, COLOR_STATE_RETRANS_BORDER,
    COLOR_STATE_SACKED_BG, COLOR_STATE_SACKED_BORDER, COLOR_SUCCESS, COLOR_TEXT_MUTED,
    COLOR_TEXT_PRIMARY, COLOR_TEXT_SECONDARY, COLOR_WARNING, COLOR_WINDOW_HIGHLIGHT,
};
use crossbeam_channel::{bounded, Receiver as ChannelReceiver, Sender as ChannelSender};
use eframe::egui::{
    self, Align, Button, Color32, FontId, Layout, Pos2, ProgressBar, Rect, RichText, Rounding,
    ScrollArea, Sense, Stroke, Ui, Vec2,
};
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

/// Bounded capacity for the worker-to-GUI event channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of snapshots retained for pause/step-through replay.
const MAX_SNAPSHOT_HISTORY: usize = 120;

/// Maximum number of timeline events retained in the UI.
const MAX_TIMELINE_EVENTS: usize = 250;

/// Maximum number of per-chunk historical snapshots cached for pinned inspection.
const MAX_PINNED_CACHE: usize = 256;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AppMode {
    Sender,
    Receiver,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TransferState {
    Idle,
    Active,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PerspectiveSelection {
    Auto,
    SenderWindow,
    ReceiverBuffer,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TimelineFilter {
    All,
    WindowAndSack,
    RetransAndTimeout,
    DataAndAck,
}

pub struct TransferApp {
    mode: AppMode,
    state: TransferState,
    status_message: String,

    // Networking
    local_ip: String,
    target_ip: String,
    port: u16,

    // Sender settings
    selected_file_path: String,
    chunk_size: u32,
    window_size: usize,

    // Receiver settings
    destination_dir: String,

    // Simulated loss
    simulated_loss_percent: u8,

    // Background worker control
    cancel_flag: Option<Arc<AtomicBool>>,
    event_rx: Option<ChannelReceiver<TransferEvent>>,

    // Live Metrics, Replay History & Bounded Logs
    stats: Option<TransferStats>,
    snapshot_history: VecDeque<TransferStats>,
    timeline_events: VecDeque<ProtocolTelemetryEvent>,
    logs: VecDeque<String>,

    // Observability Dashboard state
    perspective_selection: PerspectiveSelection,
    timeline_filter: TimelineFilter,
    vis_paused: bool,
    replay_frame_index: Option<usize>,
    pinned_chunk_seq: Option<u32>,
    chunk_inspection_cache: BTreeMap<u32, ChunkSnapshot>,
    log_search_query: String,
}

impl Default for TransferApp {
    fn default() -> Self {
        let detected_ip = local_ip_address::local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let default_dest = std::env::current_dir()
            .map(|p| p.join("downloads").display().to_string())
            .unwrap_or_else(|_| "./downloads".to_string());

        let mut logs = VecDeque::with_capacity(MAX_TIMELINE_EVENTS);
        logs.push_back("Application initialized. Ready for UDP file transfer.".to_string());

        Self {
            mode: AppMode::Sender,
            state: TransferState::Idle,
            status_message: "Ready to initiate or receive reliable file transfer.".to_string(),
            local_ip: detected_ip.clone(),
            target_ip: detected_ip,
            port: 9999,
            selected_file_path: String::new(),
            chunk_size: 1400,
            window_size: 32,
            destination_dir: default_dest,
            simulated_loss_percent: 0,
            cancel_flag: None,
            event_rx: None,
            stats: None,
            snapshot_history: VecDeque::with_capacity(MAX_SNAPSHOT_HISTORY),
            timeline_events: VecDeque::with_capacity(MAX_TIMELINE_EVENTS),
            logs,
            perspective_selection: PerspectiveSelection::Auto,
            timeline_filter: TimelineFilter::All,
            vis_paused: false,
            replay_frame_index: None,
            pinned_chunk_seq: None,
            chunk_inspection_cache: BTreeMap::new(),
            log_search_query: String::new(),
        }
    }
}

impl TransferApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        configure_theme(&cc.egui_ctx);
        Self::default()
    }

    fn refresh_local_ip(&mut self) {
        if let Ok(ip) = local_ip_address::local_ip() {
            self.local_ip = ip.to_string();
            self.add_log(format!("Refreshed local network IP: {}", self.local_ip));
        }
    }

    fn add_log(&mut self, msg: String) {
        let timestamp = chrono_free_timestamp();
        if self.logs.len() >= MAX_TIMELINE_EVENTS {
            self.logs.pop_front();
        }
        self.logs.push_back(format!("[{}] {}", timestamp, msg));
    }

    fn reset_observability_for_new_run(&mut self) {
        self.stats = None;
        self.snapshot_history.clear();
        self.timeline_events.clear();
        self.chunk_inspection_cache.clear();
        self.vis_paused = false;
        self.replay_frame_index = None;
        self.pinned_chunk_seq = None;
    }

    fn start_sender(&mut self) {
        let file_path = PathBuf::from(&self.selected_file_path);
        if !file_path.exists() {
            self.state = TransferState::Failed;
            self.status_message = "Selected file does not exist on disk.".to_string();
            self.add_log(self.status_message.clone());
            return;
        }

        let target_ip: IpAddr = match self.target_ip.trim().parse() {
            Ok(ip) => ip,
            Err(_) => {
                self.state = TransferState::Failed;
                self.status_message = "Invalid Destination IP address format.".to_string();
                self.add_log(self.status_message.clone());
                return;
            }
        };

        let target_addr = SocketAddr::new(target_ip, self.port);
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();

        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancel_flag = Some(Arc::clone(&cancel_flag));

        let (tx, rx): (ChannelSender<TransferEvent>, ChannelReceiver<TransferEvent>) =
            bounded(EVENT_CHANNEL_CAPACITY);
        self.event_rx = Some(rx);

        let config = SenderConfig {
            file_path,
            target_addr,
            bind_addr,
            chunk_size: self.chunk_size,
            window_size: self.window_size,
            simulated_loss_percent: self.simulated_loss_percent,
        };

        self.reset_observability_for_new_run();
        self.state = TransferState::Active;
        self.status_message = format!("Connecting to receiver at {}...", target_addr);
        self.add_log(format!(
            "Starting Sender -> Target: {}, Window: {} pkts, Chunk: {} B, Synthetic Drop: {}%",
            target_addr, self.window_size, self.chunk_size, self.simulated_loss_percent
        ));

        thread::spawn(move || {
            run_sender(config, cancel_flag, tx);
        });
    }

    fn start_receiver(&mut self) {
        let dest_dir = PathBuf::from(&self.destination_dir);
        let bind_ip: IpAddr = "0.0.0.0".parse().unwrap();
        let bind_addr = SocketAddr::new(bind_ip, self.port);

        let cancel_flag = Arc::new(AtomicBool::new(false));
        self.cancel_flag = Some(Arc::clone(&cancel_flag));

        let (tx, rx): (ChannelSender<TransferEvent>, ChannelReceiver<TransferEvent>) =
            bounded(EVENT_CHANNEL_CAPACITY);
        self.event_rx = Some(rx);

        let config = ReceiverConfig {
            bind_addr,
            destination_dir: dest_dir,
            simulated_loss_percent: self.simulated_loss_percent,
        };

        self.reset_observability_for_new_run();
        self.state = TransferState::Active;
        self.status_message = format!("Listening for incoming files on UDP port {}...", self.port);
        self.add_log(format!(
            "Starting Receiver on UDP port {}, Synthetic Drop: {}%",
            self.port, self.simulated_loss_percent
        ));

        thread::spawn(move || {
            run_receiver(config, cancel_flag, tx);
        });
    }

    fn cancel_transfer(&mut self) {
        if let Some(flag) = &self.cancel_flag {
            flag.store(true, Ordering::SeqCst);
        }
        self.state = TransferState::Cancelled;
        self.status_message = "Transfer cancelled by user.".to_string();
        self.add_log("Transfer cancelled by operator.".to_string());
    }

    fn ingest_snapshot(&mut self, mut st: TransferStats) {
        // Move structured protocol events into bounded UI timeline
        for ev in st.recent_events.drain(..) {
            if self.timeline_events.len() >= MAX_TIMELINE_EVENTS {
                self.timeline_events.pop_front();
            }
            self.timeline_events.push_back(ev);
        }

        // Cache chunk metadata for click-to-pin inspection
        for chunk in &st.visible_chunks {
            if self.chunk_inspection_cache.len() >= MAX_PINNED_CACHE
                && !self.chunk_inspection_cache.contains_key(&chunk.seq_num)
            {
                if let Some(&oldest_seq) = self.chunk_inspection_cache.keys().next() {
                    if Some(oldest_seq) != self.pinned_chunk_seq {
                        self.chunk_inspection_cache.remove(&oldest_seq);
                    }
                }
            }
            self.chunk_inspection_cache
                .insert(chunk.seq_num, chunk.clone());
        }

        // If pinned chunk is now behind cumulative window_base, ensure cached state reflects Cumulative Ack
        if let Some(pinned_seq) = self.pinned_chunk_seq {
            if pinned_seq < st.window_base {
                if let Some(cached) = self.chunk_inspection_cache.get_mut(&pinned_seq) {
                    if cached.state != ChunkState::Acked {
                        cached.ack_kind = if cached.state == ChunkState::Sacked {
                            AckKind::SelectiveThenCumulative
                        } else {
                            AckKind::Cumulative
                        };
                        cached.state = ChunkState::Acked;
                        if cached.acked_elapsed.is_none() {
                            cached.acked_elapsed = Some(st.elapsed_duration());
                        }
                    }
                }
            }
        }

        // Record into bounded replay history
        if self.snapshot_history.len() >= MAX_SNAPSHOT_HISTORY {
            self.snapshot_history.pop_front();
            if let Some(idx) = &mut self.replay_frame_index {
                *idx = idx.saturating_sub(1);
            }
        }
        self.snapshot_history.push_back(st.clone());

        // Always keep live `self.stats` updated
        self.stats = Some(st);
    }

    fn process_events(&mut self) {
        let events: Vec<TransferEvent> = if let Some(rx) = &self.event_rx {
            let mut evs = Vec::new();
            while let Ok(event) = rx.try_recv() {
                evs.push(event);
            }
            evs
        } else {
            Vec::new()
        };

        for event in events {
            match event {
                TransferEvent::Log(msg) => {
                    self.add_log(msg);
                }
                TransferEvent::Progress(st) => {
                    self.ingest_snapshot(st);
                }
                TransferEvent::Success(msg) => {
                    self.state = TransferState::Completed;
                    self.status_message = msg.clone();
                    self.add_log(format!("SUCCESS: {}", msg));
                }
                TransferEvent::Error(err) => {
                    self.state = TransferState::Failed;
                    self.status_message = err.clone();
                    self.add_log(format!("ERROR: {}", err));
                }
                TransferEvent::Cancelled => {
                    self.state = TransferState::Cancelled;
                    self.status_message = "Transfer cancelled.".to_string();
                    self.add_log("Transfer cancelled signal confirmed.".to_string());
                }
            }
        }
    }

    /// Return the snapshot currently displayed in the observability dashboard (live or paused/replay frame).
    fn active_display_stats(&self) -> Option<&TransferStats> {
        if let Some(idx) = self.replay_frame_index {
            if let Some(frame) = self.snapshot_history.get(idx) {
                return Some(frame);
            }
        }
        self.stats.as_ref()
    }

    /// Compute specific protocol technical state for the status pill badge.
    fn compute_detailed_state(&self) -> (&'static str, Color32, Color32) {
        match self.state {
            TransferState::Idle => ("IDLE", Color32::from_rgb(26, 30, 40), COLOR_TEXT_MUTED),
            TransferState::Completed => (
                "COMPLETED",
                Color32::from_rgb(20, 64, 36),
                COLOR_SUCCESS,
            ),
            TransferState::Failed => ("FAILED", Color32::from_rgb(76, 22, 26), COLOR_DANGER),
            TransferState::Cancelled => {
                ("CANCELLED", Color32::from_rgb(64, 46, 16), COLOR_WARNING)
            }
            TransferState::Active => {
                let msg_lower = self.status_message.to_lowercase();
                if self.mode == AppMode::Receiver {
                    if self.stats.is_none() {
                        ("LISTENING", Color32::from_rgb(20, 46, 80), COLOR_PRIMARY)
                    } else if msg_lower.contains("verif")
                        || msg_lower.contains("sha")
                        || msg_lower.contains("hash")
                    {
                        (
                            "VERIFYING_HASH",
                            Color32::from_rgb(46, 34, 76),
                            Color32::from_rgb(180, 150, 240),
                        )
                    } else if let Some(st) = &self.stats {
                        if st.corrupt_packets_dropped > 0 || st.retransmissions > 0 {
                            (
                                "RECOVERING_LOSS",
                                Color32::from_rgb(76, 48, 16),
                                COLOR_WARNING,
                            )
                        } else {
                            ("RECEIVING", Color32::from_rgb(22, 58, 42), COLOR_SUCCESS)
                        }
                    } else {
                        ("RECEIVING", Color32::from_rgb(22, 58, 42), COLOR_SUCCESS)
                    }
                } else if self.stats.is_none() {
                    ("CONNECTING", Color32::from_rgb(20, 46, 80), COLOR_PRIMARY)
                } else if msg_lower.contains("verif")
                    || msg_lower.contains("sha")
                    || msg_lower.contains("hash")
                {
                    (
                        "VERIFYING_HASH",
                        Color32::from_rgb(46, 34, 76),
                        Color32::from_rgb(180, 150, 240),
                    )
                } else if let Some(st) = &self.stats {
                    if st.retransmissions > 0 && st.in_flight_count > 0 {
                        (
                            "RECOVERING_LOSS",
                            Color32::from_rgb(76, 48, 16),
                            COLOR_WARNING,
                        )
                    } else if st.chunks_transferred == st.total_chunks && st.in_flight_count > 0 {
                        (
                            "WAITING_FOR_ACKS",
                            Color32::from_rgb(72, 50, 16),
                            COLOR_WARNING,
                        )
                    } else {
                        ("TRANSFERRING", Color32::from_rgb(22, 50, 92), COLOR_PRIMARY)
                    }
                } else {
                    ("TRANSFERRING", Color32::from_rgb(22, 50, 92), COLOR_PRIMARY)
                }
            }
        }
    }
}

impl eframe::App for TransferApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_events();

        // Repaint periodically if active to keep live stats updated
        if self.state == TransferState::Active {
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }

        // Dedicated fixed application header bar (desktop utility style)
        egui::TopBottomPanel::top("header_panel")
            .frame(
                egui::Frame::none()
                    .fill(COLOR_CARD_BG)
                    .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                    .inner_margin(egui::Margin::symmetric(14.0_f32, 8.0_f32)),
            )
            .show(ctx, |ui| {
                self.render_header(ui);
            });

        // Main dashboard workspace area
        egui::CentralPanel::default()
            .frame(
                egui::Frame::none()
                    .fill(crate::ui::theme::COLOR_PANEL_BG)
                    .inner_margin(egui::Margin::same(10.0_f32)),
            )
            .show(ctx, |ui| {
                ScrollArea::vertical()
                    .id_salt("main_dashboard_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        // 1. Configuration Panel
                        egui::Frame::none()
                            .fill(COLOR_CARD_BG)
                            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                            .rounding(Rounding::same(3.0_f32))
                            .inner_margin(10.0_f32)
                            .show(ui, |ui| match self.mode {
                                AppMode::Sender => self.render_sender_config(ui),
                                AppMode::Receiver => self.render_receiver_config(ui),
                            });

                        ui.add_space(6.0_f32);

                        // 2. Technical Metrics Grid
                        egui::Frame::none()
                            .fill(COLOR_CARD_BG)
                            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                            .rounding(Rounding::same(3.0_f32))
                            .inner_margin(10.0_f32)
                            .show(ui, |ui| {
                                self.render_metrics_panel(ui);
                            });

                        ui.add_space(6.0_f32);

                        // 3. Sliding-Window & SACK Packet Matrix Dashboard
                        egui::Frame::none()
                            .fill(COLOR_CARD_BG)
                            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                            .rounding(Rounding::same(3.0_f32))
                            .inner_margin(10.0_f32)
                            .show(ui, |ui| {
                                self.render_packet_matrix_dashboard(ui);
                            });

                        ui.add_space(6.0_f32);

                        // 4. Structured Protocol Event Timeline & Logs Panel
                        self.render_timeline_panel(ui);
                    });
            });
    }
}

impl TransferApp {
    fn render_header(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            // Left: Title and protocol spec summary
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("Reliable UDP File Transfer (RFTP)")
                            .size(15.0)
                            .strong()
                            .color(COLOR_TEXT_PRIMARY),
                    );
                    ui.label(
                        RichText::new("v1.0")
                            .size(11.0)
                            .monospace()
                            .color(COLOR_TEXT_MUTED),
                    );
                });
                ui.label(
                    RichText::new("Selective Repeat SACK · CRC32 & SHA-256 · Karn's Algorithm RTT/RTO")
                        .size(10.5)
                        .color(COLOR_TEXT_MUTED),
                );
            });

            ui.add_space(16.0_f32);

            // Center: Segmented mode tabs (Sender / Receiver)
            let is_active = self.state == TransferState::Active;
            ui.add_enabled_ui(!is_active, |ui| {
                ui.horizontal(|ui| {
                    let sender_selected = self.mode == AppMode::Sender;
                    let receiver_selected = self.mode == AppMode::Receiver;

                    if render_segment_tab(ui, "Sender Mode", sender_selected) {
                        self.mode = AppMode::Sender;
                    }
                    if render_segment_tab(ui, "Receiver Mode", receiver_selected) {
                        self.mode = AppMode::Receiver;
                    }
                });
            });

            // Right: Local IP and Technical State Badge
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let (state_text, state_bg, state_fg) = self.compute_detailed_state();
                egui::Frame::none()
                    .fill(state_bg)
                    .stroke(Stroke::new(1.0_f32, state_fg))
                    .rounding(Rounding::same(3.0_f32))
                    .inner_margin(egui::Margin::symmetric(8.0_f32, 3.0_f32))
                    .show(ui, |ui| {
                        ui.label(
                            RichText::new(state_text)
                                .size(11.0)
                                .monospace()
                                .strong()
                                .color(state_fg),
                        );
                    });

                ui.separator();

                if ui
                    .add_enabled(!is_active, Button::new("⟳ Refresh"))
                    .on_hover_text("Re-query local network interface IP addresses")
                    .clicked()
                {
                    self.refresh_local_ip();
                }

                ui.label(
                    RichText::new(format!("Local: {}", self.local_ip))
                        .size(11.5)
                        .monospace()
                        .color(COLOR_TEXT_SECONDARY),
                );
            });
        });
    }

    fn render_sender_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label(
                RichText::new("SENDER CONFIGURATION & TRANSMISSION TARGET")
                    .size(11.0)
                    .strong()
                    .color(COLOR_TEXT_SECONDARY),
            );
        });

        ui.add_space(4.0_f32);

        // Row 1: Source File Selection with live file metadata inspection
        let file_path_obj = Path::new(&self.selected_file_path);
        let file_exists = !self.selected_file_path.is_empty() && file_path_obj.exists();
        let file_size_opt = if file_exists {
            file_path_obj.metadata().ok().map(|m| m.len())
        } else {
            None
        };

        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Source File:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            );

            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.selected_file_path)
                    .desired_width(ui.available_width() - 95.0_f32)
                    .hint_text("Select or enter path to file to send..."),
            );

            if ui
                .add_enabled(!is_active, Button::new("Browse..."))
                .clicked()
            {
                if let Some(file) = rfd::FileDialog::new().pick_file() {
                    self.selected_file_path = file.display().to_string();
                }
            }
        });

        // File validation feedback
        if !self.selected_file_path.is_empty() {
            ui.horizontal(|ui| {
                if let Some(bytes) = file_size_opt {
                    let total_chunks = (bytes + self.chunk_size as u64 - 1) / (self.chunk_size as u64).max(1);
                    ui.label(
                        RichText::new(format!(
                            "File Size: {} ({} B) | Calculated Payload Chunks: {} chunks",
                            format_file_size(bytes),
                            bytes,
                            total_chunks
                        ))
                        .size(11.0)
                        .monospace()
                        .color(COLOR_SUCCESS),
                    );
                } else {
                    ui.label(
                        RichText::new("Warning: File does not exist on disk.")
                            .size(11.0)
                            .color(COLOR_DANGER),
                    );
                }
            });
        }

        ui.add_space(4.0_f32);

        // Row 2: Target parameters (IP, Port, Window, Loss)
        let ip_valid = self.target_ip.trim().parse::<IpAddr>().is_ok();
        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("Destination IP:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            );
            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.target_ip).desired_width(120.0_f32),
            );
            if !ip_valid {
                ui.label(RichText::new("Invalid IP").size(11.0).color(COLOR_DANGER));
            }

            ui.separator();

            ui.label(RichText::new("Port:").size(12.0).color(COLOR_TEXT_PRIMARY));
            let mut port_str = self.port.to_string();
            if ui
                .add_enabled(
                    !is_active,
                    egui::TextEdit::singleline(&mut port_str).desired_width(52.0_f32),
                )
                .changed()
            {
                if let Ok(p) = port_str.parse() {
                    if p > 0 {
                        self.port = p;
                    }
                }
            }

            ui.separator();

            ui.label(
                RichText::new("Sliding Window:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            );
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.window_size, 1..=64).text("pkts"),
            );

            ui.separator();

            ui.label(
                RichText::new("Synthetic Drop:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            )
            .on_hover_text(
                "Artificial drop rate injected on socket for protocol evaluation. This is a synthetic test condition, not physical wire loss.",
            );
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.simulated_loss_percent, 0..=50).text("% drop"),
            );
        });

        ui.add_space(6.0_f32);

        // Row 3: Action Buttons & Inline Status
        ui.horizontal(|ui| {
            let can_start = !is_active && file_exists && ip_valid;

            if !is_active {
                let start_btn = Button::new(
                    RichText::new("  Start Transfer  ")
                        .color(Color32::WHITE)
                        .strong(),
                )
                .fill(COLOR_PRIMARY);

                if ui
                    .add_enabled(can_start, start_btn)
                    .on_disabled_hover_text("Select a valid existing file and destination IP address to start.")
                    .clicked()
                {
                    self.start_sender();
                }
            } else {
                let cancel_btn = Button::new(
                    RichText::new("  Cancel Transfer  ")
                        .color(Color32::WHITE)
                        .strong(),
                )
                .fill(COLOR_DANGER);

                if ui.add(cancel_btn).clicked() {
                    self.cancel_transfer();
                }
            }

            if (self.state == TransferState::Failed || self.state == TransferState::Completed)
                && ui.add_enabled(can_start, Button::new("Retry Transfer")).clicked()
            {
                self.start_sender();
            }

            ui.separator();

            ui.label(
                RichText::new(&self.status_message)
                    .size(11.5)
                    .monospace()
                    .color(COLOR_TEXT_SECONDARY),
            );
        });
    }

    fn render_receiver_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label(
                RichText::new("RECEIVER CONFIGURATION & LISTENER BINDING")
                    .size(11.0)
                    .strong()
                    .color(COLOR_TEXT_SECONDARY),
            );
        });

        ui.add_space(4.0_f32);

        // Row 1: Save Directory
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Download Directory:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            );
            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.destination_dir)
                    .desired_width(ui.available_width() - 95.0_f32)
                    .hint_text("Directory where incoming files will be written..."),
            );
            if ui
                .add_enabled(!is_active, Button::new("Browse..."))
                .clicked()
            {
                if let Some(folder) = rfd::FileDialog::new().pick_folder() {
                    self.destination_dir = folder.display().to_string();
                }
            }
        });

        ui.add_space(4.0_f32);

        // Row 2: Port & Synthetic Drop
        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("Listening Port:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            );
            let mut port_str = self.port.to_string();
            if ui
                .add_enabled(
                    !is_active,
                    egui::TextEdit::singleline(&mut port_str).desired_width(55.0_f32),
                )
                .changed()
            {
                if let Ok(p) = port_str.parse() {
                    if p > 0 {
                        self.port = p;
                    }
                }
            }

            ui.separator();

            ui.label(
                RichText::new("Synthetic Drop:")
                    .size(12.0)
                    .color(COLOR_TEXT_PRIMARY),
            )
            .on_hover_text(
                "Artificial drop rate injected on incoming UDP packets for testing out-of-order SACK buffering.",
            );
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.simulated_loss_percent, 0..=50).text("% drop"),
            );
        });

        ui.add_space(6.0_f32);

        // Row 3: Action Buttons & Inline Status
        ui.horizontal(|ui| {
            if !is_active {
                let listen_btn = Button::new(
                    RichText::new("  Start Listening  ")
                        .color(Color32::WHITE)
                        .strong(),
                )
                .fill(COLOR_PRIMARY);

                if ui.add(listen_btn).clicked() {
                    self.start_receiver();
                }
            } else {
                let stop_btn = Button::new(
                    RichText::new("  Stop Listening  ")
                        .color(Color32::WHITE)
                        .strong(),
                )
                .fill(COLOR_DANGER);

                if ui.add(stop_btn).clicked() {
                    self.cancel_transfer();
                }
            }

            ui.separator();

            ui.label(
                RichText::new(&self.status_message)
                    .size(11.5)
                    .monospace()
                    .color(COLOR_TEXT_SECONDARY),
            );
        });
    }

    fn render_metrics_panel(&self, ui: &mut Ui) {
        let st_opt = self.active_display_stats();

        let (
            progress,
            speed_val,
            bytes_str,
            acked_str,
            in_flight_str,
            pkts_str,
            retrans_str,
            sim_loss_str,
            rtt_rto_str,
            eta_str,
        ) = match st_opt {
            Some(st) => {
                let rtt_s = st
                    .current_rtt
                    .map(|r| format!("{:.1} ms", r.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "--".to_string());
                let rto_s = st
                    .current_rto
                    .map(|r| format!("{:.0} ms", r.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "--".to_string());
                let pkt_label = match st.perspective {
                    WindowPerspective::Sender => {
                        format!(
                            "{} DATA tx / {} ACK rx",
                            st.packets_sent, st.ack_packets_processed
                        )
                    }
                    WindowPerspective::Receiver => {
                        format!(
                            "{} DATA rx / {} ACK tx",
                            st.ack_packets_processed, st.packets_sent
                        )
                    }
                };
                let inflight_label = match st.perspective {
                    WindowPerspective::Sender => {
                        format!(
                            "{} in-flight ({} SACKed)",
                            st.in_flight_count, st.sacked_count
                        )
                    }
                    WindowPerspective::Receiver => {
                        format!(
                            "{} gap ({} OOO buffered)",
                            st.in_flight_count, st.sacked_count
                        )
                    }
                };
                (
                    st.progress_ratio(),
                    st.formatted_speed(),
                    st.formatted_bytes(),
                    format!(
                        "#{}/#{} (Base #{})",
                        st.chunks_transferred, st.total_chunks, st.window_base
                    ),
                    inflight_label,
                    pkt_label,
                    format!(
                        "{} retries ({} drops)",
                        st.retransmissions,
                        st.corrupt_packets_dropped + st.duplicate_packets_dropped
                    ),
                    format!("{}% (Synthetic Drop)", st.configured_loss_percent),
                    format!("{} / {}", rtt_s, rto_s),
                    st.estimated_remaining()
                        .map(|e| format!("{:.1}s", e.as_secs_f64()))
                        .unwrap_or_else(|| "--".to_string()),
                )
            }
            None => (
                0.0,
                "--".to_string(),
                "-- / --".to_string(),
                "--".to_string(),
                "--".to_string(),
                "--".to_string(),
                "--".to_string(),
                format!("{}% (Synthetic Drop)", self.simulated_loss_percent),
                "-- / --".to_string(),
                "--".to_string(),
            ),
        };

        // Compact technical progress bar
        ui.add(
            ProgressBar::new(progress)
                .show_percentage()
                .animate(self.state == TransferState::Active && !self.vis_paused),
        );

        ui.add_space(6.0_f32);

        // Technical 4-column structured metrics grid with tabular numerals
        ui.columns(4, |cols| {
            // Col 0: Throughput
            cols[0].vertical(|ui| {
                ui.label(
                    RichText::new("THROUGHPUT")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(speed_val)
                        .size(14.0)
                        .monospace()
                        .strong()
                        .color(COLOR_PRIMARY),
                );
            });

            // Col 1: Progress & Volume
            cols[1].vertical(|ui| {
                ui.label(
                    RichText::new("TRANSFERRED & ETA")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(format!("{} (ETA {})", bytes_str, eta_str))
                        .size(12.5)
                        .monospace()
                        .strong()
                        .color(COLOR_TEXT_PRIMARY),
                );
            });

            // Col 2: Window Base & Chunks
            cols[2].vertical(|ui| {
                ui.label(
                    RichText::new("CUMULATIVE BASE & CHUNKS")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(acked_str)
                        .size(12.5)
                        .monospace()
                        .strong()
                        .color(COLOR_SUCCESS),
                );
            });

            // Col 3: Karn RTT / Dynamic RTO
            cols[3].vertical(|ui| {
                ui.label(
                    RichText::new("RTT (KARN) / DYNAMIC RTO")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(rtt_rto_str)
                        .size(12.5)
                        .monospace()
                        .strong()
                        .color(COLOR_TEXT_PRIMARY),
                );
            });
        });

        ui.add_space(4.0_f32);

        // Row 2 of metrics
        ui.columns(4, |cols| {
            cols[0].vertical(|ui| {
                ui.label(
                    RichText::new("PACKET TRAFFIC")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(pkts_str)
                        .size(11.5)
                        .monospace()
                        .strong()
                        .color(COLOR_TEXT_PRIMARY),
                );
            });

            cols[1].vertical(|ui| {
                ui.label(
                    RichText::new("WINDOW OCCUPANCY")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                );
                ui.label(
                    RichText::new(in_flight_str)
                        .size(11.5)
                        .monospace()
                        .strong()
                        .color(COLOR_INFO),
                );
            });

            cols[2].vertical(|ui| {
                ui.label(
                    RichText::new("ACTUAL RETRANSMISSIONS")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                )
                .on_hover_text(
                    "Observed protocol retransmissions triggered by RTO timeout or wire flag.",
                );
                ui.label(
                    RichText::new(retrans_str)
                        .size(11.5)
                        .monospace()
                        .strong()
                        .color(COLOR_WARNING),
                );
            });

            cols[3].vertical(|ui| {
                ui.label(
                    RichText::new("SIMULATED LOSS (TEST)")
                        .size(10.0)
                        .strong()
                        .color(COLOR_TEXT_MUTED),
                )
                .on_hover_text(
                    "Synthetic drop probability configured for evaluation. This is a testing configuration, not a measured network loss rate.",
                );
                ui.label(
                    RichText::new(sim_loss_str)
                        .size(11.5)
                        .monospace()
                        .color(COLOR_TEXT_MUTED),
                );
            });
        });
    }

    fn render_packet_matrix_dashboard(&mut self, ui: &mut Ui) {
        // Toolbar: Title + Perspective Selector + Pause/Replay Controls
        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("SLIDING WINDOW & SACK PACKET MATRIX")
                    .size(11.0)
                    .strong()
                    .color(COLOR_TEXT_SECONDARY),
            );

            ui.separator();

            ui.label(
                RichText::new("Perspective:")
                    .size(11.5)
                    .color(COLOR_TEXT_MUTED),
            );
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::Auto,
                RichText::new("Auto").size(11.5),
            );
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::SenderWindow,
                RichText::new("Sender Window").size(11.5),
            );
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::ReceiverBuffer,
                RichText::new("Receiver SACK Buffer").size(11.5),
            );

            ui.separator();

            // Demonstration Controls: Pause/Resume Visualization (never pauses background network worker)
            let pause_label = if self.vis_paused {
                RichText::new("▶ Resume Live").color(COLOR_SUCCESS).size(11.5).strong()
            } else {
                RichText::new("⏸ Pause Display").color(COLOR_WARNING).size(11.5)
            };
            if ui
                .button(pause_label)
                .on_hover_text(
                    "Freezes the packet matrix display for inspection while the network worker continues transferring at full speed in the background.",
                )
                .clicked()
            {
                self.vis_paused = !self.vis_paused;
                if self.vis_paused {
                    if !self.snapshot_history.is_empty() {
                        self.replay_frame_index = Some(self.snapshot_history.len() - 1);
                    }
                } else {
                    self.replay_frame_index = None;
                }
            }

            let history_len = self.snapshot_history.len();
            if history_len > 1 {
                let current_idx = self
                    .replay_frame_index
                    .unwrap_or_else(|| history_len.saturating_sub(1));

                if ui
                    .add_enabled(current_idx > 0, Button::new("◀ Step"))
                    .on_hover_text("Step backward one snapshot frame")
                    .clicked()
                {
                    self.vis_paused = true;
                    self.replay_frame_index = Some(current_idx.saturating_sub(1));
                }
                if ui
                    .add_enabled(current_idx + 1 < history_len, Button::new("Step ▶"))
                    .on_hover_text("Step forward one snapshot frame")
                    .clicked()
                {
                    self.vis_paused = true;
                    self.replay_frame_index = Some((current_idx + 1).min(history_len - 1));
                }

                let mut slider_idx = current_idx + 1;
                let slider_resp = ui.add(
                    egui::Slider::new(&mut slider_idx, 1..=history_len)
                        .text("Frame")
                        .show_value(true),
                );
                if slider_resp.changed() {
                    self.vis_paused = true;
                    self.replay_frame_index = Some(slider_idx.saturating_sub(1));
                }
            }
        });

        ui.add_space(4.0_f32);

        // Snapshot inspection
        let Some(st) = self.active_display_stats().cloned() else {
            ui.label(
                RichText::new(
                    "No active transfer telemetry. Start a transfer or listener to inspect live sliding-window and SACK protocol state.",
                )
                .size(11.5)
                .color(COLOR_TEXT_MUTED),
            );
            return;
        };

        // Determine effective perspective and observability notes
        let effective_perspective = match self.perspective_selection {
            PerspectiveSelection::Auto => st.perspective,
            PerspectiveSelection::SenderWindow => WindowPerspective::Sender,
            PerspectiveSelection::ReceiverBuffer => WindowPerspective::Receiver,
        };

        // Sliding window boundaries & SACK summary banner
        let window_end = (st.window_base + st.window_size as u32).min(st.total_chunks);
        ui.horizontal_wrapped(|ui| {
            let role_text = match (st.perspective, effective_perspective) {
                (WindowPerspective::Sender, WindowPerspective::Sender) => {
                    "Authoritative Sender Window"
                }
                (WindowPerspective::Sender, WindowPerspective::Receiver) => {
                    "Receiver SACK Buffer (Projected from Peer ACKs)"
                }
                (WindowPerspective::Receiver, WindowPerspective::Receiver) => {
                    "Authoritative Receiver Out-of-Order Buffer"
                }
                (WindowPerspective::Receiver, WindowPerspective::Sender) => {
                    "Sender View (Limited to arrived packets)"
                }
            };
            ui.label(
                RichText::new(format!("[{}]", role_text))
                    .color(COLOR_PRIMARY)
                    .strong()
                    .size(11.5),
            );
            ui.separator();
            ui.label(
                RichText::new(format!("Base (ack_num): #{}", st.window_base))
                    .color(COLOR_SUCCESS)
                    .monospace()
                    .size(11.5),
            );
            ui.separator();
            let seq_label = match st.perspective {
                WindowPerspective::Sender => format!("Next Seq: #{}", st.next_seq),
                WindowPerspective::Receiver => format!("Highest Observed: #{}", st.next_seq.saturating_sub(1)),
            };
            ui.label(RichText::new(seq_label).monospace().size(11.5));
            ui.separator();
            ui.label(
                RichText::new(format!(
                    "Active Window: [#{} .. #{}) (size {})",
                    st.window_base, window_end, st.window_size
                ))
                .color(COLOR_WINDOW_HIGHLIGHT)
                .monospace()
                .size(11.5),
            );
            ui.separator();
            ui.label(
                RichText::new(format!("SACK Bitmask: 0x{:016X}", st.last_sack_bitmask))
                    .color(COLOR_WARNING)
                    .monospace()
                    .size(11.5),
            );
        });

        ui.add_space(4.0_f32);

        // Full-File Minimap Bar
        self.render_full_file_minimap(ui, &st, window_end);

        ui.add_space(5.0_f32);

        // Project visible chunks according to selected perspective
        let displayed_chunks: Vec<ChunkSnapshot> = st
            .visible_chunks
            .iter()
            .map(|c| project_chunk_for_perspective(c, &st, effective_perspective))
            .collect();

        // Count states in visible range for Legend
        let mut count_acked = 0usize;
        let mut count_sacked = 0usize;
        let mut count_retrans = 0usize;
        let mut count_inflight = 0usize;
        let mut count_pending = 0usize;
        let mut count_retrans_history = 0usize;

        for c in &displayed_chunks {
            match c.state {
                ChunkState::Acked => count_acked += 1,
                ChunkState::Sacked => count_sacked += 1,
                ChunkState::Retransmitting => count_retrans += 1,
                ChunkState::InFlight => count_inflight += 1,
                ChunkState::Pending => count_pending += 1,
            }
            if c.had_retransmission {
                count_retrans_history += 1;
            }
        }

        // Functional legend with color, shape cue, and symbol
        ui.horizontal_wrapped(|ui| {
            render_legend_badge(
                ui,
                COLOR_STATE_ACKED_BG,
                COLOR_STATE_ACKED_BORDER,
                &format!("✓ Acked ({})", count_acked),
                "Cumulatively acknowledged (seq < base). Top horizontal accent bar.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_SACKED_BG,
                COLOR_STATE_SACKED_BORDER,
                &format!("S SACKed ({})", count_sacked),
                "Selectively acknowledged out-of-order (seq > base). Left vertical accent bar.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_RETRANS_BG,
                COLOR_STATE_RETRANS_BORDER,
                &format!("↻ Retransmitting ({})", count_retrans),
                "RTO expired or retry in-flight. Bottom warning accent bar.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_INFLIGHT_BG,
                COLOR_STATE_INFLIGHT_BORDER,
                &format!("→ In-Flight / Gap ({})", count_inflight),
                "Transmission in-flight (Sender) or unreceived sequence gap (Receiver).",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_PENDING_BG,
                COLOR_STATE_PENDING_BORDER,
                &format!("· Pending ({})", count_pending),
                "Not yet transmitted (seq >= next_seq).",
            );
            ui.label(
                RichText::new(format!("● Retrans History ({})", count_retrans_history))
                    .size(11.0)
                    .color(COLOR_WARNING),
            )
            .on_hover_text(
                "Orange corner marker identifies chunks that experienced >= 1 retransmission, preserved even after cumulative ACK.",
            );
        });

        ui.add_space(4.0_f32);

        // Dynamically compute column count based on available window width
        let avail_width = ui.available_width().max(320.0_f32);
        let spacing = 3.0_f32;
        let target_cell_width = 46.0_f32;
        let cols = ((avail_width + spacing) / (target_cell_width + spacing))
            .floor()
            .clamp(16.0_f32, 28.0_f32) as usize;
        let cell_width = ((avail_width - spacing * (cols as f32 - 1.0_f32)) / (cols as f32)).clamp(34.0_f32, 54.0_f32);
        let cell_height = 23.0_f32;

        for row_chunks in displayed_chunks.chunks(cols) {
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing = Vec2::new(spacing, spacing);
                for chunk in row_chunks {
                    let is_in_active_window =
                        chunk.seq_num >= st.window_base && chunk.seq_num < window_end;
                    let is_base_chunk = chunk.seq_num == st.window_base;
                    let is_pinned = self.pinned_chunk_seq == Some(chunk.seq_num);

                    let (rect, resp) =
                        ui.allocate_exact_size(Vec2::new(cell_width, cell_height), Sense::click());

                    if ui.is_rect_visible(rect) {
                        draw_chunk_cell(
                            ui,
                            rect,
                            chunk,
                            is_in_active_window,
                            is_base_chunk,
                            is_pinned,
                        );
                    }

                    if resp.clicked() {
                        if self.pinned_chunk_seq == Some(chunk.seq_num) {
                            self.pinned_chunk_seq = None;
                        } else {
                            self.pinned_chunk_seq = Some(chunk.seq_num);
                            self.chunk_inspection_cache
                                .insert(chunk.seq_num, chunk.clone());
                        }
                    }

                    resp.on_hover_ui(|ui| {
                        render_chunk_tooltip(ui, chunk, &st, is_in_active_window);
                    });
                }
            });
            ui.add_space(2.0_f32);
        }

        ui.add_space(4.0_f32);

        // Pinned Chunk Inspector Panel
        self.render_pinned_chunk_inspector(ui, &st);
    }

    fn render_full_file_minimap(&self, ui: &mut Ui, st: &TransferStats, window_end: u32) {
        let total = st.total_chunks.max(1) as f32;
        let vis_start = st.visible_chunk_start;
        let vis_end = (vis_start + st.visible_chunks.len() as u32).min(st.total_chunks);

        ui.horizontal(|ui| {
            ui.label(
                RichText::new(format!(
                    "File Chunk Minimap [0 .. #{}]: Viewing viewport #{}..#{}",
                    st.total_chunks, vis_start, vis_end
                ))
                .size(11.0)
                .monospace()
                .color(COLOR_TEXT_MUTED),
            );
        });

        let desired_size = Vec2::new(ui.available_width().max(200.0_f32), 11.0_f32);
        let (rect, resp) = ui.allocate_exact_size(desired_size, Sense::hover());

        if ui.is_rect_visible(rect) {
            let painter = ui.painter();
            // Background (Pending)
            painter.rect_filled(rect, Rounding::same(2.0_f32), COLOR_STATE_PENDING_BG);

            // Cumulatively ACKed region [0 .. window_base)
            let ack_ratio = (st.window_base as f32 / total).clamp(0.0_f32, 1.0_f32);
            if ack_ratio > 0.0_f32 {
                let ack_rect = Rect::from_min_max(
                    rect.min,
                    Pos2::new(rect.min.x + rect.width() * ack_ratio, rect.max.y),
                );
                painter.rect_filled(ack_rect, Rounding::same(2.0_f32), COLOR_STATE_ACKED_BORDER);
            }

            // Active Window region [window_base .. window_end)
            let win_start_ratio = ack_ratio;
            let win_end_ratio = (window_end as f32 / total).clamp(0.0_f32, 1.0_f32);
            if win_end_ratio > win_start_ratio {
                let win_rect = Rect::from_min_max(
                    Pos2::new(rect.min.x + rect.width() * win_start_ratio, rect.min.y),
                    Pos2::new(
                        (rect.min.x + rect.width() * win_end_ratio)
                            .max(rect.min.x + rect.width() * win_start_ratio + 3.0_f32),
                        rect.max.y,
                    ),
                );
                painter.rect_filled(win_rect, Rounding::same(1.0_f32), COLOR_WINDOW_HIGHLIGHT);
            }

            // Visible matrix viewport bracket [vis_start .. vis_end)
            let vp_start_ratio = (vis_start as f32 / total).clamp(0.0_f32, 1.0_f32);
            let vp_end_ratio = (vis_end as f32 / total).clamp(0.0_f32, 1.0_f32);
            let vp_rect = Rect::from_min_max(
                Pos2::new(rect.min.x + rect.width() * vp_start_ratio, rect.min.y),
                Pos2::new(
                    (rect.min.x + rect.width() * vp_end_ratio)
                        .max(rect.min.x + rect.width() * vp_start_ratio + 3.0_f32),
                    rect.max.y,
                ),
            );
            painter.rect_stroke(
                vp_rect,
                Rounding::same(2.0_f32),
                Stroke::new(1.0_f32, Color32::WHITE),
            );
        }

        resp.on_hover_text(format!(
            "Total Chunks: {}\nCumulative Base: #{}\nActive Sliding Window: [#{}..#{})\nVisible Matrix Viewport: [#{}..#{})",
            st.total_chunks, st.window_base, st.window_base, window_end, vis_start, vis_end
        ));
    }

    fn render_pinned_chunk_inspector(&mut self, ui: &mut Ui, st: &TransferStats) {
        egui::Frame::none()
            .fill(Color32::from_rgb(17, 21, 29))
            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
            .rounding(Rounding::same(3.0_f32))
            .inner_margin(8.0_f32)
            .show(ui, |ui| {
                let Some(pinned_seq) = self.pinned_chunk_seq else {
                    ui.label(
                        RichText::new(
                            "Chunk Inspector: Hover over any matrix cell for live telemetry, or click to pin byte-offsets & timestamps here.",
                        )
                        .size(11.0)
                        .color(COLOR_TEXT_MUTED),
                    );
                    return;
                };

                let chunk = st
                    .visible_chunks
                    .iter()
                    .find(|c| c.seq_num == pinned_seq)
                    .cloned()
                    .or_else(|| self.chunk_inspection_cache.get(&pinned_seq).cloned())
                    .unwrap_or_else(|| {
                        let state = if pinned_seq < st.window_base {
                            ChunkState::Acked
                        } else {
                            ChunkState::Pending
                        };
                        ChunkSnapshot {
                            seq_num: pinned_seq,
                            state,
                            retransmit_count: 0,
                            had_retransmission: false,
                            last_retransmit_reason: None,
                            ack_kind: if pinned_seq < st.window_base {
                                AckKind::Cumulative
                            } else {
                                AckKind::None
                            },
                            first_event_elapsed: None,
                            last_sent_elapsed: None,
                            acked_elapsed: None,
                        }
                    });

                let (byte_offset, byte_len) =
                    chunk_byte_range(chunk.seq_num, st.chunk_size, st.total_bytes);
                let byte_end = byte_offset + byte_len;

                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        RichText::new(format!("Pinned Chunk #{}", chunk.seq_num))
                            .strong()
                            .monospace()
                            .color(COLOR_PINNED_HIGHLIGHT)
                            .size(12.5),
                    );
                    if ui
                        .add_enabled(chunk.seq_num > 0, Button::new("◀ Prev"))
                        .clicked()
                    {
                        self.pinned_chunk_seq = Some(chunk.seq_num - 1);
                    }
                    if ui
                        .add_enabled(
                            chunk.seq_num + 1 < st.total_chunks,
                            Button::new("Next ▶"),
                        )
                        .clicked()
                    {
                        self.pinned_chunk_seq = Some(chunk.seq_num + 1);
                    }
                    if ui.button("Unpin ✖").clicked() {
                        self.pinned_chunk_seq = None;
                    }
                });

                ui.add_space(2.0_f32);

                ui.columns(3, |cols| {
                    cols[0].vertical(|ui| {
                        ui.label(
                            RichText::new(format!(
                                "State: {} {}",
                                chunk.state.symbol(),
                                chunk.state.label()
                            ))
                            .strong()
                            .size(11.5)
                            .color(COLOR_TEXT_PRIMARY),
                        );
                        ui.label(
                            RichText::new(format!(
                                "Byte Range: {} .. {} ({} B)",
                                byte_offset, byte_end, byte_len
                            ))
                            .monospace()
                            .size(11.0)
                            .color(COLOR_TEXT_SECONDARY),
                        );
                    });
                    cols[1].vertical(|ui| {
                        ui.label(
                            RichText::new(format!("ACK Type: {}", chunk.ack_kind.label()))
                                .size(11.5)
                                .color(COLOR_TEXT_PRIMARY),
                        );
                        let retrans_text = match chunk.last_retransmit_reason {
                            Some(reason) => format!(
                                "Retransmissions: {} ({})",
                                chunk.retransmit_count,
                                reason.label()
                            ),
                            None => format!("Retransmissions: {}", chunk.retransmit_count),
                        };
                        ui.label(
                            RichText::new(retrans_text).size(11.0).color(
                                if chunk.had_retransmission {
                                    COLOR_WARNING
                                } else {
                                    COLOR_TEXT_MUTED
                                },
                            ),
                        );
                    });
                    cols[2].vertical(|ui| {
                        let first_str = chunk
                            .first_event_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "--".to_string());
                        let last_str = chunk
                            .last_sent_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "--".to_string());
                        let ack_str = chunk
                            .acked_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "--".to_string());
                        ui.label(
                            RichText::new(format!("First: {} | Last Tx: {}", first_str, last_str))
                                .monospace()
                                .size(10.5)
                                .color(COLOR_TEXT_SECONDARY),
                        );
                        ui.label(
                            RichText::new(format!("Confirmed: {}", ack_str))
                                .monospace()
                                .size(10.5)
                                .color(COLOR_SUCCESS),
                        );
                    });
                });
            });
    }

    fn render_timeline_panel(&mut self, ui: &mut Ui) {
        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("PROTOCOL EVENT TIMELINE & LOGS")
                    .size(11.0)
                    .strong()
                    .color(COLOR_TEXT_SECONDARY),
            );

            ui.separator();

            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::All,
                RichText::new("All").size(11.0),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::WindowAndSack,
                RichText::new("Window & SACK").size(11.0),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::RetransAndTimeout,
                RichText::new("Timeouts & Retrans").size(11.0),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::DataAndAck,
                RichText::new("Data & ACK").size(11.0),
            );

            ui.separator();

            // Search filter box
            ui.add(
                egui::TextEdit::singleline(&mut self.log_search_query)
                    .desired_width(140.0_f32)
                    .hint_text("Filter logs..."),
            );

            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui.button("Clear Logs").clicked() {
                    self.timeline_events.clear();
                    self.logs.clear();
                }

                if ui
                    .button("Copy Logs")
                    .on_hover_text("Copy visible events and logs to system clipboard")
                    .clicked()
                {
                    let mut export = String::new();
                    for ev in &self.timeline_events {
                        export.push_str(&format!(
                            "[+{:>7.3}s] [{:<9}] {}\n",
                            ev.elapsed.as_secs_f64(),
                            ev.kind.tag(),
                            ev.message
                        ));
                    }
                    for log in &self.logs {
                        export.push_str(log);
                        export.push('\n');
                    }
                    ui.ctx().copy_text(export);
                }
            });
        });

        ui.add_space(2.0_f32);

        // Structured Technical Log Table
        egui::Frame::none()
            .fill(Color32::from_rgb(12, 15, 20))
            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
            .rounding(Rounding::same(3.0_f32))
            .inner_margin(6.0_f32)
            .show(ui, |ui| {
                ScrollArea::vertical()
                    .id_salt("protocol_timeline_scroll")
                    .stick_to_bottom(!self.vis_paused)
                    .max_height(145.0_f32)
                    .show(ui, |ui| {
                        if self.timeline_events.is_empty() && self.logs.is_empty() {
                            ui.label(
                                RichText::new("No protocol events recorded yet.")
                                    .size(11.0)
                                    .color(COLOR_TEXT_MUTED),
                            );
                            return;
                        }

                        let search_term = self.log_search_query.trim().to_lowercase();

                        // Render structured protocol telemetry events filtered by user selection & search
                        for ev in &self.timeline_events {
                            let category_matches = match self.timeline_filter {
                                TimelineFilter::All => true,
                                TimelineFilter::WindowAndSack => matches!(
                                    ev.kind,
                                    ProtocolEventKind::SackUpdate
                                        | ProtocolEventKind::WindowAdvanced
                                        | ProtocolEventKind::Handshake
                                        | ProtocolEventKind::Verification
                                        | ProtocolEventKind::Completed
                                ),
                                TimelineFilter::RetransAndTimeout => matches!(
                                    ev.kind,
                                    ProtocolEventKind::TimeoutDetected
                                        | ProtocolEventKind::Retransmission
                                ),
                                TimelineFilter::DataAndAck => matches!(
                                    ev.kind,
                                    ProtocolEventKind::DataSent | ProtocolEventKind::AckReceived
                                ),
                            };
                            if !category_matches {
                                continue;
                            }

                            if !search_term.is_empty()
                                && !ev.message.to_lowercase().contains(&search_term)
                                && !ev.kind.tag().to_lowercase().contains(&search_term)
                            {
                                continue;
                            }

                            let tag_color = match ev.kind {
                                ProtocolEventKind::TimeoutDetected
                                | ProtocolEventKind::Retransmission => COLOR_DANGER,
                                ProtocolEventKind::SackUpdate => COLOR_WARNING,
                                ProtocolEventKind::WindowAdvanced
                                | ProtocolEventKind::Completed
                                | ProtocolEventKind::Verification => COLOR_SUCCESS,
                                ProtocolEventKind::Handshake => COLOR_PRIMARY,
                                _ => COLOR_INFO,
                            };

                            ui.horizontal(|ui| {
                                ui.spacing_mut().item_spacing.x = 8.0_f32;
                                ui.label(
                                    RichText::new(format!("+{:>7.3}s", ev.elapsed.as_secs_f64()))
                                        .monospace()
                                        .size(10.5)
                                        .color(COLOR_TEXT_MUTED),
                                );
                                ui.label(
                                    RichText::new(format!("[{:<10}]", ev.kind.tag()))
                                        .monospace()
                                        .size(10.5)
                                        .strong()
                                        .color(tag_color),
                                );
                                ui.label(
                                    RichText::new(&ev.message)
                                        .monospace()
                                        .size(11.0)
                                        .color(COLOR_TEXT_PRIMARY),
                                );
                            });
                        }

                        // Also show general application status logs when filter is All
                        if self.timeline_filter == TimelineFilter::All {
                            for log in &self.logs {
                                if !search_term.is_empty()
                                    && !log.to_lowercase().contains(&search_term)
                                {
                                    continue;
                                }

                                let color = if log.contains("ERROR") || log.contains("failed") {
                                    COLOR_DANGER
                                } else if log.contains("SUCCESS") || log.contains("verified") {
                                    COLOR_SUCCESS
                                } else {
                                    COLOR_TEXT_MUTED
                                };
                                ui.label(RichText::new(log).color(color).monospace().size(10.5));
                            }
                        }
                    });
            });
    }
}

/// Project a `ChunkSnapshot` when the user toggles between Sender Window and Receiver SACK Buffer perspectives.
fn project_chunk_for_perspective(
    chunk: &ChunkSnapshot,
    st: &TransferStats,
    target_perspective: WindowPerspective,
) -> ChunkSnapshot {
    if st.perspective == target_perspective {
        return chunk.clone();
    }

    if st.perspective == WindowPerspective::Sender
        && target_perspective == WindowPerspective::Receiver
    {
        let mut projected = chunk.clone();
        if chunk.seq_num < st.window_base {
            projected.state = ChunkState::Acked;
        } else if chunk.state == ChunkState::Sacked {
            projected.state = ChunkState::Sacked;
        } else {
            let highest_sacked = st
                .visible_chunks
                .iter()
                .filter(|c| c.state == ChunkState::Sacked)
                .map(|c| c.seq_num)
                .max();
            if highest_sacked.is_some_and(|hi| chunk.seq_num < hi) {
                projected.state = ChunkState::InFlight;
            } else {
                projected.state = ChunkState::Pending;
            }
        }
        return projected;
    }

    chunk.clone()
}

fn render_legend_badge(ui: &mut Ui, bg: Color32, border: Color32, text: &str, tooltip: &str) {
    let resp = egui::Frame::none()
        .fill(bg)
        .stroke(Stroke::new(1.0_f32, border))
        .rounding(Rounding::same(2.0_f32))
        .inner_margin(egui::Margin::symmetric(5.0_f32, 2.0_f32))
        .show(ui, |ui| {
            ui.label(
                RichText::new(text)
                    .size(10.5)
                    .color(Color32::WHITE)
                    .strong(),
            );
        })
        .response;
    resp.on_hover_text(tooltip);
}

fn render_segment_tab(ui: &mut Ui, label: &str, selected: bool) -> bool {
    let (bg, stroke, text_color) = if selected {
        (COLOR_PRIMARY, Stroke::NONE, Color32::WHITE)
    } else {
        (
            Color32::from_rgb(24, 28, 38),
            Stroke::new(1.0_f32, COLOR_CARD_BORDER),
            COLOR_TEXT_SECONDARY,
        )
    };

    let button = Button::new(RichText::new(label).size(12.0).strong().color(text_color))
        .fill(bg)
        .stroke(stroke)
        .rounding(Rounding::same(3.0_f32));

    ui.add(button).clicked()
}

fn draw_chunk_cell(
    ui: &mut Ui,
    rect: Rect,
    chunk: &ChunkSnapshot,
    is_in_active_window: bool,
    is_base_chunk: bool,
    is_pinned: bool,
) {
    let painter = ui.painter();

    let (bg_color, border_color) = match chunk.state {
        ChunkState::Pending => (COLOR_STATE_PENDING_BG, COLOR_STATE_PENDING_BORDER),
        ChunkState::InFlight => (COLOR_STATE_INFLIGHT_BG, COLOR_STATE_INFLIGHT_BORDER),
        ChunkState::Retransmitting => (COLOR_STATE_RETRANS_BG, COLOR_STATE_RETRANS_BORDER),
        ChunkState::Sacked => (COLOR_STATE_SACKED_BG, COLOR_STATE_SACKED_BORDER),
        ChunkState::Acked => (COLOR_STATE_ACKED_BG, COLOR_STATE_ACKED_BORDER),
    };

    painter.rect_filled(rect, Rounding::same(2.0_f32), bg_color);

    // Shape cues for accessible distinction without depending solely on color:
    match chunk.state {
        ChunkState::Acked => {
            // Top horizontal confirmation accent bar
            let bar = Rect::from_min_max(rect.min, Pos2::new(rect.max.x, rect.min.y + 2.5_f32));
            painter.rect_filled(bar, Rounding::same(1.0_f32), COLOR_STATE_ACKED_BORDER);
        }
        ChunkState::Sacked => {
            // Left vertical accent bar indicating out-of-order SACK buffer
            let bar = Rect::from_min_max(rect.min, Pos2::new(rect.min.x + 3.0_f32, rect.max.y));
            painter.rect_filled(bar, Rounding::same(1.0_f32), COLOR_STATE_SACKED_BORDER);
        }
        ChunkState::Retransmitting => {
            // Bottom warning accent bar
            let bar = Rect::from_min_max(Pos2::new(rect.min.x, rect.max.y - 3.0_f32), rect.max);
            painter.rect_filled(bar, Rounding::same(1.0_f32), COLOR_STATE_RETRANS_BORDER);
        }
        _ => {}
    }

    // Persistent retransmission history cue: top-right dot when `had_retransmission` is true
    if chunk.had_retransmission {
        let center = Pos2::new(rect.max.x - 4.0_f32, rect.min.y + 4.0_f32);
        painter.circle_filled(center, 2.4_f32, COLOR_WARNING);
        painter.circle_stroke(center, 2.4_f32, Stroke::new(0.8_f32, Color32::BLACK));
    }

    // Active sliding window highlight & pinned border
    let stroke = if is_pinned {
        Stroke::new(2.0_f32, COLOR_PINNED_HIGHLIGHT)
    } else if is_base_chunk {
        Stroke::new(2.0_f32, COLOR_WINDOW_HIGHLIGHT)
    } else if is_in_active_window {
        Stroke::new(1.2_f32, COLOR_WINDOW_HIGHLIGHT)
    } else {
        Stroke::new(1.0_f32, border_color)
    };
    painter.rect_stroke(rect, Rounding::same(2.0_f32), stroke);

    // Sequence text with non-color state symbol
    let short_seq = chunk.seq_num % 10000;
    let label = format!("{}{}", chunk.state.symbol(), short_seq);
    let text_color = if chunk.state == ChunkState::Pending {
        COLOR_TEXT_MUTED
    } else {
        Color32::WHITE
    };

    painter.text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        label,
        FontId::monospace(9.5_f32),
        text_color,
    );
}

fn render_chunk_tooltip(
    ui: &mut Ui,
    chunk: &ChunkSnapshot,
    st: &TransferStats,
    is_in_active_window: bool,
) {
    let (byte_offset, byte_len) = chunk_byte_range(chunk.seq_num, st.chunk_size, st.total_bytes);
    ui.label(
        RichText::new(format!(
            "Chunk #{} of {}",
            chunk.seq_num,
            st.total_chunks.saturating_sub(1)
        ))
        .strong()
        .monospace()
        .color(COLOR_PRIMARY),
    );
    ui.separator();
    ui.label(format!(
        "State: {} {}",
        chunk.state.symbol(),
        chunk.state.label()
    ));
    ui.label(format!("ACK Status: {}", chunk.ack_kind.label()));
    ui.label(format!(
        "Byte Range: {} .. {} ({} B)",
        byte_offset,
        byte_offset + byte_len,
        byte_len
    ));
    ui.label(format!(
        "In Active Window: {}",
        if is_in_active_window { "Yes" } else { "No" }
    ));
    ui.label(format!("Retransmissions: {}", chunk.retransmit_count));
    if let Some(reason) = chunk.last_retransmit_reason {
        ui.label(
            RichText::new(format!("Reason: {}", reason.label())).color(COLOR_WARNING),
        );
    }
    if let Some(first_t) = chunk.first_event_elapsed {
        ui.label(format!(
            "First Event: +{:.1} ms",
            first_t.as_secs_f64() * 1000.0
        ));
    }
    if let Some(last_t) = chunk.last_sent_elapsed {
        ui.label(format!(
            "Last Sent: +{:.1} ms",
            last_t.as_secs_f64() * 1000.0
        ));
    }
    if let Some(ack_t) = chunk.acked_elapsed {
        ui.label(format!(
            "Confirmed: +{:.1} ms",
            ack_t.as_secs_f64() * 1000.0
        ));
    }
    ui.separator();
    ui.label(
        RichText::new("Click cell to pin in the Chunk Inspector below.")
            .size(10.5)
            .color(COLOR_TEXT_MUTED),
    );
}

fn format_file_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn chrono_free_timestamp() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}
