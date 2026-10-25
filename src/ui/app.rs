use crate::network::receiver::{run_receiver, ReceiverConfig};
use crate::network::sender::{run_sender, SenderConfig, TransferEvent};
use crate::protocol::stats::{
    chunk_byte_range, AckKind, ChunkSnapshot, ChunkState, ProtocolEventKind,
    ProtocolTelemetryEvent, TransferStats, WindowPerspective,
};
use crate::ui::theme::{
    configure_theme, COLOR_CARD_BG, COLOR_CARD_BORDER, COLOR_DANGER, COLOR_PINNED_HIGHLIGHT,
    COLOR_PRIMARY, COLOR_STATE_ACKED_BG, COLOR_STATE_ACKED_BORDER, COLOR_STATE_INFLIGHT_BG,
    COLOR_STATE_INFLIGHT_BORDER, COLOR_STATE_PENDING_BG, COLOR_STATE_PENDING_BORDER,
    COLOR_STATE_RETRANS_BG, COLOR_STATE_RETRANS_BORDER, COLOR_STATE_SACKED_BG,
    COLOR_STATE_SACKED_BORDER, COLOR_SUCCESS, COLOR_WARNING, COLOR_WINDOW_HIGHLIGHT,
};
use crossbeam_channel::{bounded, Receiver as ChannelReceiver, Sender as ChannelSender};
use eframe::egui::{
    self, Align, Button, Color32, FontId, Layout, Pos2, ProgressBar, Rect, RichText, Rounding,
    ScrollArea, Sense, Stroke, Ui, Vec2,
};
use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
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
        logs.push_back("Application initialized. Select Sender or Receiver mode.".to_string());

        Self {
            mode: AppMode::Sender,
            state: TransferState::Idle,
            status_message: "Ready to transfer files over UDP.".to_string(),
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
            self.add_log(format!("Refreshed local IP address: {}", self.local_ip));
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
            self.status_message = "Selected file does not exist!".to_string();
            self.add_log(self.status_message.clone());
            return;
        }

        let target_ip: IpAddr = match self.target_ip.trim().parse() {
            Ok(ip) => ip,
            Err(_) => {
                self.state = TransferState::Failed;
                self.status_message = "Invalid Receiver IP address!".to_string();
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
            "Starting Sender -> Target: {}, Window: {}, Chunk Size: {}B, Configured Sim. Drop: {}%",
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
            "Starting Receiver on UDP port {}, Configured Sim. Drop: {}%",
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
        self.add_log("Transfer cancelled.".to_string());
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

        // If pinned chunk is now behind cumulative window_base, ensure its cached state reflects Cumulative Ack
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

        // Always keep live `self.stats` updated so worker progress is tracked even if UI visualization is paused
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
                    self.add_log("Cancelled event received.".to_string());
                }
            }
        }
    }

    /// Return the snapshot currently displayed in the observability dashboard (either live or paused/replay frame).
    fn active_display_stats(&self) -> Option<&TransferStats> {
        if let Some(idx) = self.replay_frame_index {
            if let Some(frame) = self.snapshot_history.get(idx) {
                return Some(frame);
            }
        }
        self.stats.as_ref()
    }
}

impl eframe::App for TransferApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_events();

        // Repaint periodically if active to keep live stats updated
        if self.state == TransferState::Active {
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            // Header
            ui.horizontal(|ui| {
                ui.heading(
                    RichText::new("Reliable UDP File Transfer — Protocol Observability Dashboard")
                        .color(COLOR_PRIMARY)
                        .size(19.0)
                        .strong(),
                );
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.button("Refresh IP").clicked() {
                        self.refresh_local_ip();
                    }
                    ui.label(
                        RichText::new(format!("Local IP: {}", self.local_ip))
                            .color(Color32::LIGHT_GRAY),
                    );
                });
            });

            ui.add_space(4.0);

            // Mode Selector Tabs (Sender / Receiver)
            ui.horizontal(|ui| {
                let is_active = self.state == TransferState::Active;
                ui.add_enabled_ui(!is_active, |ui| {
                    ui.selectable_value(
                        &mut self.mode,
                        AppMode::Sender,
                        RichText::new("  Sender Mode  ").size(14.0).strong(),
                    );
                    ui.selectable_value(
                        &mut self.mode,
                        AppMode::Receiver,
                        RichText::new("  Receiver Mode  ").size(14.0).strong(),
                    );
                });

                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    match self.state {
                        TransferState::Idle => ui.label(RichText::new("IDLE").color(Color32::GRAY)),
                        TransferState::Active => {
                            ui.label(RichText::new("RUNNING").color(COLOR_PRIMARY).strong())
                        }
                        TransferState::Completed => {
                            ui.label(RichText::new("COMPLETED").color(COLOR_SUCCESS).strong())
                        }
                        TransferState::Failed => {
                            ui.label(RichText::new("FAILED").color(COLOR_DANGER).strong())
                        }
                        TransferState::Cancelled => {
                            ui.label(RichText::new("CANCELLED").color(COLOR_WARNING))
                        }
                    };
                });
            });

            ui.separator();

            ScrollArea::vertical()
                .id_salt("main_dashboard_scroll")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    // 1. Main Configuration Card
                    egui::Frame::none()
                        .fill(COLOR_CARD_BG)
                        .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                        .rounding(8.0_f32)
                        .inner_margin(10.0_f32)
                        .show(ui, |ui| match self.mode {
                            AppMode::Sender => self.render_sender_config(ui),
                            AppMode::Receiver => self.render_receiver_config(ui),
                        });

                    ui.add_space(6.0);

                    // 2. Live Transfer Metrics Card
                    egui::Frame::none()
                        .fill(COLOR_CARD_BG)
                        .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                        .rounding(8.0_f32)
                        .inner_margin(10.0_f32)
                        .show(ui, |ui| {
                            self.render_metrics_panel(ui);
                        });

                    ui.add_space(6.0);

                    // 3. Live Packet Matrix & Sliding Window Observability Card
                    egui::Frame::none()
                        .fill(COLOR_CARD_BG)
                        .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                        .rounding(8.0_f32)
                        .inner_margin(10.0_f32)
                        .show(ui, |ui| {
                            self.render_packet_matrix_dashboard(ui);
                        });

                    ui.add_space(6.0);

                    // 4. Protocol Event Timeline & Logs Panel
                    self.render_timeline_panel(ui);
                });
        });
    }
}

impl TransferApp {
    fn render_sender_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label("File to Send:");
            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.selected_file_path).desired_width(340.0),
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

        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Receiver IP:");
            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.target_ip).desired_width(130.0),
            );

            ui.label("Port:");
            let mut port_str = self.port.to_string();
            if ui
                .add_enabled(
                    !is_active,
                    egui::TextEdit::singleline(&mut port_str).desired_width(55.0),
                )
                .changed()
            {
                if let Ok(p) = port_str.parse() {
                    self.port = p;
                }
            }

            ui.label("Window:");
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.window_size, 1..=64).text("pkts"),
            );

            ui.label("Sim. Drop:");
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.simulated_loss_percent, 0..=50).text("%"),
            );
        });

        ui.add_space(6.0);

        ui.horizontal(|ui| {
            if !is_active {
                if ui
                    .button(
                        RichText::new("  Start Transfer  ")
                            .color(Color32::WHITE)
                            .strong(),
                    )
                    .clicked()
                {
                    self.start_sender();
                }
            } else if ui
                .button(
                    RichText::new("  Cancel Transfer  ")
                        .color(COLOR_DANGER)
                        .strong(),
                )
                .clicked()
            {
                self.cancel_transfer();
            }

            if (self.state == TransferState::Failed || self.state == TransferState::Completed)
                && ui.button("Retry").clicked()
            {
                self.start_sender();
            }

            ui.label(RichText::new(&self.status_message).color(Color32::LIGHT_GRAY));
        });
    }

    fn render_receiver_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label("Save Directory:");
            ui.add_enabled(
                !is_active,
                egui::TextEdit::singleline(&mut self.destination_dir).desired_width(340.0),
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

        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Listening Port:");
            let mut port_str = self.port.to_string();
            if ui
                .add_enabled(
                    !is_active,
                    egui::TextEdit::singleline(&mut port_str).desired_width(60.0),
                )
                .changed()
            {
                if let Ok(p) = port_str.parse() {
                    self.port = p;
                }
            }

            ui.label("Configured Sim. Drop:");
            ui.add_enabled(
                !is_active,
                egui::Slider::new(&mut self.simulated_loss_percent, 0..=50)
                    .text("% synthetic drop"),
            );
        });

        ui.add_space(6.0);

        ui.horizontal(|ui| {
            if !is_active {
                if ui
                    .button(
                        RichText::new("  Start Listening  ")
                            .color(Color32::WHITE)
                            .strong(),
                    )
                    .clicked()
                {
                    self.start_receiver();
                }
            } else if ui
                .button(
                    RichText::new("  Stop Listening  ")
                        .color(COLOR_DANGER)
                        .strong(),
                )
                .clicked()
            {
                self.cancel_transfer();
            }

            ui.label(RichText::new(&self.status_message).color(Color32::LIGHT_GRAY));
        });
    }

    fn render_metrics_panel(&self, ui: &mut Ui) {
        let st_opt = self.active_display_stats();

        let (
            progress,
            speed,
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
                    .map(|r| format!("{:.1}ms", r.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "N/A".to_string());
                let rto_s = st
                    .current_rto
                    .map(|r| format!("{:.0}ms", r.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "N/A".to_string());
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
                        "{}/{} (Base #{})",
                        st.chunks_transferred, st.total_chunks, st.window_base
                    ),
                    inflight_label,
                    pkt_label,
                    format!(
                        "{} retries ({} drops)",
                        st.retransmissions,
                        st.corrupt_packets_dropped + st.duplicate_packets_dropped
                    ),
                    format!("{}% (Synthetic)", st.configured_loss_percent),
                    format!("{} / {}", rtt_s, rto_s),
                    st.estimated_remaining()
                        .map(|e| format!("{:.1}s", e.as_secs_f64()))
                        .unwrap_or_else(|| "--".to_string()),
                )
            }
            None => (
                0.0,
                "0 B/s".to_string(),
                "0 B / 0 B".to_string(),
                "0/0 (Base #0)".to_string(),
                "0 in-flight (0 SACKed)".to_string(),
                "0 DATA / 0 ACK".to_string(),
                "0 retries (0 drops)".to_string(),
                format!("{}% (Synthetic)", self.simulated_loss_percent),
                "-- / --".to_string(),
                "--".to_string(),
            ),
        };

        ui.add(
            ProgressBar::new(progress)
                .show_percentage()
                .animate(self.state == TransferState::Active && !self.vis_paused),
        );

        ui.add_space(6.0);

        // Row 1 of metrics
        ui.columns(4, |cols| {
            cols[0].vertical(|ui| {
                ui.label(RichText::new("Throughput").size(11.0).color(Color32::GRAY));
                ui.label(
                    RichText::new(speed)
                        .size(14.0)
                        .strong()
                        .color(COLOR_PRIMARY),
                );
            });
            cols[1].vertical(|ui| {
                ui.label(RichText::new("Bytes & ETA").size(11.0).color(Color32::GRAY));
                ui.label(
                    RichText::new(format!("{} (ETA {})", bytes_str, eta_str))
                        .size(13.0)
                        .strong(),
                );
            });
            cols[2].vertical(|ui| {
                ui.label(
                    RichText::new("Acked Chunks & Base")
                        .size(11.0)
                        .color(Color32::GRAY),
                );
                ui.label(
                    RichText::new(acked_str)
                        .size(13.0)
                        .strong()
                        .color(COLOR_SUCCESS),
                );
            });
            cols[3].vertical(|ui| {
                ui.label(
                    RichText::new("RTT (Karn) / Dynamic RTO")
                        .size(11.0)
                        .color(Color32::GRAY),
                );
                ui.label(RichText::new(rtt_rto_str).size(13.0).strong());
            });
        });

        ui.add_space(4.0);

        // Row 2 of metrics (Explicitly distinguishing Configured Simulated Loss from Observed Retransmissions)
        ui.columns(4, |cols| {
            cols[0].vertical(|ui| {
                ui.label(RichText::new("Packets Transmitted").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(pkts_str).size(12.5).strong());
            });
            cols[1].vertical(|ui| {
                ui.label(RichText::new("Window Occupancy").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(in_flight_str).size(12.5).strong());
            });
            cols[2].vertical(|ui| {
                ui.label(
                    RichText::new("Observed Retransmissions")
                        .size(11.0)
                        .color(Color32::GRAY),
                )
                .on_hover_text(
                    "Actual protocol retransmissions triggered by RTO timeouts (Sender) or observed via FLAG_RETRANSMISSION (Receiver).",
                );
                ui.label(RichText::new(retrans_str).size(12.5).strong().color(COLOR_WARNING));
            });
            cols[3].vertical(|ui| {
                ui.label(
                    RichText::new("Configured Sim. Drop (Not Measured Loss)")
                        .size(11.0)
                        .color(Color32::GRAY),
                )
                .on_hover_text(
                    "Artificial drop probability configured on ReliableUdpSocket for testing. This is a synthetic setting, not a measured physical network loss rate.",
                );
                ui.label(RichText::new(sim_loss_str).size(12.5).strong());
            });
        });
    }

    fn render_packet_matrix_dashboard(&mut self, ui: &mut Ui) {
        // Top bar: Title + Perspective Selector + Pause/Replay Controls
        ui.horizontal_wrapped(|ui| {
            ui.label(
                RichText::new("Live Sliding Window & SACK Packet Matrix")
                    .size(14.0)
                    .strong()
                    .color(COLOR_PRIMARY),
            );

            ui.separator();
            ui.label(RichText::new("Perspective:").size(12.0).color(Color32::GRAY));
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::Auto,
                RichText::new("Auto").size(12.0),
            );
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::SenderWindow,
                RichText::new("Sender Window").size(12.0),
            );
            ui.selectable_value(
                &mut self.perspective_selection,
                PerspectiveSelection::ReceiverBuffer,
                RichText::new("Receiver SACK Buffer").size(12.0),
            );

            ui.separator();

            // Demonstration Controls: Pause/Resume Visualization (never pauses network worker)
            let pause_label = if self.vis_paused {
                RichText::new("▶ Resume Live Vis").color(COLOR_SUCCESS).size(12.0).strong()
            } else {
                RichText::new("⏸ Pause Vis").color(COLOR_WARNING).size(12.0)
            };
            if ui
                .button(pause_label)
                .on_hover_text(
                    "Freezes the matrix display for inspection while the network worker continues running at full speed in the background.",
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
                    .on_hover_text("Step backward one 100ms snapshot frame")
                    .clicked()
                {
                    self.vis_paused = true;
                    self.replay_frame_index = Some(current_idx.saturating_sub(1));
                }
                if ui
                    .add_enabled(current_idx + 1 < history_len, Button::new("Step ▶"))
                    .on_hover_text("Step forward one 100ms snapshot frame")
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

        ui.add_space(4.0);

        // Clone the active snapshot for rendering so `self` can be mutated on cell click (pinning)
        let Some(st) = self.active_display_stats().cloned() else {
            ui.label(
                RichText::new(
                    "No active transfer snapshot yet. Start a transfer or receiver to inspect live sliding-window and SACK telemetry.",
                )
                .color(Color32::GRAY),
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
                    "Sender View (Limited: Receiver only observes arrived packets)"
                }
            };
            ui.label(
                RichText::new(format!("[{}]", role_text))
                    .color(COLOR_PRIMARY)
                    .strong()
                    .size(12.0),
            );
            ui.separator();
            ui.label(
                RichText::new(format!("Cumulative Base (ack_num): #{}", st.window_base))
                    .color(COLOR_SUCCESS)
                    .monospace()
                    .size(12.0),
            );
            ui.separator();
            let seq_label = match st.perspective {
                WindowPerspective::Sender => format!("Next Seq: #{}", st.next_seq),
                WindowPerspective::Receiver => format!("Highest Observed + 1: #{}", st.next_seq),
            };
            ui.label(RichText::new(seq_label).monospace().size(12.0));
            ui.separator();
            ui.label(
                RichText::new(format!(
                    "Active Window: [#{} .. #{}) (size {})",
                    st.window_base, window_end, st.window_size
                ))
                .color(COLOR_WINDOW_HIGHLIGHT)
                .monospace()
                .size(12.0),
            );
            ui.separator();
            ui.label(
                RichText::new(format!("SACK Mask: 0x{:016X}", st.last_sack_bitmask))
                    .color(COLOR_WARNING)
                    .monospace()
                    .size(12.0),
            );
        });

        if st.perspective == WindowPerspective::Receiver
            && effective_perspective == WindowPerspective::Sender
        {
            ui.label(
                RichText::new(
                    "Note: Receiver cannot observe sender-internal unreceived next_seq or RTO timers before packets arrive on the wire.",
                )
                .size(11.0)
                .color(COLOR_WARNING),
            );
        }

        ui.add_space(4.0);

        // Full-File Minimap Bar
        self.render_full_file_minimap(ui, &st, window_end);

        ui.add_space(6.0);

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

        // Legend with Color + Symbol + Shape cues
        ui.horizontal_wrapped(|ui| {
            render_legend_badge(
                ui,
                COLOR_STATE_ACKED_BG,
                COLOR_STATE_ACKED_BORDER,
                &format!("✓ Acked ({})", count_acked),
                "Cumulatively acknowledged (seq < base). Top bar + '✓' symbol.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_SACKED_BG,
                COLOR_STATE_SACKED_BORDER,
                &format!("S SACKed ({})", count_sacked),
                "Selectively acknowledged out-of-order (seq > base). Corner triangle + 'S' symbol.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_RETRANS_BG,
                COLOR_STATE_RETRANS_BORDER,
                &format!("↻ Retransmitting ({})", count_retrans),
                "RTO timeout expired; retry currently in-flight. Bottom warning bar + '↻' symbol.",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_INFLIGHT_BG,
                COLOR_STATE_INFLIGHT_BORDER,
                &format!("→ In-Flight / Gap ({})", count_inflight),
                "Initial transmission in-flight (Sender) or unreceived sequence gap below highest seq (Receiver).",
            );
            render_legend_badge(
                ui,
                COLOR_STATE_PENDING_BG,
                COLOR_STATE_PENDING_BORDER,
                &format!("· Pending ({})", count_pending),
                "Not yet transmitted (seq >= next_seq).",
            );
            ui.label(
                RichText::new(format!(
                    "● Retrans History Dot ({})",
                    count_retrans_history
                ))
                .size(11.5)
                .color(COLOR_WARNING),
            )
            .on_hover_text(
                "Orange/red top-right dot marks chunks that experienced >= 1 retransmission, preserved even after SACK or Cumulative ACK.",
            );
        });

        ui.add_space(4.0);

        // Render Matrix Grid (16 columns x up to 5 rows = 80 cells)
        let cols = 16usize;
        let spacing = 4.0f32;
        let avail_width = ui.available_width().max(320.0);
        let cell_width =
            ((avail_width - spacing * (cols as f32 - 1.0)) / (cols as f32)).clamp(32.0, 56.0);
        let cell_height = 24.0f32;

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
            ui.add_space(2.0);
        }

        ui.add_space(6.0);

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
                    "Full-File Minimap (0..{}): Viewing chunks #{}..#{}",
                    st.total_chunks, vis_start, vis_end
                ))
                .size(11.5)
                .color(Color32::LIGHT_GRAY),
            );
        });

        let desired_size = Vec2::new(ui.available_width().max(200.0), 14.0);
        let (rect, resp) = ui.allocate_exact_size(desired_size, Sense::hover());

        if ui.is_rect_visible(rect) {
            let painter = ui.painter();
            // Background (Pending)
            painter.rect_filled(rect, Rounding::same(4.0), COLOR_STATE_PENDING_BG);

            // Cumulatively ACKed region [0 .. window_base)
            let ack_ratio = (st.window_base as f32 / total).clamp(0.0, 1.0);
            if ack_ratio > 0.0 {
                let ack_rect = Rect::from_min_max(
                    rect.min,
                    Pos2::new(rect.min.x + rect.width() * ack_ratio, rect.max.y),
                );
                painter.rect_filled(ack_rect, Rounding::same(4.0), COLOR_STATE_ACKED_BORDER);
            }

            // Active Window region [window_base .. window_end)
            let win_start_ratio = ack_ratio;
            let win_end_ratio = (window_end as f32 / total).clamp(0.0, 1.0);
            if win_end_ratio > win_start_ratio {
                let win_rect = Rect::from_min_max(
                    Pos2::new(rect.min.x + rect.width() * win_start_ratio, rect.min.y),
                    Pos2::new(
                        (rect.min.x + rect.width() * win_end_ratio)
                            .max(rect.min.x + rect.width() * win_start_ratio + 3.0),
                        rect.max.y,
                    ),
                );
                painter.rect_filled(win_rect, Rounding::same(2.0), COLOR_WINDOW_HIGHLIGHT);
            }

            // Visible matrix viewport bracket [vis_start .. vis_end)
            let vp_start_ratio = (vis_start as f32 / total).clamp(0.0, 1.0);
            let vp_end_ratio = (vis_end as f32 / total).clamp(0.0, 1.0);
            let vp_rect = Rect::from_min_max(
                Pos2::new(rect.min.x + rect.width() * vp_start_ratio, rect.min.y),
                Pos2::new(
                    (rect.min.x + rect.width() * vp_end_ratio)
                        .max(rect.min.x + rect.width() * vp_start_ratio + 4.0),
                    rect.max.y,
                ),
            );
            painter.rect_stroke(
                vp_rect,
                Rounding::same(3.0),
                Stroke::new(1.5, Color32::WHITE),
            );
        }

        resp.on_hover_text(format!(
            "Full file: {} chunks\nCumulative Base: #{}\nActive Window: [#{}..#{})\nVisible Matrix Viewport: [#{}..#{})",
            st.total_chunks, st.window_base, st.window_base, window_end, vis_start, vis_end
        ));
    }

    fn render_pinned_chunk_inspector(&mut self, ui: &mut Ui, st: &TransferStats) {
        egui::Frame::none()
            .fill(Color32::from_rgb(18, 24, 36))
            .stroke(Stroke::new(1.0, COLOR_CARD_BORDER))
            .rounding(6.0)
            .inner_margin(8.0)
            .show(ui, |ui| {
                let Some(pinned_seq) = self.pinned_chunk_seq else {
                    ui.label(
                        RichText::new(
                            "📌 Chunk Inspector: Hover any cell for live tooltip, or click any chunk cell to pin it here for detailed byte-offset & timestamp inspection.",
                        )
                        .size(11.5)
                        .color(Color32::GRAY),
                    );
                    return;
                };

                // Look up in current visible chunks first, then fallback to cache, or synthesize if < base / >= next_seq
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
                        RichText::new(format!("📌 Pinned Chunk #{}", chunk.seq_num))
                            .strong()
                            .color(COLOR_PINNED_HIGHLIGHT)
                            .size(13.0),
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

                ui.add_space(2.0);

                ui.columns(3, |cols| {
                    cols[0].vertical(|ui| {
                        ui.label(
                            RichText::new(format!(
                                "State: {} ({})",
                                chunk.state.symbol(),
                                chunk.state.label()
                            ))
                            .strong()
                            .size(12.0),
                        );
                        ui.label(
                            RichText::new(format!(
                                "Byte Offset: {} .. {} ({} B)",
                                byte_offset, byte_end, byte_len
                            ))
                            .monospace()
                            .size(11.5),
                        );
                    });
                    cols[1].vertical(|ui| {
                        ui.label(
                            RichText::new(format!("ACK Details: {}", chunk.ack_kind.label()))
                                .size(12.0),
                        );
                        let retrans_text = match chunk.last_retransmit_reason {
                            Some(reason) => format!(
                                "Retransmissions: {} ({})",
                                chunk.retransmit_count,
                                reason.label()
                            ),
                            None => format!("Retransmissions: {}", chunk.retransmit_count),
                        };
                        ui.label(RichText::new(retrans_text).size(11.5).color(
                            if chunk.had_retransmission {
                                COLOR_WARNING
                            } else {
                                Color32::LIGHT_GRAY
                            },
                        ));
                    });
                    cols[2].vertical(|ui| {
                        let first_str = chunk
                            .first_event_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "Not observed".to_string());
                        let last_str = chunk
                            .last_sent_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "N/A".to_string());
                        let ack_str = chunk
                            .acked_elapsed
                            .map(|d| format!("+{:.1}ms", d.as_secs_f64() * 1000.0))
                            .unwrap_or_else(|| "Pending".to_string());
                        ui.label(
                            RichText::new(format!("First Event: {} | Last Tx: {}", first_str, last_str))
                                .monospace()
                                .size(11.0),
                        );
                        ui.label(
                            RichText::new(format!("Confirmed At: {}", ack_str))
                                .monospace()
                                .size(11.0),
                        );
                    });
                });
            });
    }

    fn render_timeline_panel(&mut self, ui: &mut Ui) {
        ui.horizontal_wrapped(|ui| {
            ui.label(RichText::new("Live Protocol Event Timeline & Trace").strong());
            ui.separator();
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::All,
                RichText::new("All").size(11.5),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::WindowAndSack,
                RichText::new("Window & SACK").size(11.5),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::RetransAndTimeout,
                RichText::new("Timeouts & Retrans").size(11.5),
            );
            ui.selectable_value(
                &mut self.timeline_filter,
                TimelineFilter::DataAndAck,
                RichText::new("Data & ACK").size(11.5),
            );

            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui.button("Clear Logs").clicked() {
                    self.timeline_events.clear();
                    self.logs.clear();
                }
            });
        });

        egui::Frame::none()
            .fill(Color32::from_rgb(14, 18, 26))
            .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
            .rounding(6.0_f32)
            .inner_margin(8.0_f32)
            .show(ui, |ui| {
                ScrollArea::vertical()
                    .id_salt("protocol_timeline_scroll")
                    .stick_to_bottom(!self.vis_paused)
                    .max_height(135.0)
                    .show(ui, |ui| {
                        if self.timeline_events.is_empty() && self.logs.is_empty() {
                            ui.label(
                                RichText::new("No protocol events recorded yet.")
                                    .color(Color32::DARK_GRAY),
                            );
                            return;
                        }

                        // Render structured protocol telemetry events filtered by user selection
                        for ev in &self.timeline_events {
                            let matches = match self.timeline_filter {
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
                            if !matches {
                                continue;
                            }

                            let color = match ev.kind {
                                ProtocolEventKind::TimeoutDetected
                                | ProtocolEventKind::Retransmission => COLOR_DANGER,
                                ProtocolEventKind::SackUpdate => COLOR_WARNING,
                                ProtocolEventKind::WindowAdvanced
                                | ProtocolEventKind::Completed
                                | ProtocolEventKind::Verification => COLOR_SUCCESS,
                                ProtocolEventKind::Handshake => COLOR_PRIMARY,
                                _ => Color32::from_rgb(195, 208, 225),
                            };

                            let line = format!(
                                "[+{:>7.3}s] [{:<9}] {}",
                                ev.elapsed.as_secs_f64(),
                                ev.kind.tag(),
                                ev.message
                            );
                            ui.label(RichText::new(line).color(color).monospace().size(11.5));
                        }

                        // Also show general application status logs when filter is All
                        if self.timeline_filter == TimelineFilter::All {
                            for log in &self.logs {
                                let color = if log.contains("ERROR") || log.contains("failed") {
                                    COLOR_DANGER
                                } else if log.contains("SUCCESS") || log.contains("verified") {
                                    COLOR_SUCCESS
                                } else {
                                    Color32::from_rgb(155, 168, 185)
                                };
                                ui.label(RichText::new(log).color(color).monospace().size(11.0));
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

    // If running on Sender and user inspects the Receiver SACK Buffer perspective:
    // Derive receiver buffer state strictly from peer-confirmed `window_base` (`ack_num`) and 64-bit `last_sack_bitmask`.
    if st.perspective == WindowPerspective::Sender
        && target_perspective == WindowPerspective::Receiver
    {
        let mut projected = chunk.clone();
        if chunk.seq_num < st.window_base {
            projected.state = ChunkState::Acked;
        } else if chunk.state == ChunkState::Sacked {
            projected.state = ChunkState::Sacked;
        } else {
            // Check if any higher chunk in the visible set is already SACKed (meaning receiver has seen past this chunk)
            let highest_sacked = st
                .visible_chunks
                .iter()
                .filter(|c| c.state == ChunkState::Sacked)
                .map(|c| c.seq_num)
                .max();
            if highest_sacked.is_some_and(|hi| chunk.seq_num < hi) {
                projected.state = ChunkState::InFlight; // Unreceived gap at receiver
            } else {
                projected.state = ChunkState::Pending; // Not yet confirmed at receiver
            }
        }
        return projected;
    }

    chunk.clone()
}

fn render_legend_badge(ui: &mut Ui, bg: Color32, border: Color32, text: &str, tooltip: &str) {
    let resp = egui::Frame::none()
        .fill(bg)
        .stroke(Stroke::new(1.2, border))
        .rounding(4.0)
        .inner_margin(egui::Margin::symmetric(6.0, 2.0))
        .show(ui, |ui| {
            ui.label(
                RichText::new(text)
                    .size(11.0)
                    .color(Color32::WHITE)
                    .strong(),
            );
        })
        .response;
    resp.on_hover_text(tooltip);
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

    painter.rect_filled(rect, Rounding::same(4.0), bg_color);

    // Shape cues (so states are distinguishable without relying solely on color):
    match chunk.state {
        ChunkState::Acked => {
            // Top horizontal confirmation bar
            let bar = Rect::from_min_max(rect.min, Pos2::new(rect.max.x, rect.min.y + 3.0));
            painter.rect_filled(bar, Rounding::same(2.0), COLOR_STATE_ACKED_BORDER);
        }
        ChunkState::Sacked => {
            // Left vertical accent bar indicating out-of-order SACK buffer
            let bar = Rect::from_min_max(rect.min, Pos2::new(rect.min.x + 3.5, rect.max.y));
            painter.rect_filled(bar, Rounding::same(2.0), COLOR_STATE_SACKED_BORDER);
        }
        ChunkState::Retransmitting => {
            // Bottom warning bar
            let bar = Rect::from_min_max(Pos2::new(rect.min.x, rect.max.y - 3.5), rect.max);
            painter.rect_filled(bar, Rounding::same(2.0), COLOR_STATE_RETRANS_BORDER);
        }
        _ => {}
    }

    // Persistent retransmission history cue: top-right dot when `had_retransmission` is true
    if chunk.had_retransmission {
        let center = Pos2::new(rect.max.x - 4.5, rect.min.y + 4.5);
        painter.circle_filled(center, 2.8, COLOR_WARNING);
        painter.circle_stroke(center, 2.8, Stroke::new(0.8, Color32::BLACK));
    }

    // Active sliding window highlight & pinned border
    let stroke = if is_pinned {
        Stroke::new(2.2, COLOR_PINNED_HIGHLIGHT)
    } else if is_base_chunk {
        Stroke::new(2.0, COLOR_WINDOW_HIGHLIGHT)
    } else if is_in_active_window {
        Stroke::new(1.5, COLOR_WINDOW_HIGHLIGHT)
    } else {
        Stroke::new(1.0, border_color)
    };
    painter.rect_stroke(rect, Rounding::same(4.0), stroke);

    // Text label combining non-color symbol + sequence number
    let short_seq = chunk.seq_num % 10000;
    let label = format!("{}{}", chunk.state.symbol(), short_seq);
    let text_color = if chunk.state == ChunkState::Pending {
        Color32::from_rgb(140, 152, 175)
    } else {
        Color32::WHITE
    };

    painter.text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        label,
        FontId::monospace(10.0),
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
        "Byte Range: {} .. {} ({} bytes)",
        byte_offset,
        byte_offset + byte_len,
        byte_len
    ));
    ui.label(format!(
        "In Active Window: {}",
        if is_in_active_window { "Yes" } else { "No" }
    ));
    ui.label(format!("Retransmission Count: {}", chunk.retransmit_count));
    if let Some(reason) = chunk.last_retransmit_reason {
        ui.label(
            RichText::new(format!("Retransmit Reason: {}", reason.label())).color(COLOR_WARNING),
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
            "Confirmed At: +{:.1} ms",
            ack_t.as_secs_f64() * 1000.0
        ));
    }
    ui.separator();
    ui.label(
        RichText::new("Click cell to pin/unpin in the Chunk Inspector.")
            .size(11.0)
            .color(Color32::GRAY),
    );
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
