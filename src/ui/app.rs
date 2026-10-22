use crate::network::receiver::{run_receiver, ReceiverConfig};
use crate::network::sender::{run_sender, SenderConfig, TransferEvent};
use crate::protocol::stats::TransferStats;
use crate::ui::theme::{configure_theme, COLOR_CARD_BG, COLOR_CARD_BORDER, COLOR_DANGER, COLOR_PRIMARY, COLOR_SUCCESS, COLOR_WARNING};
use crossbeam_channel::{unbounded, Receiver as ChannelReceiver, Sender as ChannelSender};
use eframe::egui::{self, Align, Button, Color32, Layout, ProgressBar, RichText, ScrollArea, Stroke, Ui};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

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

    // Live Metrics & Logs
    stats: Option<TransferStats>,
    logs: Vec<String>,
}

impl Default for TransferApp {
    fn default() -> Self {
        let detected_ip = local_ip_address::local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let default_dest = std::env::current_dir()
            .map(|p| p.join("downloads").display().to_string())
            .unwrap_or_else(|_| "./downloads".to_string());

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
            logs: vec!["Application initialized. Select Sender or Receiver mode.".to_string()],
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
        self.logs.push(format!("[{}] {}", timestamp, msg));
        if self.logs.len() > 500 {
            self.logs.remove(0);
        }
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

        let (tx, rx): (ChannelSender<TransferEvent>, ChannelReceiver<TransferEvent>) = unbounded();
        self.event_rx = Some(rx);

        let config = SenderConfig {
            file_path,
            target_addr,
            bind_addr,
            chunk_size: self.chunk_size,
            window_size: self.window_size,
            simulated_loss_percent: self.simulated_loss_percent,
        };

        self.state = TransferState::Active;
        self.status_message = format!("Connecting to receiver at {}...", target_addr);
        self.stats = None;
        self.add_log(format!(
            "Starting Sender -> Target: {}, Window: {}, Chunks Size: {}B, Loss: {}%",
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

        let (tx, rx): (ChannelSender<TransferEvent>, ChannelReceiver<TransferEvent>) = unbounded();
        self.event_rx = Some(rx);

        let config = ReceiverConfig {
            bind_addr,
            destination_dir: dest_dir,
            simulated_loss_percent: self.simulated_loss_percent,
        };

        self.state = TransferState::Active;
        self.status_message = format!("Listening for incoming files on UDP port {}...", self.port);
        self.stats = None;
        self.add_log(format!(
            "Starting Receiver on UDP port {}, Loss: {}%",
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
                    self.stats = Some(st);
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
                ui.heading(RichText::new("Reliable UDP File Transfer").color(COLOR_PRIMARY).size(22.0).strong());
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.button("Refresh IP").clicked() {
                        self.refresh_local_ip();
                    }
                    ui.label(RichText::new(format!("Local IP: {}", self.local_ip)).color(Color32::LIGHT_GRAY));
                });
            });

            ui.add_space(8.0);

            // Mode Selector Tabs (Sender / Receiver)
            ui.horizontal(|ui| {
                let is_active = self.state == TransferState::Active;
                ui.add_enabled_ui(!is_active, |ui| {
                    ui.selectable_value(&mut self.mode, AppMode::Sender, RichText::new("  Sender Mode  ").size(15.0).strong());
                    ui.selectable_value(&mut self.mode, AppMode::Receiver, RichText::new("  Receiver Mode  ").size(15.0).strong());
                });

                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    match self.state {
                        TransferState::Idle => ui.label(RichText::new("IDLE").color(Color32::GRAY)),
                        TransferState::Active => ui.label(RichText::new("RUNNING").color(COLOR_PRIMARY).strong()),
                        TransferState::Completed => ui.label(RichText::new("COMPLETED").color(COLOR_SUCCESS).strong()),
                        TransferState::Failed => ui.label(RichText::new("FAILED").color(COLOR_DANGER).strong()),
                        TransferState::Cancelled => ui.label(RichText::new("CANCELLED").color(COLOR_WARNING)),
                    };
                });
            });

            ui.separator();
            ui.add_space(4.0);

            // Main Configuration Card
            egui::Frame::none()
                .fill(COLOR_CARD_BG)
                .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                .rounding(8.0_f32)
                .inner_margin(12.0_f32)
                .show(ui, |ui| {
                    match self.mode {
                        AppMode::Sender => self.render_sender_config(ui),
                        AppMode::Receiver => self.render_receiver_config(ui),
                    }
                });

            ui.add_space(8.0);

            // Live Transfer Metrics Card
            egui::Frame::none()
                .fill(COLOR_CARD_BG)
                .stroke(Stroke::new(1.0_f32, COLOR_CARD_BORDER))
                .rounding(8.0_f32)
                .inner_margin(12.0_f32)
                .show(ui, |ui| {
                    self.render_metrics_panel(ui);
                });

            ui.add_space(8.0);

            // Logs / Status Panel
            ui.horizontal(|ui| {
                ui.label(RichText::new("Event Log & Protocol Trace").strong());
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.button("Clear Logs").clicked() {
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
                        .stick_to_bottom(true)
                        .max_height(140.0)
                        .show(ui, |ui| {
                            if self.logs.is_empty() {
                                ui.label(RichText::new("No log events yet.").color(Color32::DARK_GRAY));
                            } else {
                                for log in &self.logs {
                                    if log.contains("ERROR") || log.contains("failed") {
                                        ui.label(RichText::new(log).color(COLOR_DANGER).monospace());
                                    } else if log.contains("SUCCESS") || log.contains("verified") {
                                        ui.label(RichText::new(log).color(COLOR_SUCCESS).monospace());
                                    } else if log.contains("retrans") || log.contains("Corrupt") {
                                        ui.label(RichText::new(log).color(COLOR_WARNING).monospace());
                                    } else {
                                        ui.label(RichText::new(log).color(Color32::from_rgb(200, 210, 225)).monospace());
                                    }
                                }
                            }
                        });
                });
        });
    }
}

impl TransferApp {
    fn render_sender_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label("File to Send:");
            ui.add_enabled(!is_active, egui::TextEdit::singleline(&mut self.selected_file_path).desired_width(340.0));
            if ui.add_enabled(!is_active, Button::new("Browse...")).clicked() {
                if let Some(file) = rfd::FileDialog::new().pick_file() {
                    self.selected_file_path = file.display().to_string();
                }
            }
        });

        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Receiver IP:");
            ui.add_enabled(!is_active, egui::TextEdit::singleline(&mut self.target_ip).desired_width(140.0));

            ui.label("Port:");
            let mut port_str = self.port.to_string();
            if ui.add_enabled(!is_active, egui::TextEdit::singleline(&mut port_str).desired_width(60.0)).changed() {
                if let Ok(p) = port_str.parse() {
                    self.port = p;
                }
            }

            ui.label("Window:");
            ui.add_enabled(!is_active, egui::Slider::new(&mut self.window_size, 1..=64).text("packets"));
        });

        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Simulated Loss:");
            ui.add_enabled(!is_active, egui::Slider::new(&mut self.simulated_loss_percent, 0..=50).text("% packet drop"));
        });

        ui.add_space(8.0);

        // Control buttons
        ui.horizontal(|ui| {
            if !is_active {
                if ui.button(RichText::new("  Start Transfer  ").color(Color32::WHITE).strong()).clicked() {
                    self.start_sender();
                }
            } else {
                if ui.button(RichText::new("  Cancel Transfer  ").color(COLOR_DANGER).strong()).clicked() {
                    self.cancel_transfer();
                }
            }

            if self.state == TransferState::Failed || self.state == TransferState::Completed {
                if ui.button("Retry").clicked() {
                    self.start_sender();
                }
            }

            ui.label(RichText::new(&self.status_message).color(Color32::LIGHT_GRAY));
        });
    }

    fn render_receiver_config(&mut self, ui: &mut Ui) {
        let is_active = self.state == TransferState::Active;

        ui.horizontal(|ui| {
            ui.label("Save Directory:");
            ui.add_enabled(!is_active, egui::TextEdit::singleline(&mut self.destination_dir).desired_width(340.0));
            if ui.add_enabled(!is_active, Button::new("Browse...")).clicked() {
                if let Some(folder) = rfd::FileDialog::new().pick_folder() {
                    self.destination_dir = folder.display().to_string();
                }
            }
        });

        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Listening Port:");
            let mut port_str = self.port.to_string();
            if ui.add_enabled(!is_active, egui::TextEdit::singleline(&mut port_str).desired_width(60.0)).changed() {
                if let Ok(p) = port_str.parse() {
                    self.port = p;
                }
            }

            ui.label("Simulated Loss:");
            ui.add_enabled(!is_active, egui::Slider::new(&mut self.simulated_loss_percent, 0..=50).text("% packet drop"));
        });

        ui.add_space(8.0);

        // Control buttons
        ui.horizontal(|ui| {
            if !is_active {
                if ui.button(RichText::new("  Start Listening  ").color(Color32::WHITE).strong()).clicked() {
                    self.start_receiver();
                }
            } else {
                if ui.button(RichText::new("  Stop Listening  ").color(COLOR_DANGER).strong()).clicked() {
                    self.cancel_transfer();
                }
            }

            ui.label(RichText::new(&self.status_message).color(Color32::LIGHT_GRAY));
        });
    }

    fn render_metrics_panel(&self, ui: &mut Ui) {
        let (progress, speed, bytes_str, chunks_str, retrans, drops, rtt_str, eta_str) = match &self.stats {
            Some(st) => (
                st.progress_ratio(),
                st.formatted_speed(),
                st.formatted_bytes(),
                format!("{}/{}", st.chunks_transferred, st.total_chunks),
                st.retransmissions.to_string(),
                (st.corrupt_packets_dropped + st.duplicate_packets_dropped).to_string(),
                st.current_rtt.map(|r| format!("{:.1}ms", r.as_secs_f64() * 1000.0)).unwrap_or_else(|| "--".to_string()),
                st.estimated_remaining().map(|e| format!("{:.0}s", e.as_secs_f64())).unwrap_or_else(|| "--".to_string()),
            ),
            None => (0.0, "0 B/s".to_string(), "0 B / 0 B".to_string(), "0/0".to_string(), "0".to_string(), "0".to_string(), "--".to_string(), "--".to_string()),
        };

        ui.add(ProgressBar::new(progress).show_percentage().animate(self.state == TransferState::Active));

        ui.add_space(6.0);

        ui.columns(4, |cols| {
            cols[0].vertical(|ui| {
                ui.label(RichText::new("Speed").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(speed).size(15.0).strong().color(COLOR_PRIMARY));
            });
            cols[1].vertical(|ui| {
                ui.label(RichText::new("Transferred").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(bytes_str).size(14.0).strong());
            });
            cols[2].vertical(|ui| {
                ui.label(RichText::new("Chunks (Retrans / Drops)").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(format!("{} ({} / {})", chunks_str, retrans, drops)).size(14.0).strong());
            });
            cols[3].vertical(|ui| {
                ui.label(RichText::new("RTT / ETA").size(11.0).color(Color32::GRAY));
                ui.label(RichText::new(format!("{} / {}", rtt_str, eta_str)).size(14.0).strong());
            });
        });
    }
}

fn chrono_free_timestamp() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}
