use eframe::egui::{self, Color32, Rounding, Stroke, Visuals};

pub fn configure_theme(ctx: &egui::Context) {
    let mut visuals = Visuals::dark();
    visuals.override_text_color = Some(COLOR_TEXT_PRIMARY);
    visuals.panel_fill = COLOR_PANEL_BG;
    visuals.window_fill = COLOR_WINDOW_BG;
    visuals.faint_bg_color = Color32::from_rgb(24, 28, 37);
    visuals.extreme_bg_color = Color32::from_rgb(13, 16, 21);

    // Standardized modest rounding for an authentic engineering desktop tool
    let radius = Rounding::same(3.0_f32);

    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(20, 24, 32);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, Color32::from_rgb(38, 45, 60));
    visuals.widgets.noninteractive.rounding = radius;

    visuals.widgets.inactive.bg_fill = Color32::from_rgb(26, 31, 42);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0_f32, Color32::from_rgb(46, 55, 74));
    visuals.widgets.inactive.rounding = radius;

    visuals.widgets.hovered.bg_fill = Color32::from_rgb(36, 44, 60);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0_f32, Color32::from_rgb(65, 82, 114));
    visuals.widgets.hovered.rounding = radius;

    visuals.widgets.active.bg_fill = Color32::from_rgb(44, 56, 80);
    visuals.widgets.active.bg_stroke = Stroke::new(1.5_f32, COLOR_PRIMARY);
    visuals.widgets.active.rounding = radius;

    visuals.selection.bg_fill = Color32::from_rgb(38, 70, 128);
    visuals.selection.stroke = Stroke::new(1.0_f32, COLOR_PRIMARY);

    ctx.set_visuals(visuals);
}

// Typography & Base Layout Colors
pub const COLOR_TEXT_PRIMARY: Color32 = Color32::from_rgb(226, 232, 242);
pub const COLOR_TEXT_SECONDARY: Color32 = Color32::from_rgb(155, 168, 188);
pub const COLOR_TEXT_MUTED: Color32 = Color32::from_rgb(105, 118, 138);

pub const COLOR_PANEL_BG: Color32 = Color32::from_rgb(16, 19, 25);
pub const COLOR_WINDOW_BG: Color32 = Color32::from_rgb(20, 24, 32);
pub const COLOR_CARD_BG: Color32 = Color32::from_rgb(22, 26, 35);
pub const COLOR_CARD_BORDER: Color32 = Color32::from_rgb(38, 46, 62);
pub const COLOR_SEPARATOR: Color32 = Color32::from_rgb(34, 40, 54);

// Functional Action & Status Colors
pub const COLOR_PRIMARY: Color32 = Color32::from_rgb(58, 118, 204);
pub const COLOR_PRIMARY_HOVER: Color32 = Color32::from_rgb(72, 136, 228);
pub const COLOR_SUCCESS: Color32 = Color32::from_rgb(48, 164, 86);
pub const COLOR_WARNING: Color32 = Color32::from_rgb(212, 142, 38);
pub const COLOR_DANGER: Color32 = Color32::from_rgb(218, 68, 68);
pub const COLOR_INFO: Color32 = Color32::from_rgb(74, 146, 214);

// Sliding Window & SACK Packet Matrix State Colors
pub const COLOR_STATE_PENDING_BG: Color32 = Color32::from_rgb(24, 28, 38);
pub const COLOR_STATE_PENDING_BORDER: Color32 = Color32::from_rgb(42, 50, 68);

pub const COLOR_STATE_INFLIGHT_BG: Color32 = Color32::from_rgb(24, 52, 90);
pub const COLOR_STATE_INFLIGHT_BORDER: Color32 = Color32::from_rgb(56, 114, 192);

pub const COLOR_STATE_SACKED_BG: Color32 = Color32::from_rgb(84, 58, 16);
pub const COLOR_STATE_SACKED_BORDER: Color32 = Color32::from_rgb(196, 138, 34);

pub const COLOR_STATE_RETRANS_BG: Color32 = Color32::from_rgb(96, 28, 32);
pub const COLOR_STATE_RETRANS_BORDER: Color32 = Color32::from_rgb(212, 66, 66);

pub const COLOR_STATE_ACKED_BG: Color32 = Color32::from_rgb(18, 66, 38);
pub const COLOR_STATE_ACKED_BORDER: Color32 = Color32::from_rgb(46, 158, 82);

pub const COLOR_WINDOW_HIGHLIGHT: Color32 = Color32::from_rgb(78, 172, 232);
pub const COLOR_PINNED_HIGHLIGHT: Color32 = Color32::from_rgb(250, 252, 255);
