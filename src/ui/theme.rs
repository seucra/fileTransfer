use eframe::egui::{self, Color32, Rounding, Stroke, Visuals};

pub fn configure_theme(ctx: &egui::Context) {
    let mut visuals = Visuals::dark();
    visuals.override_text_color = Some(Color32::from_rgb(230, 235, 245));
    visuals.panel_fill = Color32::from_rgb(18, 22, 32);
    visuals.window_fill = Color32::from_rgb(24, 29, 42);
    visuals.faint_bg_color = Color32::from_rgb(28, 35, 52);
    visuals.extreme_bg_color = Color32::from_rgb(12, 15, 22);

    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(25, 32, 48);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, Color32::from_rgb(45, 55, 80));
    visuals.widgets.noninteractive.rounding = Rounding::same(6.0_f32);

    visuals.widgets.inactive.bg_fill = Color32::from_rgb(32, 42, 64);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0_f32, Color32::from_rgb(55, 70, 100));
    visuals.widgets.inactive.rounding = Rounding::same(6.0_f32);

    visuals.widgets.hovered.bg_fill = Color32::from_rgb(44, 58, 88);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.5_f32, Color32::from_rgb(90, 120, 180));
    visuals.widgets.hovered.rounding = Rounding::same(6.0_f32);

    visuals.widgets.active.bg_fill = Color32::from_rgb(55, 75, 115);
    visuals.widgets.active.bg_stroke = Stroke::new(2.0_f32, Color32::from_rgb(110, 150, 230));
    visuals.widgets.active.rounding = Rounding::same(6.0_f32);

    ctx.set_visuals(visuals);
}

pub const COLOR_PRIMARY: Color32 = Color32::from_rgb(76, 140, 245);
pub const COLOR_SUCCESS: Color32 = Color32::from_rgb(72, 199, 116);
pub const COLOR_WARNING: Color32 = Color32::from_rgb(255, 184, 77);
pub const COLOR_DANGER: Color32 = Color32::from_rgb(245, 90, 90);
pub const COLOR_CARD_BG: Color32 = Color32::from_rgb(26, 33, 48);
pub const COLOR_CARD_BORDER: Color32 = Color32::from_rgb(45, 58, 85);
