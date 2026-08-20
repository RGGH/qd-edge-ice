mod edge_sync;

use std::time::Duration;

use futures_util::StreamExt;
use iced::time::Instant;
use iced::widget::{
    button, column, container, progress_bar, row, scrollable, space, text, text_input,
};
use iced::{Animation, Element, Fill, Font, Size, Subscription, Task, Theme, window};

use edge_sync::{CollectionSyncRequest, SyncEvent};

const MONA_SANS: &[u8] = include_bytes!("assets/fonts/MonaSans-Regular.ttf");
const MONA_SANS_FONT: Font = Font::with_name("Mona Sans");

pub fn main() -> iced::Result {
    // In iced 0.14 the first argument to `application()` is the *boot*
    // function (State, or (State, Task<Message>)), not the title — title is
    // set via `.title(...)` below.
    iced::application(App::new, App::update, App::view)
        .title("Qdrant Edge Sync")
        .theme(App::theme)
        .subscription(App::subscription)
        .font(MONA_SANS) // registers the bytes at boot
        .default_font(MONA_SANS_FONT) // makes it the default for every text widget
        .window(window::Settings {
            size: Size::new(640.0, 760.0),
            icon: window_icon(),
            ..Default::default()
        })
        .run()
}

/// Loads `src/assets/qdrant.png`, embedded at compile time, and decodes it
/// into a window icon. Requires iced's `image` feature (see Cargo.toml) to
/// decode PNG bytes — without it only raw RGBA icons are supported.
///
/// Note this sets the *window* icon (titlebar/taskbar/dock while running).
/// A packaged app icon (Windows .exe icon, macOS .app bundle icon) is a
/// separate, platform-specific packaging step — this alone won't cover that.
fn window_icon() -> Option<window::icon::Icon> {
    let bytes = include_bytes!("assets/qdrant.png");
    match window::icon::from_file_data(bytes, None) {
        Ok(icon) => Some(icon),
        Err(err) => {
            eprintln!("Could not load window icon from assets/qdrant.png: {err}");
            None
        }
    }
}

struct App {
    server_url: String,
    api_key: String,
    collection: String,
    target_dir: String,

    log: Vec<String>,
    busy: bool,

    // All shards discovered for the collection, and where we are in them.
    shards: Vec<u32>,
    total_shards: usize,
    current_shard: Option<u32>,
    current_shard_index: usize,

    progress: Animation<f32>,
    downloaded_bytes: u64,
    total_bytes: Option<u64>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:6333".to_string(),
            api_key: String::new(),
            collection: String::new(),
            target_dir: "./qdrant-edge-directory".to_string(),
            log: vec!["Ready.".to_string()],
            busy: false,
            shards: Vec::new(),
            total_shards: 0,
            current_shard: None,
            current_shard_index: 0,
            progress: Animation::new(0.0).duration(Duration::from_millis(250)),
            downloaded_bytes: 0,
            total_bytes: None,
        }
    }
}

#[derive(Debug, Clone)]
enum Message {
    ServerUrlChanged(String),
    ApiKeyChanged(String),
    CollectionChanged(String),
    TargetDirChanged(String),

    PickFolder,
    FolderPicked(Option<String>),

    Sync,
    ShardsDiscovered(Vec<u32>),
    ShardStarted {
        shard_id: u32,
        index: usize,
        total_shards: usize,
    },
    ShardProgress {
        shard_id: u32,
        downloaded: u64,
        total: Option<u64>,
    },
    ShardCompleted {
        shard_id: u32,
        summary: String,
    },
    ShardFailed {
        shard_id: u32,
        error: String,
    },
    SyncDone(Result<(), String>),
    Tick(Instant),
}

impl App {
    fn new() -> (Self, Task<Message>) {
        (Self::default(), Task::none())
    }

    fn theme(&self) -> Theme {
        Theme::CatppuccinLatte
    }

    fn push_log(&mut self, line: impl Into<String>) {
        self.log.push(line.into());
    }

    fn subscription(&self) -> Subscription<Message> {
        if self.progress.is_animating(Instant::now()) {
            iced::window::frames().map(Message::Tick)
        } else {
            Subscription::none()
        }
    }

    /// Overall progress (0..100) across every shard. Each shard owns an
    /// equal-width slice of the bar; `shard_fraction` is how far (0..100)
    /// we are through the *current* shard's slice.
    fn overall_progress(&self, shard_fraction: f32) -> f32 {
        if self.total_shards == 0 {
            return 0.0;
        }
        let completed = self.current_shard_index as f32;
        let total = self.total_shards as f32;
        ((completed + shard_fraction / 100.0) / total * 100.0).clamp(0.0, 100.0)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::ServerUrlChanged(v) => {
                self.server_url = v;
                Task::none()
            }
            Message::ApiKeyChanged(v) => {
                self.api_key = v;
                Task::none()
            }
            Message::CollectionChanged(v) => {
                self.collection = v;
                Task::none()
            }
            Message::TargetDirChanged(v) => {
                self.target_dir = v;
                Task::none()
            }

            Message::PickFolder => Task::perform(pick_folder(), Message::FolderPicked),
            Message::FolderPicked(Some(dir)) => {
                self.target_dir = dir;
                Task::none()
            }
            Message::FolderPicked(None) => Task::none(),

            Message::Sync => {
                if self.collection.trim().is_empty() {
                    self.push_log("⚠ Collection name is required.");
                    return Task::none();
                }
                if self.target_dir.trim().is_empty() {
                    self.push_log("⚠ Local Edge Shard directory is required.");
                    return Task::none();
                }

                self.busy = true;
                self.shards.clear();
                self.total_shards = 0;
                self.current_shard = None;
                self.current_shard_index = 0;
                self.downloaded_bytes = 0;
                self.total_bytes = None;
                self.progress = Animation::new(0.0).duration(Duration::from_millis(250));
                self.push_log(format!(
                    "Discovering shards for '{}' on {}…",
                    self.collection, self.server_url
                ));

                let req = CollectionSyncRequest {
                    server_url: self.server_url.clone(),
                    api_key: self.api_key.clone(),
                    collection: self.collection.clone(),
                    target_dir: self.target_dir.clone(),
                };

                Task::stream(
                    edge_sync::sync_all_shards_stream(req).map(|event| match event {
                        SyncEvent::ShardsDiscovered(ids) => Message::ShardsDiscovered(ids),
                        SyncEvent::ShardStarted {
                            shard_id,
                            index,
                            total_shards,
                        } => Message::ShardStarted {
                            shard_id,
                            index,
                            total_shards,
                        },
                        SyncEvent::Progress {
                            shard_id,
                            downloaded,
                            total,
                        } => Message::ShardProgress {
                            shard_id,
                            downloaded,
                            total,
                        },
                        SyncEvent::ShardCompleted { shard_id, summary } => {
                            Message::ShardCompleted { shard_id, summary }
                        }
                        SyncEvent::ShardFailed { shard_id, error } => {
                            Message::ShardFailed { shard_id, error }
                        }
                        SyncEvent::Done(result) => Message::SyncDone(result),
                    }),
                )
            }

            Message::ShardsDiscovered(ids) => {
                self.total_shards = ids.len();
                self.push_log(format!(
                    "Found {} shard(s): {}",
                    ids.len(),
                    ids.iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                self.shards = ids;
                Task::none()
            }

            Message::ShardStarted {
                shard_id,
                index,
                total_shards,
            } => {
                self.current_shard = Some(shard_id);
                self.current_shard_index = index;
                self.total_shards = total_shards;
                self.downloaded_bytes = 0;
                self.total_bytes = None;
                self.push_log(format!(
                    "[{}/{}] Downloading snapshot of shard {shard_id}…",
                    index + 1,
                    total_shards
                ));
                Task::none()
            }

            Message::ShardProgress {
                shard_id,
                downloaded,
                total,
            } => {
                if self.current_shard != Some(shard_id) {
                    return Task::none();
                }
                self.downloaded_bytes = downloaded;
                self.total_bytes = total;

                // Download fills 0–90% of this shard's slice; the last 10%
                // is reserved for the unpack step, which has no granular
                // progress signal.
                let shard_fraction = match total {
                    Some(total) if total > 0 => (downloaded as f32 / total as f32 * 90.0).min(90.0),
                    _ => {
                        // No Content-Length: creep forward instead of
                        // claiming a precision we don't have.
                        let current_overall = self.progress.interpolate_with(|v| v, Instant::now());
                        let current_shard_fraction = if self.total_shards > 0 {
                            (current_overall / 100.0 * self.total_shards as f32
                                - self.current_shard_index as f32)
                                * 100.0
                        } else {
                            0.0
                        };
                        (current_shard_fraction + 2.0).min(85.0)
                    }
                };

                let target = self.overall_progress(shard_fraction);
                self.progress = self.progress.clone().go(target, Instant::now());
                Task::none()
            }

            Message::ShardCompleted { shard_id, summary } => {
                self.push_log(format!("✓ Shard {shard_id}: {summary}"));
                let target = self.overall_progress(100.0);
                self.progress = self.progress.clone().go(target, Instant::now());
                Task::none()
            }

            Message::ShardFailed { shard_id, error } => {
                self.push_log(format!("✗ Shard {shard_id} failed: {error}"));
                // Still advance the bar — this shard's slice is "done"
                // (unsuccessfully) and we move on to the next one.
                let target = self.overall_progress(100.0);
                self.progress = self.progress.clone().go(target, Instant::now());
                Task::none()
            }

            Message::SyncDone(Ok(())) => {
                self.busy = false;
                self.current_shard = None;
                self.progress = self.progress.clone().go(100.0, Instant::now());
                self.push_log(format!(
                    "✓ All {} shard(s) synced to {}",
                    self.total_shards, self.target_dir
                ));
                Task::none()
            }
            Message::SyncDone(Err(err)) => {
                self.busy = false;
                self.current_shard = None;
                self.push_log(format!("✗ Sync finished with errors: {err}"));
                Task::none()
            }

            Message::Tick(_now) => Task::none(),
        }
    }

    fn view(&self) -> Element<'_, Message> {
        let field =
            |label: &str, value: &str, placeholder: &str, on_change: fn(String) -> Message| {
                column![
                    text(label.to_string()).size(13),
                    text_input(placeholder, value)
                        .on_input(on_change)
                        .padding(8),
                ]
                .spacing(4)
            };

        let target_dir_row = row![
            text_input("./qdrant-edge-directory", &self.target_dir)
                .on_input(Message::TargetDirChanged)
                .padding(8)
                .width(Fill),
            button(text("Browse…")).on_press(Message::PickFolder),
        ]
        .spacing(8)
        .align_y(iced::Alignment::Center);

        let sync_button = if self.busy {
            button(text("Working…")).padding([10, 20])
        } else {
            button(text("Sync all shards to Edge"))
                .padding([10, 20])
                .on_press(Message::Sync)
        };

        let progress_value = self.progress.interpolate_with(|v| v, Instant::now());
        let progress_label = if self.total_shards > 0 {
            let shard_part = match (self.current_shard, self.total_bytes) {
                (Some(shard_id), Some(total)) if total > 0 => format!(
                    "shard {shard_id} ({}/{}) · {:.1} MB / {:.1} MB",
                    self.current_shard_index + 1,
                    self.total_shards,
                    self.downloaded_bytes as f64 / (1024.0 * 1024.0),
                    total as f64 / (1024.0 * 1024.0),
                ),
                (Some(shard_id), _) => format!(
                    "shard {shard_id} ({}/{}) · {:.1} MB downloaded",
                    self.current_shard_index + 1,
                    self.total_shards,
                    self.downloaded_bytes as f64 / (1024.0 * 1024.0),
                ),
                (None, _) if self.busy => "finishing up…".to_string(),
                (None, _) => String::new(),
            };
            format!("{:.0}% · {shard_part}", progress_value)
        } else {
            String::new()
        };

        let progress_section = column![
            progress_bar(0.0..=100.0, progress_value),
            text(progress_label).size(12),
        ]
        .spacing(4);

        let log_view = scrollable(
            column(
                self.log
                    .iter()
                    .map(|line| text(line.clone()).size(13).into())
                    .collect::<Vec<Element<Message>>>(),
            )
            .spacing(4)
            .padding(10),
        )
        .height(Fill);

        let content = column![
            text("Qdrant Server → Qdrant Edge").size(22),
            text(
                "Pulls a snapshot of every shard in a collection and unpacks each one into its \
                 own local Edge Shard, so the whole collection stays in sync by default."
            )
            .size(13),
            space().height(8),
            field(
                "Server URL",
                &self.server_url,
                "http://localhost:6333",
                Message::ServerUrlChanged
            ),
            field(
                "API key (optional)",
                &self.api_key,
                "",
                Message::ApiKeyChanged
            ),
            field(
                "Collection",
                &self.collection,
                "my-collection",
                Message::CollectionChanged
            ),
            column![
                text("Local Edge Shard directory (one subfolder per shard)").size(13),
                target_dir_row,
            ]
            .spacing(4),
            space().height(4),
            sync_button,
            progress_section,
            space().height(4),
            text("Log").size(13),
            container(log_view).height(Fill).width(Fill),
        ]
        .spacing(12)
        .padding(20);

        container(content).width(Fill).height(Fill).into()
    }
}

async fn pick_folder() -> Option<String> {
    rfd::AsyncFileDialog::new()
        .set_title("Choose (or create) the parent directory for Edge Shards")
        .pick_folder()
        .await
        .map(|handle| handle.path().to_string_lossy().to_string())
}
