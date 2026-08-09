mod edge_sync;

use std::time::Duration;

use futures_util::StreamExt;
use iced::time::Instant;
use iced::widget::{
    button, column, container, progress_bar, row, scrollable, space, text, text_input,
};
use iced::{Animation, Element, Fill, Size, Subscription, Task, Theme, window};

use edge_sync::{DownloadEvent, DownloadedSnapshot, SnapshotRequest};

pub fn main() -> iced::Result {
    // In iced 0.14 the first argument to `application()` is the *boot*
    // function (State, or (State, Task<Message>)), not the title — title is
    // set via `.title(...)` below.
    iced::application(App::new, App::update, App::view)
        .title("Qdrant Edge Snapshotter")
        .theme(App::theme)
        .subscription(App::subscription)
        .window(window::Settings {
            size: Size::new(640.0, 760.0),
            ..Default::default()
        })
        .run()
}

struct App {
    server_url: String,
    api_key: String,
    collection: String,
    shard_id: String,
    target_dir: String,

    log: Vec<String>,
    busy: bool,

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
            shard_id: "0".to_string(),
            target_dir: "./qdrant-edge-directory".to_string(),
            log: vec!["Ready.".to_string()],
            busy: false,
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
    ShardIdChanged(String),
    TargetDirChanged(String),

    PickFolder,
    FolderPicked(Option<String>),

    Sync,
    DownloadProgress { downloaded: u64, total: Option<u64> },
    Downloaded(Result<DownloadedSnapshot, String>),
    Unpacked(Result<String, String>),
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
            Message::ShardIdChanged(v) => {
                self.shard_id = v;
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
                self.downloaded_bytes = 0;
                self.total_bytes = None;
                self.progress = Animation::new(0.0).duration(Duration::from_millis(250));
                self.push_log(format!(
                    "Downloading snapshot of '{}' shard {} from {}…",
                    self.collection, self.shard_id, self.server_url
                ));

                let req = SnapshotRequest {
                    server_url: self.server_url.clone(),
                    api_key: self.api_key.clone(),
                    collection: self.collection.clone(),
                    shard_id: self.shard_id.clone(),
                    target_dir: self.target_dir.clone(),
                };

                Task::stream(edge_sync::download_snapshot_stream(req).map(|event| match event {
                    DownloadEvent::Progress { downloaded, total } => {
                        Message::DownloadProgress { downloaded, total }
                    }
                    DownloadEvent::Done(result) => Message::Downloaded(result),
                }))
            }

            Message::DownloadProgress { downloaded, total } => {
                self.downloaded_bytes = downloaded;
                self.total_bytes = total;

                // Download fills 0–90%; the last 10% is reserved for the
                // unpack step, which has no granular progress signal.
                let target = match total {
                    Some(total) if total > 0 => {
                        (downloaded as f32 / total as f32 * 90.0).min(90.0)
                    }
                    _ => {
                        // No Content-Length: creep forward instead of
                        // claiming a precision we don't have.
                        let current = self.progress.interpolate_with(|v| v, Instant::now());
                        (current + 2.0).min(85.0)
                    }
                };
                self.progress = self.progress.clone().go(target, Instant::now());
                Task::none()
            }

            Message::Downloaded(Ok(snap)) => {
                self.progress = self.progress.clone().go(92.0, Instant::now());
                self.push_log(format!(
                    "Downloaded {:.2} MB to {}. Unpacking into Edge Shard…",
                    snap.bytes as f64 / (1024.0 * 1024.0),
                    snap.path.display()
                ));

                Task::perform(
                    edge_sync::unpack_snapshot_to_edge(snap.path, self.target_dir.clone()),
                    Message::Unpacked,
                )
            }
            Message::Downloaded(Err(err)) => {
                self.busy = false;
                self.progress = self.progress.clone().go(0.0, Instant::now());
                self.push_log(format!("✗ Download failed: {err}"));
                Task::none()
            }

            Message::Unpacked(Ok(summary)) => {
                self.busy = false;
                self.progress = self.progress.clone().go(100.0, Instant::now());
                self.push_log(format!("✓ {summary}"));
                Task::none()
            }
            Message::Unpacked(Err(err)) => {
                self.busy = false;
                self.progress = self.progress.clone().go(0.0, Instant::now());
                self.push_log(format!("✗ Unpack failed: {err}"));
                Task::none()
            }

            Message::Tick(_now) => Task::none(),
        }
    }

    fn view(&self) -> Element<Message> {
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
            button(text("Download snapshot & sync to Edge"))
                .padding([10, 20])
                .on_press(Message::Sync)
        };

        let progress_value = self.progress.interpolate_with(|v| v, Instant::now());
        let progress_label = match self.total_bytes {
            Some(total) if total > 0 => format!(
                "{:.0}% · {:.1} MB / {:.1} MB",
                progress_value,
                self.downloaded_bytes as f64 / (1024.0 * 1024.0),
                total as f64 / (1024.0 * 1024.0),
            ),
            _ if self.busy => format!(
                "{:.1} MB downloaded",
                self.downloaded_bytes as f64 / (1024.0 * 1024.0)
            ),
            _ => String::new(),
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
                "Pull a shard snapshot from a Qdrant server and unpack it into a local Edge Shard."
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
            row![
                field(
                    "Collection",
                    &self.collection,
                    "my-collection",
                    Message::CollectionChanged
                )
                .width(Fill),
                field("Shard ID", &self.shard_id, "0", Message::ShardIdChanged).width(120),
            ]
            .spacing(12),
            column![text("Local Edge Shard directory").size(13), target_dir_row,].spacing(4),
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
        .set_title("Choose (or create) the local Edge Shard directory")
        .pick_folder()
        .await
        .map(|handle| handle.path().to_string_lossy().to_string())
}