use std::{
    io::SeekFrom,
    os::unix::{fs::MetadataExt, process::ExitStatusExt},
    process::{ExitStatus, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt},
    process::Command,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{timeout, timeout_at, Instant},
};

use crate::{
    media::{
        processors::{failed_variant_marker, remove_if_exists, MediaProcessorError},
        FileVariant,
    },
    models::file::FileDetails,
};

use super::{BaseProcessingOptions, VariantProcessor};

const SMALL_IMAGE_WIDTH: &str = "320";
const FEED_IMAGE_WIDTH: &str = "720";
const IMAGE_FORMAT: &str = "webp";

/// Largest stored original the processor decodes. Clients downscale uploads to a few MB, so
/// anything larger is not a legitimate image upload.
pub const MAX_IMAGE_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// How long a request waits for a free conversion slot before it is answered as busy. Together
/// with [`IMAGE_CONVERSION_TIMEOUT`] this stays under a typical 60 second proxy timeout.
const SLOT_WAIT_TIMEOUT: Duration = Duration::from_secs(20);

/// Wall-clock budget for one conversion, counted from when it holds a conversion slot.
pub const IMAGE_CONVERSION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a variant that could not be built is left alone before it is tried again.
const FAILED_VARIANT_RETRY_AFTER: Duration = Duration::from_secs(60 * 60);

/// ImageMagick error output kept per run.
const MAX_CAPTURED_STDERR_BYTES: u64 = 4 * 1024;

/// ImageMagick processes that may run at once across the whole process.
const MAX_CONCURRENT_CONVERSIONS: usize = 2;

/// Resource limits passed to every ImageMagick run. They apply even where the host has no
/// restrictive `policy.xml`; the image's policy can only tighten them further.
const IMAGEMAGICK_LIMITS: &[(&str, &str)] = &[
    ("memory", "256MiB"),
    ("map", "512MiB"),
    ("disk", "1GiB"),
    ("width", "16KP"),
    ("height", "16KP"),
    ("area", "128MP"),
    ("list-length", "256"),
    ("time", "30"),
    ("thread", "2"),
];

static CONVERSION_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CONCURRENT_CONVERSIONS)));

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Raster formats the processor decodes. The decoder is chosen from the file's leading bytes and
/// passed to ImageMagick explicitly, so it never picks a decoder by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageInputFormat {
    Jpeg,
    Png,
    Gif,
    Webp,
}

impl ImageInputFormat {
    /// Number of leading bytes [`Self::sniff`] needs.
    pub const SNIFF_LEN: usize = 12;

    pub fn sniff(head: &[u8]) -> Option<Self> {
        if head.starts_with(&[0xFF, 0xD8, 0xFF]) {
            Some(Self::Jpeg)
        } else if head.starts_with(b"\x89PNG\r\n\x1a\n") {
            Some(Self::Png)
        } else if head.starts_with(b"GIF87a") || head.starts_with(b"GIF89a") {
            Some(Self::Gif)
        } else if head.len() >= 12 && head.starts_with(b"RIFF") && &head[8..12] == b"WEBP" {
            Some(Self::Webp)
        } else {
            None
        }
    }

    fn coder(self) -> &'static str {
        match self {
            Self::Jpeg => "jpeg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Webp => "webp",
        }
    }

    /// Whether a declared content type is one this processor can make variants for.
    pub fn supports_content_type(content_type: &str) -> bool {
        let essence = content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        matches!(
            essence.as_str(),
            "image/jpeg" | "image/png" | "image/gif" | "image/webp"
        )
    }
}

pub struct ImageOptions {
    width: String,
    format: String,
    content_type: String,
}

impl BaseProcessingOptions for ImageOptions {
    fn content_type(&self) -> String {
        self.content_type.clone()
    }
}

pub struct ImageProcessor;

#[async_trait]
impl VariantProcessor for ImageProcessor {
    type ProcessingOptions = ImageOptions;

    fn get_valid_variants_for_content_type(content_type: &str) -> Vec<FileVariant> {
        if ImageInputFormat::supports_content_type(content_type) {
            vec![FileVariant::Main, FileVariant::Small, FileVariant::Feed]
        } else {
            vec![FileVariant::Main]
        }
    }

    fn get_content_type_for_variant(file: &FileDetails, variant: &FileVariant) -> String {
        if variant.eq(&FileVariant::Main) {
            return file.content_type.clone();
        }
        String::from("image/webp")
    }

    fn get_options_for_variant(
        file: &FileDetails,
        variant: &FileVariant,
    ) -> Result<ImageOptions, MediaProcessorError> {
        let width = match variant {
            FileVariant::Small => String::from(SMALL_IMAGE_WIDTH),
            FileVariant::Feed => String::from(FEED_IMAGE_WIDTH),
            _ => return Err(MediaProcessorError::UnsupportedFileVariant),
        };
        let content_type = Self::get_content_type_for_variant(file, variant);
        Ok(ImageOptions {
            format: IMAGE_FORMAT.to_string(),
            width,
            content_type,
        })
    }

    async fn process(
        origin_file_path: &str,
        output_file_path: &str,
        options: &ImageOptions,
    ) -> Result<String, MediaProcessorError> {
        Self::process_with_slot_wait(
            origin_file_path,
            output_file_path,
            options,
            SLOT_WAIT_TIMEOUT,
        )
        .await
    }
}

impl ImageProcessor {
    async fn process_with_slot_wait(
        origin_file_path: &str,
        output_file_path: &str,
        options: &ImageOptions,
        slot_wait: Duration,
    ) -> Result<String, MediaProcessorError> {
        let marker = failed_variant_marker(output_file_path);
        // Requests for a variant that is known to fail do not queue for a slot.
        if let Some(outcome) = known_outcome(output_file_path, &marker).await {
            return outcome;
        }

        let slot = timeout(slot_wait, CONVERSION_SLOTS.clone().acquire_owned())
            .await
            .map_err(|_| MediaProcessorError::Busy)?
            .map_err(MediaProcessorError::command_failed)?;

        // A concurrent request may have built this variant, or failed to, while we waited.
        if let Some(outcome) = known_outcome(output_file_path, &marker).await {
            return outcome;
        }
        // Opened only once a slot is held, so waiting requests hold no file.
        let (input, input_format) = match Self::open_input(origin_file_path).await {
            Ok(opened) => opened,
            Err(e) => {
                record_failure(&marker, &e).await;
                return Err(e);
            }
        };

        let conversion = Conversion {
            input,
            input_format,
            origin_file_path: origin_file_path.to_string(),
            output_file_path: output_file_path.to_string(),
            width: options.width.clone(),
            format: options.format.clone(),
            // Starts with the slot held, so waiting for a slot takes nothing from this budget.
            deadline: Instant::now() + IMAGE_CONVERSION_TIMEOUT,
        };
        conversion
            .spawn(slot)
            .await
            .map_err(MediaProcessorError::command_failed)?
    }

    /// Opens the stored original, checks its size and identifies its format from its leading
    /// bytes. The returned handle, rewound to the start, is what gets decoded: a re-ingest that
    /// replaces the path afterwards does not change these bytes.
    async fn open_input(
        file_path: &str,
    ) -> Result<(std::fs::File, ImageInputFormat), MediaProcessorError> {
        let mut file = fs::File::open(file_path)
            .await
            .map_err(MediaProcessorError::command_failed)?;
        let size = file
            .metadata()
            .await
            .map_err(MediaProcessorError::command_failed)?
            .len();
        if size > MAX_IMAGE_INPUT_BYTES {
            return Err(MediaProcessorError::InputTooLarge {
                size,
                max: MAX_IMAGE_INPUT_BYTES,
            });
        }

        let mut head = [0u8; ImageInputFormat::SNIFF_LEN];
        let mut read = 0;
        while read < head.len() {
            let n = file
                .read(&mut head[read..])
                .await
                .map_err(MediaProcessorError::command_failed)?;
            if n == 0 {
                break;
            }
            read += n;
        }
        let format = ImageInputFormat::sniff(&head[..read])
            .ok_or(MediaProcessorError::UnsupportedInputFormat)?;

        file.seek(SeekFrom::Start(0))
            .await
            .map_err(MediaProcessorError::command_failed)?;
        Ok((file.into_std().await, format))
    }
}

/// What earlier requests already settled for this variant, if anything.
async fn known_outcome(
    output_file_path: &str,
    marker: &str,
) -> Option<Result<String, MediaProcessorError>> {
    if fs::metadata(output_file_path).await.is_ok() {
        return Some(Ok(String::new()));
    }
    if marker_is_fresh(marker).await {
        return Some(Err(MediaProcessorError::RecentlyFailed));
    }
    None
}

async fn marker_is_fresh(marker: &str) -> bool {
    let Ok(modified) = fs::metadata(marker)
        .await
        .and_then(|metadata| metadata.modified())
    else {
        return false;
    };
    // `elapsed` fails for a time in the future, which counts as fresh.
    modified
        .elapsed()
        .map_or(true, |age| age < FAILED_VARIANT_RETRY_AFTER)
}

/// Writes the variant's failure marker if the failure counts as the file's own. A slot-wait
/// timeout, a killed process or an I/O error on the host says nothing about the file and writes no
/// marker, so it does not hold back a good image. A non-zero `convert` exit cannot be told apart
/// from a host problem such as a full disk or a broken coder install, so it counts as the file's
/// failure and clears within the hour.
async fn record_failure(marker: &str, error: &MediaProcessorError) {
    if !error.is_caused_by_file() {
        return;
    }
    // Removed first: creating over an empty file may keep its modified time, and a repeat failure
    // must start a new retry period.
    let written = match remove_if_exists(marker).await {
        Ok(()) => fs::File::create(marker).await.map(drop),
        Err(e) => Err(e),
    };
    match written {
        // The only record of why the variant is held back: the request that hit the failure may
        // be gone, and later requests get `RecentlyFailed` without the cause.
        Ok(()) => tracing::warn!("Wrote the failure marker {marker}: {error}"),
        Err(e) => tracing::warn!("Could not write the failure marker {marker}: {e}"),
    }
}

/// Reads a process's error output to its end and returns at most
/// [`MAX_CAPTURED_STDERR_BYTES`] of it. The rest is read and discarded, so the process can never
/// block on a full pipe.
async fn capture_stderr(mut stderr: impl AsyncRead + Unpin) -> Vec<u8> {
    let mut message = Vec::new();
    let _ = (&mut stderr)
        .take(MAX_CAPTURED_STDERR_BYTES)
        .read_to_end(&mut message)
        .await;
    let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
    message
}

/// What a `convert` run that did not succeed says about the file. SIGKILL comes from outside the
/// process, typically from the OOM killer, so it says nothing about the file. Any other signal,
/// such as SIGSEGV or SIGABRT, is a decoder crash caused by the input.
fn failure_from_exit(status: ExitStatus, stderr: &[u8]) -> MediaProcessorError {
    const SIGKILL: i32 = 9;
    if status.signal() == Some(SIGKILL) {
        return MediaProcessorError::command_failed("convert was killed by signal 9 (SIGKILL)");
    }
    // A crashed process often writes nothing, so the status can be the only clue.
    let stderr = String::from_utf8_lossy(stderr);
    MediaProcessorError::ConversionFailed(format!("{status}: {stderr}"))
}

/// One ImageMagick run: decodes `input` with its sniffed decoder and writes the variant next to
/// `output_file_path`, renaming it into place on success.
struct Conversion {
    /// Stays open until the outcome is recorded: an open file's inode cannot be reused, so
    /// comparing it with `origin_file_path` cannot mistake a new file for this one.
    input: std::fs::File,
    input_format: ImageInputFormat,
    origin_file_path: String,
    output_file_path: String,
    width: String,
    format: String,
    deadline: Instant,
}

impl Conversion {
    /// Runs as its own task, which continues when the request that started it goes away: the
    /// child is always reaped, its files removed and its outcome recorded, and the slot is held
    /// until all of that is done. If the original was replaced meanwhile, the outcome is discarded
    /// and the answer is [`MediaProcessorError::Busy`], so the client asks again.
    fn spawn(self, slot: OwnedSemaphorePermit) -> JoinHandle<Result<String, MediaProcessorError>> {
        let marker = failed_variant_marker(&self.output_file_path);
        tokio::spawn(async move {
            let result = self.run().await;
            match &result {
                // Best effort: only an expired marker can still be there.
                Ok(_) => {
                    let _ = fs::remove_file(&marker).await;
                }
                Err(e) => record_failure(&marker, e).await,
            }
            // Checked after writing, which closes the race with `Blob::put_to_static`. It renames
            // main before its cleanup, so if this sees the old main the cleanup runs later and
            // removes the output, and if it sees the new main the output is removed here.
            let result = if self.origin_changed().await {
                let written = if result.is_ok() {
                    &self.output_file_path
                } else {
                    &marker
                };
                let _ = remove_if_exists(written).await;
                Err(MediaProcessorError::Busy)
            } else {
                result
            };
            drop(slot);
            result
        })
    }

    /// Whether the origin path no longer holds the file this conversion read. A missing or
    /// unreadable origin counts as changed.
    async fn origin_changed(&self) -> bool {
        let (Ok(read), Ok(current)) = (
            self.input.metadata(),
            fs::metadata(&self.origin_file_path).await,
        ) else {
            return true;
        };
        (read.dev(), read.ino()) != (current.dev(), current.ino())
    }

    async fn run(&self) -> Result<String, MediaProcessorError> {
        // ImageMagick's own temporary files (spilled input, disk pixel cache) go here and are
        // removed with it, including after the process is killed.
        let scratch = tempfile::Builder::new()
            .prefix("nexus-convert-")
            .tempdir()
            .map_err(MediaProcessorError::command_failed)?;
        let temp_output = TempOutput(format!(
            "{}.tmp-{}-{}",
            self.output_file_path,
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        self.convert(scratch.path(), &temp_output.0).await?;
        fs::rename(&temp_output.0, &self.output_file_path)
            .await
            .map_err(MediaProcessorError::command_failed)?;
        Ok(String::new())
    }

    async fn convert(
        &self,
        scratch: &std::path::Path,
        temp_output: &str,
    ) -> Result<(), MediaProcessorError> {
        let input = self
            .input
            .try_clone()
            .map_err(MediaProcessorError::command_failed)?;

        let mut command = Command::new("convert");
        for (resource, value) in IMAGEMAGICK_LIMITS {
            command.arg("-limit").arg(resource).arg(value);
        }
        command
            .arg(format!("{}:-", self.input_format.coder()))
            .arg("-resize")
            .arg(format!("{}x", self.width))
            .arg("-auto-orient") // https://github.com/ImageMagick/ImageMagick/issues/6396
            .arg(format!("{}:{temp_output}", self.format))
            .env("MAGICK_TEMPORARY_PATH", scratch)
            .stdin(Stdio::from(input))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(MediaProcessorError::command_failed)?;
        let mut stderr = child.stderr.take();

        let finished = timeout_at(self.deadline, async {
            let read_stderr = async {
                match stderr.as_mut() {
                    Some(stderr) => capture_stderr(stderr).await,
                    None => Vec::new(),
                }
            };
            let (status, message) = tokio::join!(child.wait(), read_stderr);
            status.map(|status| (status, message))
        })
        .await;

        let result = match finished {
            Ok(Ok((status, _))) if status.success() => return Ok(()),
            Ok(Ok((status, message))) => return Err(failure_from_exit(status, &message)),
            Ok(Err(e)) => Err(MediaProcessorError::command_failed(e)),
            Err(_) => Err(MediaProcessorError::TimedOut),
        };
        // The process may still be running: kill it and wait until it is gone, so nothing writes
        // to the scratch directory or the temporary output once they are removed.
        let _ = child.start_kill();
        let _ = child.wait().await;
        result
    }
}

/// A temporary variant file, removed when dropped unless it was renamed into place.
struct TempOutput(String);

impl Drop for TempOutput {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Instant;

    /// A 69-byte PNG whose header declares a 60000x60000 RGB canvas.
    const PNG_HEADER_BOMB_60K: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0xea, 0x60, 0x00, 0x00, 0xea, 0x60, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0f,
        0xb0, 0xe2, 0x15, 0x00, 0x00, 0x00, 0x0c, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x60,
        0xa0, 0x3d, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x86, 0x64, 0x3c, 0x35, 0x00, 0x00, 0x00,
        0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];

    fn png_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
        let mut crc = flate2::Crc::new();
        crc.update(kind);
        crc.update(data);
        let mut chunk = (data.len() as u32).to_be_bytes().to_vec();
        chunk.extend_from_slice(kind);
        chunk.extend_from_slice(data);
        chunk.extend_from_slice(&crc.sum().to_be_bytes());
        chunk
    }

    /// A valid all-black 1-bit greyscale PNG of `side` x `side` pixels: a few hundred KB on disk
    /// that decodes to `side`² pixels.
    fn png_decompression_bomb(side: u32) -> Vec<u8> {
        use std::io::Write;
        let row = vec![0u8; 1 + (side as usize).div_ceil(8)];
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
        for _ in 0..side {
            encoder.write_all(&row).unwrap();
        }
        let idat = encoder.finish().unwrap();

        let mut ihdr = side.to_be_bytes().to_vec();
        ihdr.extend_from_slice(&side.to_be_bytes());
        ihdr.extend_from_slice(&[1, 0, 0, 0, 0]);

        let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
        png.extend(png_chunk(b"IHDR", &ihdr));
        png.extend(png_chunk(b"IDAT", &idat));
        png.extend(png_chunk(b"IEND", &[]));
        png
    }

    /// Held by every test that converts through `ImageProcessor::process`, so tests that depend
    /// on when a conversion runs do not compete with other tests for the shared slots.
    static SLOT_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Peak resident memory of any child process this test binary has waited for. Every other
    /// test here keeps ImageMagick well under the bomb test's 512 MiB bound.
    fn peak_child_rss_bytes() -> u64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: getrusage only writes into the provided, properly sized rusage struct.
        let usage = unsafe {
            assert_eq!(
                libc::getrusage(libc::RUSAGE_CHILDREN, usage.as_mut_ptr()),
                0
            );
            usage.assume_init()
        };
        let max_rss = usage.ru_maxrss as u64;
        // Linux reports kilobytes, macOS bytes.
        if cfg!(target_os = "macos") {
            max_rss
        } else {
            max_rss * 1024
        }
    }

    fn small_options() -> ImageOptions {
        ImageOptions {
            width: SMALL_IMAGE_WIDTH.to_string(),
            format: IMAGE_FORMAT.to_string(),
            content_type: "image/webp".to_string(),
        }
    }

    /// A file that is identified as a PNG and that no decoder accepts.
    const CORRUPT_PNG: &[u8] = b"\x89PNG\r\n\x1a\nthis is not an image";

    /// Writes a failure marker of the given age.
    fn write_aged_marker(marker: &Path, age: Duration) {
        std::fs::File::create(marker)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - age)
            .unwrap();
    }

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("utf-8 temp path")
    }

    /// Opens a conversion of `source` the way a request does, without running it.
    async fn open_conversion(source: &Path, output: &Path, budget: Duration) -> Conversion {
        let (input, input_format) = ImageProcessor::open_input(path_str(source)).await.unwrap();
        Conversion {
            input,
            input_format,
            origin_file_path: path_str(source).to_string(),
            output_file_path: path_str(output).to_string(),
            width: SMALL_IMAGE_WIDTH.to_string(),
            format: IMAGE_FORMAT.to_string(),
            deadline: tokio::time::Instant::now() + budget,
        }
    }

    /// Replaces a file the way `Blob::put_to_static` does: a different file is renamed over it.
    fn replace_by_rename(origin: &Path) {
        let replacement = origin.with_extension("replacement");
        std::fs::write(&replacement, b"new").unwrap();
        std::fs::rename(&replacement, origin).unwrap();
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Writes a source image with the system ImageMagick.
    async fn make_source(dir: &Path, name: &str, spec: &[&str]) -> std::path::PathBuf {
        let path = dir.join(name);
        let status = Command::new("convert")
            .args(spec)
            .arg(format!("{}:{}", name, path_str(&path)))
            .status()
            .await
            .expect("ImageMagick convert must be installed to run the image tests");
        assert!(status.success(), "could not create the {name} source");
        path
    }

    #[tokio::test]
    async fn payloads_that_are_not_allowlisted_rasters_never_reach_a_decoder() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let canary = dir.path().join("secret");
        std::fs::write(&canary, "CANARY-1f0c8e").unwrap();
        let canary = path_str(&canary);

        let payloads: Vec<(&str, String)> = vec![
            (
                "svg-text-href",
                format!(
                    r#"<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="400" height="100"><image width="400" height="100" xlink:href="text:{canary}"/></svg>"#
                ),
            ),
            (
                "svg-file-href",
                format!(
                    r#"<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="400" height="100"><image width="400" height="100" xlink:href="file://{canary}"/></svg>"#
                ),
            ),
            (
                "mvg-image-text",
                format!(
                    "push graphic-context\nviewbox 0 0 400 100\nimage over 0,0 400,100 'text:{canary}'\npop graphic-context\n"
                ),
            ),
            (
                "msl-read",
                format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><image><read filename="text:{canary}"/></image>"#
                ),
            ),
            (
                "pixel-enumeration",
                "# ImageMagick pixel enumeration: 2,1,255,srgb\n0,0: (255,0,0)\n1,0: (0,0,255)\n"
                    .to_string(),
            ),
        ];

        for (name, payload) in payloads {
            let source = dir.path().join(format!("{name}-main"));
            std::fs::write(&source, payload).unwrap();
            let output = dir.path().join(format!("{name}-small"));

            let result =
                ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
                    .await;

            assert!(result.is_err(), "{name}: was decoded");
            assert!(!output.exists(), "{name}: a variant was written");
            assert!(
                dir.path().join(format!("{name}-small.failed")).exists(),
                "{name}: no marker"
            );

            let second =
                ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
                    .await;
            assert!(
                matches!(second, Err(MediaProcessorError::RecentlyFailed)),
                "{name}: {second:?}"
            );
        }
        assert!(
            entries(dir.path())
                .iter()
                .all(|entry| !entry.contains(".tmp-")),
            "temporary output left behind"
        );
    }

    #[tokio::test]
    async fn decompression_bombs_fail_fast_without_a_variant() {
        let _serial = SLOT_TESTS.lock().await;
        let bombs = [
            ("compressed-40k", png_decompression_bomb(40_000)),
            ("header-60k", PNG_HEADER_BOMB_60K.to_vec()),
        ];
        for (name, bomb) in bombs {
            let dir = tempfile::TempDir::new().unwrap();
            let source = dir.path().join("main");
            std::fs::write(&source, &bomb).unwrap();
            let output = dir.path().join("small");

            let started = Instant::now();
            let result =
                ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
                    .await;

            assert!(result.is_err(), "{name}: the bomb was decoded");
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{name}: took {:?}",
                started.elapsed()
            );
            assert_eq!(
                entries(dir.path()),
                vec!["main".to_string(), "small.failed".to_string()],
                "{name}"
            );
        }
        assert!(
            peak_child_rss_bytes() < 512 * 1024 * 1024,
            "ImageMagick peaked at {} MiB",
            peak_child_rss_bytes() / (1024 * 1024)
        );
    }

    #[tokio::test]
    async fn an_oversized_original_is_refused_before_decoding() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        let file = std::fs::File::create(&source).unwrap();
        std::io::Write::write_all(&mut &file, b"\x89PNG\r\n\x1a\n").unwrap();
        file.set_len(MAX_IMAGE_INPUT_BYTES + 1).unwrap();
        let output = dir.path().join("small");

        let result =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(matches!(
            result,
            Err(MediaProcessorError::InputTooLarge { .. })
        ));
        assert!(!output.exists());
        assert!(dir.path().join("small.failed").exists());

        let second =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;
        assert!(
            matches!(second, Err(MediaProcessorError::RecentlyFailed)),
            "{second:?}"
        );
    }

    #[tokio::test]
    async fn allowlisted_rasters_become_webp_variants() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let sources = [
            make_source(
                dir.path(),
                "jpeg",
                &["-size", "640x480", "gradient:red-blue"],
            )
            .await,
            make_source(
                dir.path(),
                "png",
                &["-size", "640x480", "gradient:white-black"],
            )
            .await,
            make_source(
                dir.path(),
                "gif",
                &["-size", "64x48", "xc:red", "xc:blue", "-set", "delay", "20"],
            )
            .await,
            make_source(
                dir.path(),
                "webp",
                &["-size", "640x480", "gradient:yellow-green"],
            )
            .await,
        ];

        for source in sources {
            let output = source.with_extension("small");
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
                .await
                .unwrap_or_else(|e| panic!("{source:?}: {e}"));

            let variant = std::fs::read(&output).unwrap();
            assert_eq!(&variant[0..4], b"RIFF", "{source:?}");
            assert_eq!(&variant[8..12], b"WEBP", "{source:?}");
            let animated = variant.windows(4).any(|chunk| chunk == b"ANIM");
            assert_eq!(animated, source.ends_with("gif"), "{source:?} animation");
        }
        assert!(entries(dir.path())
            .iter()
            .all(|entry| !entry.contains(".tmp-")));
    }

    #[tokio::test]
    async fn concurrent_requests_for_one_variant_all_succeed_with_one_complete_file() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "1600x1200", "plasma:"]).await;
        let output = dir.path().join("feed");

        let runs = (0..6).map(|_| {
            let source = source.clone();
            let output = output.clone();
            tokio::spawn(async move {
                ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
                    .await
            })
        });
        for run in futures::future::join_all(runs).await {
            run.unwrap().expect("conversion failed");
        }

        let variant = std::fs::read(&output).unwrap();
        assert_eq!(&variant[8..12], b"WEBP");
        assert_eq!(
            entries(dir.path()),
            vec!["feed".to_string(), "png".to_string()]
        );
    }

    #[tokio::test]
    async fn a_dropped_request_still_finishes_its_conversion_cleanly() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(
            dir.path(),
            "png",
            &["-size", "4000x3000", "gradient:red-blue"],
        )
        .await;
        let output = dir.path().join("feed");

        let dropped = tokio::time::timeout(
            Duration::from_millis(100),
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()),
        )
        .await;
        assert!(
            dropped.is_err(),
            "the conversion finished before the request was dropped"
        );

        // The conversion keeps its slot until it has finished and cleaned up.
        let _all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();
        let variant = std::fs::read(&output).expect("the dropped request's variant is missing");
        assert_eq!(&variant[8..12], b"WEBP");
        assert_eq!(
            entries(dir.path()),
            vec!["feed".to_string(), "png".to_string()]
        );
    }

    #[tokio::test]
    async fn a_conversion_past_its_deadline_is_killed_and_cleaned_up() {
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(
            dir.path(),
            "png",
            &["-size", "4000x3000", "gradient:red-blue"],
        )
        .await;
        let output = dir.path().join("feed");
        let (input, input_format) = ImageProcessor::open_input(path_str(&source)).await.unwrap();

        let conversion = Conversion {
            input,
            input_format,
            origin_file_path: path_str(&source).to_string(),
            output_file_path: path_str(&output).to_string(),
            width: FEED_IMAGE_WIDTH.to_string(),
            format: IMAGE_FORMAT.to_string(),
            deadline: tokio::time::Instant::now() + Duration::from_millis(50),
        };
        let started = Instant::now();
        let result = conversion.run().await;

        assert!(
            matches!(result, Err(MediaProcessorError::TimedOut)),
            "{result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(entries(dir.path()), vec!["png".to_string()]);
    }

    #[tokio::test]
    async fn a_spawned_conversion_past_its_deadline_leaves_its_marker() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(
            dir.path(),
            "png",
            &["-size", "4000x3000", "gradient:red-blue"],
        )
        .await;
        let output = dir.path().join("feed");
        let conversion = open_conversion(&source, &output, Duration::from_millis(50)).await;
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        let result = conversion.spawn(slot).await.unwrap();

        assert!(
            matches!(result, Err(MediaProcessorError::TimedOut)),
            "{result:?}"
        );
        assert_eq!(
            entries(dir.path()),
            vec!["feed.failed".to_string(), "png".to_string()]
        );
    }

    #[tokio::test]
    async fn a_replaced_original_discards_the_finished_variant() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        let conversion = open_conversion(&source, &output, IMAGE_CONVERSION_TIMEOUT).await;
        replace_by_rename(&source);
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        let result = conversion.spawn(slot).await.unwrap();

        assert!(
            matches!(result, Err(MediaProcessorError::Busy)),
            "{result:?}"
        );
        assert_eq!(entries(dir.path()), vec!["png".to_string()]);
    }

    #[tokio::test]
    async fn a_replaced_original_discards_the_failure_marker() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("small");
        let conversion = open_conversion(&source, &output, IMAGE_CONVERSION_TIMEOUT).await;
        replace_by_rename(&source);
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        let result = conversion.spawn(slot).await.unwrap();

        assert!(
            matches!(result, Err(MediaProcessorError::Busy)),
            "{result:?}"
        );
        assert_eq!(entries(dir.path()), vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn a_deleted_original_discards_the_finished_variant() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        let conversion = open_conversion(&source, &output, IMAGE_CONVERSION_TIMEOUT).await;
        std::fs::remove_file(&source).unwrap();
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        let result = conversion.spawn(slot).await.unwrap();

        assert!(
            matches!(result, Err(MediaProcessorError::Busy)),
            "{result:?}"
        );
        assert!(entries(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn an_unchanged_original_keeps_the_finished_variant() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        let conversion = open_conversion(&source, &output, IMAGE_CONVERSION_TIMEOUT).await;
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        conversion
            .spawn(slot)
            .await
            .unwrap()
            .expect("conversion failed");

        assert_eq!(
            entries(dir.path()),
            vec!["png".to_string(), "small".to_string()]
        );
    }

    #[test]
    fn only_a_killed_process_is_not_the_files_failure() {
        let killed = failure_from_exit(ExitStatus::from_raw(9), b"");
        assert!(
            matches!(killed, MediaProcessorError::CommandFailed { .. }),
            "{killed:?}"
        );
        assert!(killed.to_string().contains("SIGKILL"), "{killed}");
        assert!(!killed.is_caused_by_file());

        // SIGSEGV and exit code 1.
        for raw_status in [11, 256] {
            let status = ExitStatus::from_raw(raw_status);
            let failure = failure_from_exit(status, b"message");
            assert!(
                matches!(failure, MediaProcessorError::ConversionFailed(_)),
                "{raw_status}: {failure:?}"
            );
            assert!(failure.is_caused_by_file(), "{raw_status}");
            assert!(
                failure.to_string().contains(&status.to_string()),
                "{raw_status}: {failure}"
            );
            assert!(
                failure.to_string().contains("message"),
                "{raw_status}: {failure}"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_variant_is_not_converted_again_while_its_marker_is_fresh() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("small");
        let marker = dir.path().join("small.failed");

        let first =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(
            matches!(first, Err(MediaProcessorError::ConversionFailed(_))),
            "{first:?}"
        );
        assert_eq!(std::fs::metadata(&marker).unwrap().len(), 0);

        // The original now converts, so only the marker can keep the variant from being built.
        let valid = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        std::fs::copy(valid, &source).unwrap();

        let second =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(
            matches!(second, Err(MediaProcessorError::RecentlyFailed)),
            "{second:?}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn an_expired_marker_allows_a_retry() {
        let _serial = SLOT_TESTS.lock().await;
        let expired = FAILED_VARIANT_RETRY_AFTER + Duration::from_secs(60);
        let dir = tempfile::TempDir::new().unwrap();

        // A retry that succeeds builds the variant and removes the marker.
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        let marker = dir.path().join("small.failed");
        write_aged_marker(&marker, expired);

        ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
            .await
            .expect("the retry failed");

        assert!(output.exists());
        assert!(!marker.exists());

        // A retry that fails starts a new retry period.
        let source = dir.path().join("corrupt");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("feed");
        let marker = dir.path().join("feed.failed");
        write_aged_marker(&marker, expired);

        let result =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(
            matches!(result, Err(MediaProcessorError::ConversionFailed(_))),
            "{result:?}"
        );
        let age = std::fs::metadata(&marker)
            .unwrap()
            .modified()
            .unwrap()
            .elapsed()
            .unwrap();
        assert!(age < Duration::from_secs(60), "marker age is {age:?}");
    }

    #[tokio::test]
    async fn a_marker_dated_in_the_future_counts_as_fresh() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        std::fs::File::create(dir.path().join("small.failed"))
            .unwrap()
            .set_modified(std::time::SystemTime::now() + Duration::from_secs(60 * 60))
            .unwrap();

        let result =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(
            matches!(result, Err(MediaProcessorError::RecentlyFailed)),
            "{result:?}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn a_waiter_that_gets_a_slot_respects_a_failure_recorded_meanwhile() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = make_source(dir.path(), "png", &["-size", "64x48", "xc:red"]).await;
        let output = dir.path().join("small");
        let all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();

        let waiter = {
            let (source, output) = (source.clone(), output.clone());
            tokio::spawn(async move {
                ImageProcessor::process_with_slot_wait(
                    path_str(&source),
                    path_str(&output),
                    &small_options(),
                    Duration::from_secs(60),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::File::create(dir.path().join("small.failed")).unwrap();
        drop(all_slots);

        let result = waiter.await.unwrap();

        assert!(
            matches!(result, Err(MediaProcessorError::RecentlyFailed)),
            "{result:?}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn a_timeout_waiting_for_a_slot_is_busy_and_leaves_no_marker() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("small");
        let _all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();

        let result = ImageProcessor::process_with_slot_wait(
            path_str(&source),
            path_str(&output),
            &small_options(),
            Duration::from_millis(50),
        )
        .await;

        assert!(
            matches!(result, Err(MediaProcessorError::Busy)),
            "{result:?}"
        );
        assert_eq!(entries(dir.path()), vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn a_known_failure_does_not_wait_for_a_slot() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("small");
        std::fs::File::create(dir.path().join("small.failed")).unwrap();
        let _all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();

        let result = ImageProcessor::process_with_slot_wait(
            path_str(&source),
            path_str(&output),
            &small_options(),
            Duration::from_millis(50),
        )
        .await;

        assert!(
            matches!(result, Err(MediaProcessorError::RecentlyFailed)),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_host_problem_leaves_no_marker() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        let output = dir.path().join("small");

        let result =
            ImageProcessor::process(path_str(&source), path_str(&output), &small_options()).await;

        assert!(
            matches!(result, Err(MediaProcessorError::CommandFailed { .. })),
            "{result:?}"
        );
        assert!(entries(dir.path()).is_empty());
    }

    /// A corrupt file fails too fast to drop a real request while its conversion runs, so this
    /// drops the handle of the spawned task instead, which is what a dropped request leaves.
    #[tokio::test]
    async fn a_failed_conversion_leaves_its_marker_after_the_request_is_gone() {
        let _serial = SLOT_TESTS.lock().await;
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("main");
        std::fs::write(&source, CORRUPT_PNG).unwrap();
        let output = dir.path().join("small");
        let (input, input_format) = ImageProcessor::open_input(path_str(&source)).await.unwrap();
        let conversion = Conversion {
            input,
            input_format,
            origin_file_path: path_str(&source).to_string(),
            output_file_path: path_str(&output).to_string(),
            width: SMALL_IMAGE_WIDTH.to_string(),
            format: IMAGE_FORMAT.to_string(),
            deadline: tokio::time::Instant::now() + IMAGE_CONVERSION_TIMEOUT,
        };
        let slot = CONVERSION_SLOTS.clone().acquire_owned().await.unwrap();

        drop(conversion.spawn(slot));

        // The task keeps its slot until the marker is written.
        let _all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();
        assert_eq!(
            entries(dir.path()),
            vec!["main".to_string(), "small.failed".to_string()]
        );
    }

    #[tokio::test]
    async fn captured_stderr_is_capped_and_the_rest_is_drained() {
        let total = MAX_CAPTURED_STDERR_BYTES * 3;
        let mut stderr = std::io::Cursor::new(vec![b'x'; total as usize]);

        let message = capture_stderr(&mut stderr).await;

        assert_eq!(message.len() as u64, MAX_CAPTURED_STDERR_BYTES);
        assert_eq!(stderr.position(), total);
    }

    #[test]
    fn sniff_accepts_only_the_allowlisted_raster_formats() {
        assert_eq!(
            ImageInputFormat::sniff(&[0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10, b'J', b'F']),
            Some(ImageInputFormat::Jpeg)
        );
        assert_eq!(
            ImageInputFormat::sniff(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR"),
            Some(ImageInputFormat::Png)
        );
        assert_eq!(
            ImageInputFormat::sniff(b"GIF89a\x01\x00\x01\x00"),
            Some(ImageInputFormat::Gif)
        );
        assert_eq!(
            ImageInputFormat::sniff(b"GIF87a\x01\x00\x01\x00"),
            Some(ImageInputFormat::Gif)
        );
        assert_eq!(
            ImageInputFormat::sniff(b"RIFF\x24\x00\x00\x00WEBPVP8 "),
            Some(ImageInputFormat::Webp)
        );

        for rejected in [
            b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>".as_slice(),
            b"<?xml version=\"1.0\"?><svg/>",
            b"push graphic-context\nviewbox 0 0 1 1\n",
            b"# ImageMagick pixel enumeration: 1,1,255,srgb\n",
            b"id=ImageMagick  version=1.0\n",
            b"/* XPM */\nstatic char",
            b"%!PS-Adobe-3.0\n",
            b"%PDF-1.7\n",
            b"II*\x00",
            b"RIFF\x24\x00\x00\x00WAVEfmt ",
            b"\x89PN",
            b"",
        ] {
            assert_eq!(ImageInputFormat::sniff(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn only_raster_content_types_get_resized_variants() {
        for content_type in [
            "image/jpeg",
            "image/png",
            "image/gif",
            "image/webp",
            "IMAGE/PNG; q=1",
        ] {
            assert!(ImageInputFormat::supports_content_type(content_type));
            assert_eq!(
                ImageProcessor::get_valid_variants_for_content_type(content_type),
                vec![FileVariant::Main, FileVariant::Small, FileVariant::Feed]
            );
        }
        for content_type in ["image/svg+xml", "image/tiff", "image/heic", "image/"] {
            assert!(!ImageInputFormat::supports_content_type(content_type));
            assert_eq!(
                ImageProcessor::get_valid_variants_for_content_type(content_type),
                vec![FileVariant::Main]
            );
        }
    }
}
