use std::{
    io::SeekFrom,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    process::Command,
    sync::Semaphore,
    time::{timeout_at, Instant},
};

use crate::{
    media::{processors::MediaProcessorError, FileVariant},
    models::file::FileDetails,
};

use super::{BaseProcessingOptions, VariantProcessor};

const SMALL_IMAGE_WIDTH: &str = "320";
const FEED_IMAGE_WIDTH: &str = "720";
const IMAGE_FORMAT: &str = "webp";

/// Largest stored original the processor decodes. Clients downscale uploads to a few MB, so
/// anything larger is not a legitimate image upload.
pub const MAX_IMAGE_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Wall-clock budget for one conversion, including the wait for a free conversion slot.
pub const IMAGE_CONVERSION_TIMEOUT: Duration = Duration::from_secs(30);

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
        let deadline = Instant::now() + IMAGE_CONVERSION_TIMEOUT;
        let (input, input_format) = Self::open_input(origin_file_path).await?;

        let slot = timeout_at(deadline, CONVERSION_SLOTS.clone().acquire_owned())
            .await
            .map_err(|_| MediaProcessorError::TimedOut)?
            .map_err(MediaProcessorError::command_failed)?;

        // A concurrent request may have produced this variant while we waited for a slot.
        if fs::metadata(output_file_path).await.is_ok() {
            return Ok(String::new());
        }

        let conversion = Conversion {
            input,
            input_format,
            output_file_path: output_file_path.to_string(),
            width: options.width.clone(),
            format: options.format.clone(),
            deadline,
        };
        // Runs to completion even when the request that started it goes away, so the child is
        // always reaped and its files removed, and the slot is held for as long as it runs.
        tokio::spawn(async move {
            let result = conversion.run().await;
            drop(slot);
            result
        })
        .await
        .map_err(MediaProcessorError::command_failed)?
    }
}

impl ImageProcessor {
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

/// One ImageMagick run: decodes `input` with its sniffed decoder and writes the variant next to
/// `output_file_path`, renaming it into place on success.
struct Conversion {
    input: std::fs::File,
    input_format: ImageInputFormat,
    output_file_path: String,
    width: String,
    format: String,
    deadline: Instant,
}

impl Conversion {
    async fn run(self) -> Result<String, MediaProcessorError> {
        // ImageMagick's own temporary files (spilled input, disk pixel cache) go here and are
        // removed with it, including after the process is killed.
        let scratch = tempfile::Builder::new()
            .prefix("nexus-convert-")
            .tempdir()
            .map_err(MediaProcessorError::command_failed)?;
        let temp_output = format!(
            "{}.tmp-{}-{}",
            self.output_file_path,
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        let result = self.convert(scratch.path(), &temp_output).await;
        let result = match result {
            Ok(()) => fs::rename(&temp_output, &self.output_file_path)
                .await
                .map_err(MediaProcessorError::command_failed),
            Err(e) => Err(e),
        };
        if result.is_err() {
            let _ = fs::remove_file(&temp_output).await;
        }
        drop(scratch);
        result.map(|_| String::new())
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
            let mut message = Vec::new();
            let read_stderr = async {
                if let Some(stderr) = stderr.as_mut() {
                    let _ = stderr.read_to_end(&mut message).await;
                }
            };
            let (status, ()) = tokio::join!(child.wait(), read_stderr);
            status.map(|status| (status, message))
        })
        .await;

        match finished {
            Ok(Ok((status, _))) if status.success() => Ok(()),
            Ok(Ok((_, message))) => Err(MediaProcessorError::command_failed(format!(
                "ImageMagick command failed: {}",
                String::from_utf8_lossy(&message)
            ))),
            Ok(Err(e)) => Err(MediaProcessorError::command_failed(e)),
            Err(_) => {
                // Kill and reap before the scratch directory is removed.
                let _ = child.kill().await;
                Err(MediaProcessorError::TimedOut)
            }
        }
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

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("utf-8 temp path")
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
            assert_eq!(entries(dir.path()), vec!["main".to_string()], "{name}");
        }
        assert!(
            peak_child_rss_bytes() < 512 * 1024 * 1024,
            "ImageMagick peaked at {} MiB",
            peak_child_rss_bytes() / (1024 * 1024)
        );
    }

    #[tokio::test]
    async fn an_oversized_original_is_refused_before_decoding() {
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
    }

    #[tokio::test]
    async fn allowlisted_rasters_become_webp_variants() {
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

        ImageProcessor::process(path_str(&source), path_str(&output), &small_options())
            .await
            .expect("conversion after a dropped request failed");
        // Every conversion releases its slot only after cleaning up.
        let _all_slots = CONVERSION_SLOTS
            .acquire_many(MAX_CONCURRENT_CONVERSIONS as u32)
            .await
            .unwrap();

        let variant = std::fs::read(&output).unwrap();
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
