use crate::{
    media::{
        processors::{ImageProcessor, VariantProcessor, VideoProcessor},
        FileVariant, VariantController,
    },
    models::error::{ModelError, ModelResult},
};
use pubky_app_specs::PubkyAppBlob;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use super::FileDetails;

pub struct Blob;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Blob {
    pub async fn put_to_static(
        name: String,
        files_path: PathBuf,
        blob: &PubkyAppBlob,
    ) -> ModelResult<()> {
        if !fs::metadata(&files_path)
            .await
            .is_ok_and(|metadata| metadata.is_dir())
        {
            fs::create_dir_all(&files_path).await?;
        };

        // Written aside and renamed into place, so a concurrent variant build never reads a partial file.
        let file_path = files_path.join(&name);
        let temp_path = files_path.join(format!(
            "{name}.tmp-{}-{}",
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let written = async {
            let mut static_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await?;
            static_file.write_all(&blob.0).await?;
            static_file.flush().await?;
            fs::rename(&temp_path, &file_path).await
        }
        .await;
        if written.is_err() {
            let _ = fs::remove_file(&temp_path).await;
        }
        written?;

        Ok(())
    }

    pub async fn get_by_id(
        file: &FileDetails,
        variant: &FileVariant,
        file_path: PathBuf,
    ) -> ModelResult<String> {
        let file_variant_exists =
            VariantController::check_variant_exists(file, variant.clone(), file_path.clone()).await;

        if file_variant_exists {
            Ok(VariantController::get_content_type_for_variant(
                file, variant,
            ))
        } else {
            Self::put_variant(file, variant, file_path)
                .await
                .inspect_err(|e| {
                    tracing::error!("Creating variant failed for file: {file:?} with error: {e}")
                })
        }
    }

    async fn put_variant(
        file: &FileDetails,
        variant: &FileVariant,
        file_path: PathBuf,
    ) -> ModelResult<String> {
        match &file.content_type {
            content_type if content_type.starts_with("image/") => {
                ImageProcessor::create_variant(file, variant, file_path)
                    .await
                    .map_err(Into::into)
            }
            content_type if content_type.starts_with("video/") => {
                VideoProcessor::create_variant(file, variant, file_path)
                    .await
                    .map_err(Into::into)
            }
            _ => Err(ModelError::from_generic(format!(
                "Unsupported content type: {}",
                file.content_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky_app_specs::PubkyAppBlob;
    use tokio::{fs::File, io::AsyncReadExt};

    #[tokio_shared_rt::test(shared)]
    async fn test_put_to_static_creates_new_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let files_path = tmp_dir.path().join("user1").join("file1");
        let blob = PubkyAppBlob::new(b"hello world".to_vec());

        Blob::put_to_static("main".to_string(), files_path.clone(), &blob)
            .await
            .expect("put_to_static should succeed for a new file");

        let mut content = Vec::new();
        File::open(files_path.join("main"))
            .await
            .unwrap()
            .read_to_end(&mut content)
            .await
            .unwrap();
        assert_eq!(content, b"hello world");
    }

    #[tokio_shared_rt::test(shared)]
    async fn test_put_to_static_overwrites_existing_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let files_path = tmp_dir.path().join("user1").join("file1");
        let blob1 = PubkyAppBlob::new(b"first write".to_vec());

        Blob::put_to_static("main".to_string(), files_path.clone(), &blob1)
            .await
            .expect("first put_to_static should succeed");

        // Calling put_to_static again simulates re-indexing when the file already exists on disk
        let blob2 = PubkyAppBlob::new(b"second write".to_vec());
        Blob::put_to_static("main".to_string(), files_path.clone(), &blob2)
            .await
            .expect("put_to_static should succeed even when file already exists (re-indexing)");

        let mut content = Vec::new();
        File::open(files_path.join("main"))
            .await
            .unwrap()
            .read_to_end(&mut content)
            .await
            .unwrap();
        assert_eq!(content, b"second write");

        let mut names = Vec::new();
        let mut dir = fs::read_dir(&files_path).await.unwrap();
        while let Some(entry) = dir.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(names, vec!["main".to_string()]);
    }

    #[tokio_shared_rt::test(shared)]
    async fn concurrent_writes_of_one_file_leave_one_complete_version() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let files_path = tmp_dir.path().join("user1").join("file1");
        let versions: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 256 * 1024]).collect();

        let writes = versions.iter().cloned().map(|bytes| {
            let files_path = files_path.clone();
            tokio::spawn(async move {
                Blob::put_to_static("main".to_string(), files_path, &PubkyAppBlob::new(bytes)).await
            })
        });
        for write in futures::future::join_all(writes).await {
            write.unwrap().expect("put_to_static failed");
        }

        let content = fs::read(files_path.join("main")).await.unwrap();
        assert!(versions.contains(&content), "main is a mix of versions");
        let mut names = Vec::new();
        let mut dir = fs::read_dir(&files_path).await.unwrap();
        while let Some(entry) = dir.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(names, vec!["main".to_string()]);
    }
}
