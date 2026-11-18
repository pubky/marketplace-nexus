use std::{fs, path::PathBuf};

use crate::utils::host_url;
use anyhow::Result;
use nexus_common::media::FileVariant;
use nexus_common::models::{
    file::{FileDetails, FileUrls},
    traits::Collection,
};
use tokio::fs::create_dir_all;

const IMAGE_BLOB_NAME: &str = "SynonymLogo.png";
const BLOB_PATH: &str = "tests/files/blobs";

const USER_PUBKY: &str = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

/// Indexes a PNG file record of its own, with its own original on disk.
///
/// Each test needs a separate original: a variant is refused when its original changes during the
/// conversion, so tests running in parallel must never rewrite one another's original.
async fn index_image(file_id: &str) -> Result<FileDetails> {
    let dir = format!("static/files/{USER_PUBKY}/{file_id}");
    create_dir_all(&dir).await?;
    let original = format!("{dir}/main");
    fs::copy(PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME), &original)?;
    let size = fs::metadata(&original)?.len() as i64;

    let file = FileDetails {
        id: file_id.to_string(),
        uri: format!("pubky://{USER_PUBKY}/pub/pubky.app/files/{file_id}"),
        owner_id: USER_PUBKY.to_string(),
        indexed_at: 0,
        created_at: 0,
        src: format!("pubky://{USER_PUBKY}/pub/pubky.app/blobs/{file_id}"),
        name: IMAGE_BLOB_NAME.to_string(),
        size,
        content_type: "image/png".to_string(),
        urls: FileUrls::new(
            &PathBuf::from(format!("{USER_PUBKY}/{file_id}")),
            &[FileVariant::Feed, FileVariant::Small],
        ),
        metadata: None,
    };
    file.put_to_graph().await.expect("file graph write");
    FileDetails::put_to_index(&[&[USER_PUBKY, file_id]], vec![Some(file.clone())])
        .await
        .expect("file index write");
    Ok(file)
}

async fn remove_image(file: &FileDetails) -> Result<()> {
    file.delete().await.expect("file removal");
    let _ = fs::remove_dir_all(format!("static/files/{}/{}", file.owner_id, file.id));
    Ok(())
}

/// Requests `variant` of a freshly indexed image and returns its status and content type.
async fn serve_variant(file_id: &str, variant: FileVariant) -> Result<(FileDetails, u16, String)> {
    let client = httpc_test::new_client(host_url().await)?;
    let file = index_image(file_id).await?;

    let res = client
        .do_get(&format!("/static/files/{USER_PUBKY}/{file_id}/{variant}"))
        .await;
    remove_image(&file).await?;
    let res = res?;

    let content_type = res.header("content-type").unwrap_or_default();
    Ok((file, res.status().as_u16(), content_type))
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_main() -> Result<()> {
    let (file, status, content_type) = serve_variant("2ZKRACE0MA1N0", FileVariant::Main).await?;

    assert_eq!(status, 200);
    assert_eq!(content_type, file.content_type);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_feed() -> Result<()> {
    let (file, status, content_type) = serve_variant("2ZKRACE0FEED0", FileVariant::Feed).await?;

    assert_eq!(status, 200);
    assert_ne!(content_type, file.content_type);
    assert_eq!(content_type, "image/webp");

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_small() -> Result<()> {
    let (file, status, content_type) = serve_variant("2ZKRACE0SMA10", FileVariant::Small).await?;

    assert_eq!(status, 200);
    assert_ne!(content_type, file.content_type);
    assert_eq!(content_type, "image/webp");

    Ok(())
}
