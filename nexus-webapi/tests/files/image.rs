use std::{
    fs::{self},
    io::ErrorKind,
    path::PathBuf,
};

use crate::utils::host_url;
use anyhow::Result;
use nexus_common::media::FileVariant;
use nexus_common::models::{file::FileDetails, traits::Collection};
use tokio::fs::create_dir_all;

const IMAGE_BLOB_NAME: &str = "SynonymLogo.png";
const BLOB_PATH: &str = "tests/files/blobs";

const FILE_ID: &str = "2ZKH7K7M9G3G0";
const USER_PUBKY: &str = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

/// Removes a variant's file and failure marker, so a leftover from an earlier failed run can't
/// decide the result.
fn remove_variant_leftovers(file_dir: &str, variant: FileVariant) -> Result<()> {
    for leftover in [
        format!("{file_dir}/{variant}"),
        format!("{file_dir}/{variant}.failed"),
    ] {
        if let Err(e) = fs::remove_file(leftover) {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_main() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    let test_image_dir_path = format!("static/files/{USER_PUBKY}/{FILE_ID}");
    let full_image_path = format!("{}/main", test_image_dir_path.clone());

    // make sure directory exists
    let exists = match fs::metadata(test_image_dir_path.clone()) {
        Err(_) => false,
        Ok(metadata) => metadata.is_dir(),
    };
    if !exists {
        create_dir_all(&test_image_dir_path).await?;
    }
    // copy the image from mocks folder to static folder
    fs::copy(
        PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME),
        &full_image_path,
    )?;

    let files = FileDetails::get_by_ids(vec![vec![USER_PUBKY, FILE_ID].as_slice()].as_slice())
        .await
        .expect("Failed to fetch files from Nexus");

    let result_file = files[0].as_ref().expect("Created file was not found.");

    let test_file_version_name = FileVariant::Main.to_string();
    let test_file_path = format!("static/files/{USER_PUBKY}/{FILE_ID}/{test_file_version_name}");

    let res = client
        .do_get(format!("/{}", test_file_path.as_str()).as_str())
        .await?;

    assert_eq!(res.status(), 200);
    assert_eq!(
        res.header("content-type")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        result_file.content_type
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_feed() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    let test_image_dir_path = format!("static/files/{USER_PUBKY}/{FILE_ID}");
    let full_image_path = format!("{}/main", test_image_dir_path.clone());

    // make sure directory exists
    let exists = match fs::metadata(test_image_dir_path.clone()) {
        Err(_) => false,
        Ok(metadata) => metadata.is_dir(),
    };
    if !exists {
        create_dir_all(&test_image_dir_path).await?;
    }
    // copy the image from mocks folder to static folder
    fs::copy(
        PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME),
        &full_image_path,
    )?;

    let files = FileDetails::get_by_ids(vec![vec![USER_PUBKY, FILE_ID].as_slice()].as_slice())
        .await
        .expect("Failed to fetch files from Nexus");

    let result_file = files[0].as_ref().expect("Created file was not found.");

    remove_variant_leftovers(&test_image_dir_path, FileVariant::Feed)?;

    let test_file_version_name = FileVariant::Feed.to_string();
    let test_file_path = format!("static/files/{USER_PUBKY}/{FILE_ID}/{test_file_version_name}");

    let res = client
        .do_get(format!("/{}", test_file_path.as_str()).as_str())
        .await?;

    assert_eq!(res.status(), 200);
    assert_ne!(
        res.header("content-type")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        result_file.content_type
    );
    assert_eq!(
        res.header("content-type")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        "image/webp"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_serving_small() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    let test_image_dir_path = format!("static/files/{USER_PUBKY}/{FILE_ID}");
    let full_image_path = format!("{}/main", test_image_dir_path.clone());

    // make sure directory exists
    let exists = match fs::metadata(test_image_dir_path.clone()) {
        Err(_) => false,
        Ok(metadata) => metadata.is_dir(),
    };
    if !exists {
        create_dir_all(&test_image_dir_path).await?;
    }
    // copy the image from mocks folder to static folder
    fs::copy(
        PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME),
        &full_image_path,
    )?;

    let files = FileDetails::get_by_ids(vec![vec![USER_PUBKY, FILE_ID].as_slice()].as_slice())
        .await
        .expect("Failed to fetch files from Nexus");

    let result_file = files[0].as_ref().expect("Created file was not found.");

    remove_variant_leftovers(&test_image_dir_path, FileVariant::Small)?;

    let test_file_version_name = FileVariant::Small.to_string();
    let test_file_path = format!("static/files/{USER_PUBKY}/{FILE_ID}/{test_file_version_name}");

    let res = client
        .do_get(format!("/{}", test_file_path.as_str()).as_str())
        .await?;

    assert_eq!(res.status(), 200);
    assert_ne!(
        res.header("content-type")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        result_file.content_type
    );
    assert_eq!(
        res.header("content-type")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        "image/webp"
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_image_variant_failure_is_remembered() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    // Declared as image/jpeg while the bytes are PNG: the decoder is chosen from the leading bytes.
    let owner_id = "sfgetccnq7s3h57a7imf6n7k5fqxus33yg85f1ndhnrnofjdmhjy";
    let file_id = "2ZK2RH7KN91G0";

    let file_dir = PathBuf::from(format!("static/files/{owner_id}/{file_id}"));
    let main_path = file_dir.join("main");
    let small_path = file_dir.join("small");
    let marker_path = file_dir.join("small.failed");
    let small_url = format!("/static/files/{owner_id}/{file_id}/small");
    let remembered_failure_text = "File variant could not be built";

    create_dir_all(&file_dir).await?;
    for leftover in [&small_path, &marker_path] {
        if leftover.exists() {
            fs::remove_file(leftover)?;
        }
    }
    fs::write(&main_path, b"\x89PNG\r\n\x1a\nnot an image")?;

    // The corrupt file fails to convert and the failure is recorded next to the variant.
    let res = client.do_get(&small_url).await?;
    assert_eq!(res.status(), 500);
    let body = res.json_body()?.to_string();
    for detail in ["static/files", "convert", "ImageMagick"] {
        assert!(!body.contains(detail), "{body}");
    }
    assert!(!body.contains(remembered_failure_text), "{body}");
    assert!(marker_path.exists());
    assert!(!small_path.exists());

    // A valid main written straight to disk is not picked up, so no second conversion ran.
    fs::copy(PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME), &main_path)?;
    let res = client.do_get(&small_url).await?;
    assert_eq!(res.status(), 500);
    let body = res.json_body()?.to_string();
    assert!(body.contains(remembered_failure_text), "{body}");
    assert!(!small_path.exists());

    // Without the marker, as after a re-publish or the retry period, the variant is built.
    fs::remove_file(&marker_path)?;
    let res = client.do_get(&small_url).await?;
    assert_eq!(res.status(), 200);
    assert_eq!(res.header("content-type").unwrap(), "image/webp");
    assert!(small_path.exists());

    fs::remove_dir_all(&file_dir)?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_file_and_avatar_not_modified_keep_cache_header() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    // This file is the user's avatar and no other test touches it on disk. Declared as image/jpeg
    // while the bytes are PNG: the decoder is chosen from the leading bytes.
    let owner_id = "4snwyct86m383rsduhw5xgcxpw7c63j3pq8x4ycqikxgik8y64ro";
    let file_id = "003286NSMY490";
    let file_dir = format!("static/files/{owner_id}/{file_id}");

    create_dir_all(&file_dir).await?;
    fs::copy(
        PathBuf::from(BLOB_PATH).join(IMAGE_BLOB_NAME),
        format!("{file_dir}/main"),
    )?;
    remove_variant_leftovers(&file_dir, FileVariant::Small)?;

    let res = client.do_get(&format!("/{file_dir}/main")).await?;
    assert_eq!(res.status(), 200);
    let last_modified = res.header("last-modified").unwrap();

    let res = client
        .reqwest_client()
        .get(format!("{}/{file_dir}/main", host_url().await))
        .header("if-modified-since", last_modified)
        .send()
        .await?;
    assert_eq!(res.status(), 304);
    assert_eq!(res.headers()["cache-control"], "public, max-age=3600");

    let avatar_url = format!("/static/avatar/{owner_id}");
    let res = client.do_get(&avatar_url).await?;
    assert_eq!(res.status(), 200);
    assert_eq!(res.header("content-type").unwrap(), "image/webp");
    assert_eq!(res.header("cache-control").unwrap(), "public, max-age=3600");
    let last_modified = res.header("last-modified").unwrap();

    let res = client
        .reqwest_client()
        .get(format!("{}{avatar_url}", host_url().await))
        .header("if-modified-since", last_modified)
        .send()
        .await?;
    assert_eq!(res.status(), 304);
    assert_eq!(res.headers()["cache-control"], "public, max-age=3600");

    fs::remove_dir_all(&file_dir)?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_file_missing_from_disk_is_not_cached_publicly() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    // Has graph details but no file on disk.
    let owner_id = "sfgetccnq7s3h57a7imf6n7k5fqxus33yg85f1ndhnrnofjdmhjy";
    let file_id = "2ZK1VCJN4YE00";
    let file_dir = format!("static/files/{owner_id}/{file_id}");
    assert!(!PathBuf::from(&file_dir).exists());

    let res = client.do_get(&format!("/{file_dir}/main")).await?;

    assert_eq!(res.status(), 404);
    let cache_control = res.header("cache-control").unwrap_or_default();
    assert!(!cache_control.contains("public"), "{cache_control}");

    Ok(())
}
