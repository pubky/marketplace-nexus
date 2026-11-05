use std::{
    fs::{self, File},
    io::Write,
};

use anyhow::Result;
use nexus_common::models::{file::FileDetails, traits::Collection};
use tokio::fs::create_dir_all;

use crate::utils::host_url;

#[tokio_shared_rt::test(shared)]
async fn test_static_serving() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    let test_file_id = "2ZK2H8P2T5NG0";
    let test_file_user = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

    let files =
        FileDetails::get_by_ids(vec![vec![test_file_user, test_file_id].as_slice()].as_slice())
            .await
            .expect("Failed to fetch files from Nexus");

    let result_file = files[0].as_ref().expect("Created file was not found.");

    let test_file_path = format!("static/files/{test_file_user}/{test_file_id}");
    let test_file_name = "main";

    let full_path = format!("{}/{}", test_file_path.clone(), test_file_name);

    let exists = match fs::metadata(test_file_path.clone()) {
        Err(_) => false,
        Ok(metadata) => metadata.is_dir(),
    };

    if !exists {
        create_dir_all(test_file_path.clone()).await?;
    }

    let mut file = File::create(full_path.as_str())?;
    file.write_all(b"Hello, world!")?;

    let res = client
        .do_get(format!("/{}", full_path.as_str()).as_str())
        .await?;

    assert_eq!(res.status(), 200);
    assert_eq!(
        res.header("content-length")
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        result_file.size
    );

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_serving_dl_param() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;

    let test_file_id = "2ZKH7K7B2RY00";
    let test_file_user = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

    let files =
        FileDetails::get_by_ids(vec![vec![test_file_user, test_file_id].as_slice()].as_slice())
            .await
            .expect("Failed to fetch files from Nexus");

    let result_file = files[0].as_ref().expect("Created file was not found.");

    let test_file_path = format!("static/files/{test_file_user}/{test_file_id}");
    let test_file_name = "main";

    let full_path = format!("{}/{}", test_file_path.clone(), test_file_name);

    let exists = match fs::metadata(test_file_path.clone()) {
        Err(_) => false,
        Ok(metadata) => metadata.is_dir(),
    };

    if !exists {
        create_dir_all(test_file_path.clone()).await?;
    }

    let mut file = File::create(full_path.as_str())?;
    file.write_all(b"Hello, world!")?;

    let res = client
        .do_get(format!("/{}?dl", full_path.as_str()).as_str())
        .await?;

    assert_eq!(res.status(), 200);
    assert_eq!(
        res.header("content-length")
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        result_file.size
    );
    assert_eq!(
        res.header("content-disposition")
            .unwrap()
            .parse::<String>()
            .unwrap(),
        format!("attachment; filename=\"{}\"", result_file.name)
    );

    Ok(())
}

const ACTIVE_FILE_USER: &str = "y4euc58gnmxun9wo87gwmanu6kztt9pgw1zz1yp1azp7trrsjamy";

/// Indexes a file record whose publisher-declared type is `content_type`, with `body` on disk.
async fn index_published_file(
    file_id: &str,
    name: &str,
    content_type: &str,
    body: &[u8],
) -> Result<FileDetails> {
    let dir = format!("static/files/{ACTIVE_FILE_USER}/{file_id}");
    create_dir_all(&dir).await?;
    fs::write(format!("{dir}/main"), body)?;

    let file = FileDetails {
        id: file_id.to_string(),
        uri: format!("pubky://{ACTIVE_FILE_USER}/pub/pubky.app/files/{file_id}"),
        owner_id: ACTIVE_FILE_USER.to_string(),
        indexed_at: 0,
        created_at: 0,
        src: format!("pubky://{ACTIVE_FILE_USER}/pub/pubky.app/blobs/{file_id}"),
        name: name.to_string(),
        size: body.len() as i64,
        content_type: content_type.to_string(),
        urls: nexus_common::models::file::FileUrls {
            main: format!("{ACTIVE_FILE_USER}/{file_id}/main"),
            feed: None,
            small: None,
        },
        metadata: None,
    };
    file.put_to_graph().await.expect("file graph write");
    FileDetails::put_to_index(&[&[ACTIVE_FILE_USER, file_id]], vec![Some(file.clone())])
        .await
        .expect("file index write");
    Ok(file)
}

async fn remove_published_file(file: &FileDetails) -> Result<()> {
    file.delete().await.expect("file removal");
    let _ = fs::remove_dir_all(format!("static/files/{}/{}", file.owner_id, file.id));
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_serving_publisher_html_is_sandboxed_and_not_sniffed() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;
    let file = index_published_file(
        "2ZKSEC0HTML00",
        "page.html",
        "text/html",
        b"<script>fetch('/v0/events')</script>",
    )
    .await?;

    let res = client
        .do_get(&format!(
            "/static/files/{ACTIVE_FILE_USER}/{}/main",
            file.id
        ))
        .await?;
    remove_published_file(&file).await?;

    assert_eq!(res.status(), 200);
    let header = |name: &str| res.header(name).unwrap_or_default();
    assert_eq!(header("x-content-type-options"), "nosniff");
    assert_eq!(
        header("content-security-policy"),
        "default-src 'none'; sandbox"
    );
    assert_eq!(header("content-disposition"), "attachment");
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_serving_publisher_svg_is_sandboxed_with_a_safe_download_name() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;
    let file = index_published_file(
        "2ZKSEC00SVG00",
        "logo\"\u{00e9}.svg",
        "image/svg+xml",
        b"<svg xmlns='http://www.w3.org/2000/svg' onload='alert(1)'/>",
    )
    .await?;

    let res = client
        .do_get(&format!(
            "/static/files/{ACTIVE_FILE_USER}/{}/main?dl",
            file.id
        ))
        .await?;
    remove_published_file(&file).await?;

    assert_eq!(res.status(), 200);
    let header = |name: &str| res.header(name).unwrap_or_default();
    assert_eq!(header("x-content-type-options"), "nosniff");
    assert_eq!(
        header("content-security-policy"),
        "default-src 'none'; sandbox"
    );
    assert_eq!(
        header("content-disposition"),
        "attachment; filename=\"logo__.svg\"; filename*=UTF-8''logo%22%C3%A9.svg"
    );
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_serving_pdf_stays_inline_but_is_not_sniffed() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;
    let file =
        index_published_file("2ZKSEC00PDF00", "doc.pdf", "application/pdf", b"%PDF-1.4").await?;

    let res = client
        .do_get(&format!(
            "/static/files/{ACTIVE_FILE_USER}/{}/main",
            file.id
        ))
        .await?;
    remove_published_file(&file).await?;

    assert_eq!(res.status(), 200);
    assert_eq!(
        res.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );
    assert!(res.header("content-security-policy").is_none());
    assert!(res.header("content-disposition").is_none());
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_static_serving_publisher_empty_type_is_a_sandboxed_download() -> Result<()> {
    let client = httpc_test::new_client(host_url().await)?;
    let file = index_published_file(
        "2ZKSEC000EMPT",
        "page.html",
        "",
        b"<script>alert(1)</script>",
    )
    .await?;

    let res = client
        .do_get(&format!(
            "/static/files/{ACTIVE_FILE_USER}/{}/main",
            file.id
        ))
        .await?;
    remove_published_file(&file).await?;

    assert_eq!(res.status(), 200);
    let header = |name: &str| res.header(name).unwrap_or_default();
    assert_eq!(header("content-type"), "application/octet-stream");
    assert_eq!(header("x-content-type-options"), "nosniff");
    assert_eq!(
        header("content-security-policy"),
        "default-src 'none'; sandbox"
    );
    assert_eq!(header("content-disposition"), "attachment");
    Ok(())
}
