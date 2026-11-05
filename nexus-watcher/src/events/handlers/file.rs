use crate::events::handlers::listing::is_homeserver_not_found;
use crate::events::remote_body::{read_capped, within};
use crate::events::EventProcessorError;

use nexus_common::db::PubkyConnector;
use nexus_common::media::FileVariant;
use nexus_common::media::VariantController;
use nexus_common::models::file::Blob;
use nexus_common::models::{
    file::{FileDetails, FileMeta},
    traits::Collection,
};
use pubky_app_specs::{PubkyAppFile, PubkyAppObject, PubkyId, VALIDATION_LIMITS};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::remove_dir_all;
use tracing::debug;

/// Budget for fetching one file's blob, from request to the last body byte.
const BLOB_FETCH_TIMEOUT: Duration = Duration::from_secs(120);

pub async fn sync_put(
    file: PubkyAppFile,
    uri: String,
    user_id: PubkyId,
    file_id: String,
    files_path: PathBuf,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new file resource at {}/{}", user_id, file_id);

    let Some(file_meta) = ingest(&user_id, file_id.as_str(), &file, files_path).await? else {
        debug!(
            "File blob {} is already gone from its homeserver; skipping",
            file.src
        );
        return Ok(());
    };

    // Create FileDetails object
    let file_details =
        FileDetails::from_homeserver(&file, uri, user_id.to_string(), file_id, file_meta);

    // SAVE TO GRAPH
    file_details.put_to_graph().await?;

    FileDetails::put_to_index(
        &[&[
            file_details.owner_id.clone().as_str(),
            file_details.id.clone().as_str(),
        ]],
        vec![Some(file_details)],
    )
    .await?;

    Ok(())
}

// TODO: Move it into its own process, server, etc
async fn ingest(
    user_id: &PubkyId,
    file_id: &str,
    pubkyapp_file: &PubkyAppFile,
    files_path: PathBuf,
) -> Result<Option<FileMeta>, EventProcessorError> {
    let pubky = PubkyConnector::get()?;
    let fetch = async {
        let response = match pubky.public_storage().get(&pubkyapp_file.src).await {
            Ok(response) => response,
            Err(e) if is_homeserver_not_found(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Refuse an oversized blob as soon as that is known, instead of buffering it first.
        read_capped(
            response,
            VALIDATION_LIMITS.max_blob_size_bytes,
            &format!("File blob {}", pubkyapp_file.src),
        )
        .await
        .map(Some)
    };
    let Some(blob) = within(
        BLOB_FETCH_TIMEOUT,
        &format!("Fetching file blob {}", pubkyapp_file.src),
        fetch,
    )
    .await?
    else {
        return Ok(None);
    };

    let path = Path::new(&user_id.to_string()).join(file_id);
    let full_path = files_path.join(path.clone());
    let pubky_app_object = PubkyAppObject::from_uri(&pubkyapp_file.src, &blob)
        .map_err(EventProcessorError::generic)?;

    match pubky_app_object {
        PubkyAppObject::Blob(blob) => {
            Blob::put_to_static(FileVariant::Main.to_string(), full_path, &blob)
                .await
                .map_err(EventProcessorError::static_save_failed)?;

            let urls = VariantController::get_file_urls_by_content_type(
                pubkyapp_file.content_type.as_str(),
                &path,
            );
            Ok(Some(FileMeta { urls }))
        }
        _ => Err(EventProcessorError::InvalidEventLine(format!(
            "The file has a source uri that is not a blob path: {}",
            pubkyapp_file.src
        ))),
    }
}

pub async fn del(
    user_id: &PubkyId,
    file_id: String,
    files_path: PathBuf,
) -> Result<(), EventProcessorError> {
    debug!("Deleting File resource at {}/{}", user_id, file_id);
    let result = FileDetails::get_by_ids(&[&[user_id, &file_id]]).await?;

    if result.is_empty() {
        return Ok(());
    }

    let file = &result[0];
    if let Some(file_details) = file {
        file_details.delete().await?;
    }

    let folder_path = Path::new(&user_id.to_string()).join(&file_id);
    let full_path = files_path.join(folder_path);

    match remove_dir_all(full_path.as_path()).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
