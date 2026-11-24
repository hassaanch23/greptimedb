// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! One-shot publication of already-admitted shared inputs. Task tracking is external.

pub(crate) mod output;
#[cfg(test)]
mod tests;

use api::v1::SemanticType;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{FileId, RegionId};
use tokio::sync::oneshot;

use crate::engine::MitoEngine;
use crate::error::{
    Error, InvalidRequestSnafu, RecvSnafu, RegionBusySnafu, RegionNotFoundSnafu, Result,
    SerdeJsonSnafu,
};
use crate::manifest::action::{RegionEdit, TableCompactionReceipt};
use crate::region::options::RegionOptions;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::{RegionEditKind, RegionEditRequest, WorkerRequest};
use crate::sst::file::{FileMeta, LEVEL_SHARED, SHARED_LEVEL_SLOT};

/// A query-visible input snapshot. Callers may select a subset of `inputs` for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCompactionSnapshot {
    pub metadata: RegionMetadataRef,
    pub options: serde_json::Value,
    pub inputs: Vec<FileMeta>,
}

/// Transient publication preconditions, never stored in a manifest.
#[derive(Debug)]
pub(crate) struct PublicationContext {
    pub metadata: RegionMetadataRef,
    pub options: RegionOptions,
}

/// A result for this publication attempt, not a durable task receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationOutcome {
    Applied,
    /// The captured inputs/schema/options no longer match. Re-snapshot and replan.
    StaleInputs,
}

/// Whether outputs built under `captured` remain valid after a schema change.
///
/// Sparse keys identify tags by column ID, so adding nullable tags without defaults
/// leaves captured rows unchanged. Existing column definitions, key order, routing,
/// and encoding must remain unchanged. This is directional; it does not authorize
/// rebuilding a plan or changing its captured output metadata.
pub fn is_table_compaction_metadata_compatible(
    captured: &RegionMetadata,
    current: &RegionMetadata,
) -> bool {
    if captured == current {
        return true;
    }
    if captured.region_id != current.region_id
        || captured.primary_key_encoding != PrimaryKeyEncoding::Sparse
        || current.primary_key_encoding != captured.primary_key_encoding
        || captured.partition_expr != current.partition_expr
        || captured.partition_expr_version != current.partition_expr_version
        || current.schema_version <= captured.schema_version
        || current.column_metadatas.len() <= captured.column_metadatas.len()
        || !current.primary_key.starts_with(&captured.primary_key)
    {
        return false;
    }
    if !captured
        .column_metadatas
        .iter()
        .all(|column| current.column_by_id(column.column_id) == Some(column))
    {
        return false;
    }
    let added = current.column_metadatas.len() - captured.column_metadatas.len();
    current.primary_key.len() == captured.primary_key.len() + added
        && current.column_metadatas.iter().all(|column| {
            captured.column_by_id(column.column_id).is_some()
                || (column.semantic_type == SemanticType::Tag
                    && column.column_schema.is_nullable()
                    && column.column_schema.default_constraint().is_none()
                    && current.primary_key_index(column.column_id).is_some())
        })
}

impl MitoEngine {
    /// Installs shared ingestion references using the ingestion cache policy.
    /// Keep import accounting: the same SST is referenced by multiple targets
    /// and must not be counted as fresh writes at every target.
    pub async fn ingest_shared_ssts(&self, region_id: RegionId, edit: RegionEdit) -> Result<()> {
        ensure!(
            edit.files_to_remove.is_empty()
                && edit
                    .files_to_add
                    .iter()
                    .all(|file| file.level == LEVEL_SHARED),
            InvalidRequestSnafu {
                region_id,
                reason: "Shared ingestion requires shared files and cannot remove files"
            }
        );
        self.edit_region_with_kind(
            region_id,
            edit,
            RegionEditKind::Import,
            *super::INGEST_PRELOAD_SST_CACHE,
        )
        .await
    }

    /// Reads a committed version without waiting for manifest work to become idle.
    /// Publication revalidates the selected inputs, metadata and options in the worker.
    pub fn table_compaction_snapshot(
        &self,
        region_id: RegionId,
    ) -> Result<TableCompactionSnapshot> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            matches!(
                region.state(),
                RegionRoleState::Leader(
                    RegionLeaderState::Writable
                        | RegionLeaderState::Editing
                        | RegionLeaderState::Altering
                )
            ) && !region.in_staging_session(),
            RegionBusySnafu { region_id }
        );
        let current = region.version_control.current();
        Ok(TableCompactionSnapshot {
            metadata: current.version.metadata.clone(),
            options: serde_json::to_value(&current.version.options).context(SerdeJsonSnafu)?,
            inputs: current.version.ssts.levels()[SHARED_LEVEL_SLOT]
                .files()
                .map(|file| file.meta_ref().clone())
                .collect(),
        })
    }

    /// Reads committed shared inputs for admission checks, without requiring an idle worker.
    ///
    /// An empty selection reads only metadata and options. Lookups are bounded by the requested
    /// file ids, not the region's file count. Editing/Altering leave the committed version readable;
    /// actual admission still goes through the worker queue and its lifecycle checks.
    /// This is not a compaction planning snapshot or a reservation of the selected inputs.
    pub fn shared_ingest_snapshot(
        &self,
        region_id: RegionId,
        file_ids: &[FileId],
    ) -> Result<TableCompactionSnapshot> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            matches!(
                region.state(),
                RegionRoleState::Leader(
                    RegionLeaderState::Writable
                        | RegionLeaderState::Editing
                        | RegionLeaderState::Altering
                )
            ) && !region.in_staging_session(),
            RegionBusySnafu { region_id }
        );
        let current = region.version_control.current();
        let shared = &current.version.ssts.levels()[SHARED_LEVEL_SLOT];
        Ok(TableCompactionSnapshot {
            metadata: current.version.metadata.clone(),
            options: serde_json::to_value(&current.version.options).context(SerdeJsonSnafu)?,
            inputs: file_ids
                .iter()
                .filter_map(|id| shared.files.get(id))
                .map(|file| file.meta_ref().clone())
                .collect(),
        })
    }

    /// Publishes one region independently, using compaction rather than import sequences.
    ///
    /// Duplicate submissions are allowed. A stale outcome does NOT prove these
    /// outputs were never published (an earlier ACK may have been lost). Never
    /// blindly delete outputs on stale/error: keep their protection until recovery
    /// or manifest-aware GC determines liveness. No input registration is performed.
    /// Input admission must be complete/quiescent; a compaction retry must not replay
    /// imports. The caller must produce a logically equivalent replacement under this snapshot.
    pub async fn publish_table_compaction(
        &self,
        region_id: RegionId,
        snapshot: TableCompactionSnapshot,
        outputs: Vec<FileMeta>,
    ) -> Result<PublicationOutcome> {
        self.publish_table_compaction_with_receipt(region_id, snapshot, outputs, None)
            .await
    }

    /// Reads the persisted receipt even if the job's output files have since been compacted.
    pub async fn table_compaction_receipt(
        &self,
        region_id: RegionId,
        job_id: uuid::Uuid,
    ) -> Result<Option<[u8; 32]>> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let manifest = region.manifest_ctx.manifest().await;
        Ok(manifest.table_compaction_receipts.get(&job_id).copied())
    }

    /// Applies the edit and its optional idempotency receipt in one manifest version.
    pub async fn publish_table_compaction_with_receipt(
        &self,
        region_id: RegionId,
        snapshot: TableCompactionSnapshot,
        outputs: Vec<FileMeta>,
        receipt: Option<TableCompactionReceipt>,
    ) -> Result<PublicationOutcome> {
        ensure!(
            snapshot.metadata.region_id == region_id && !snapshot.inputs.is_empty(),
            InvalidRequestSnafu {
                region_id,
                reason: "Publication requires this region's nonempty input snapshot"
            }
        );
        // The public snapshot keeps JSON options; parse them once here so the
        // worker compares typed options and reads typed flags without reparsing.
        let options = serde_json::from_value(snapshot.options).context(SerdeJsonSnafu)?;
        let edit = RegionEdit {
            table_compaction_receipt: receipt,
            files_to_remove: snapshot.inputs,
            files_to_add: outputs,
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            ..Default::default()
        };
        let (tx, rx) = oneshot::channel();
        let mut request = RegionEditRequest::new(region_id, edit, false, tx);
        request.kind = RegionEditKind::Compaction(Box::new(PublicationContext {
            metadata: snapshot.metadata,
            options,
        }));
        self.inner
            .workers
            .submit_to_worker(region_id, WorkerRequest::EditRegion(request))
            .await?;
        match rx.await.context(RecvSnafu)? {
            Ok(()) => Ok(PublicationOutcome::Applied),
            Err(Error::EditRegion { source, .. })
                if matches!(source.as_ref(), Error::StaleCompactionExecution { .. }) =>
            {
                Ok(PublicationOutcome::StaleInputs)
            }
            Err(error) => Err(error),
        }
    }
}
