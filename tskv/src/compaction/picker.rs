use std::fmt::Debug;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use trace::{debug, error, info};

use super::CompactTask;
use crate::compaction::CompactReq;
use crate::tseries_family::{ColumnFile, ColumnFiles, LevelInfo, LevelInfos, Version};
use crate::tsm::TsmTombstoneCache;
use crate::{LevelId, Result};

pub async fn pick_compaction(
    compact_task: CompactTask,
    version: Arc<Version>,
) -> Option<CompactReq> {
    match &compact_task {
        CompactTask::Normal(_) => {
            LevelCompactionPicker
                .pick_compaction(compact_task, version)
                .await
        }
        CompactTask::Delta(_) => {
            DeltaCompactionPicker
                .pick_compaction(compact_task, version)
                .await
        }
        CompactTask::Cold(_) => {
            LevelCompactionPicker
                .pick_compaction(compact_task, version)
                .await
        }
    }
}

/// Compaction picker for picking a level from level-1 to level-4, and then
/// pick inner files of the level.
#[derive(Debug)]
struct LevelCompactionPicker;

impl LevelCompactionPicker {
    async fn pick_compaction(
        &self,
        compact_task: CompactTask,
        version: Arc<Version>,
    ) -> Option<CompactReq> {
        // 1. Get TseriesFamily's newest **version**(`Arc<Version>`)
        // 2. Get all level's score, pick LevelInfo with the max score.
        // 3. Get files(`Vec<Arc<ColumnFile>>`) from the picked level, sorted by min_ts(ascending)
        //    and size(ascending), pick ColumnFile until picking_files_size reaches
        //    max_compact_size, remove away the last picked files with overlapped time_range.
        // 4. (Deprecated and skipped): Pick files from level-0.
        // 5. Build CompactReq using **version**, picked level and picked files.

        debug!(
            "Picker(level): version: [ {} ]",
            LevelInfos(version.levels_info())
        );

        let storage_opt = version.storage_opt();
        let level_infos = version.levels_info();

        // Pick a level to compact
        let level_start: &LevelInfo;
        let in_level;
        let out_level;
        if let Some((start_lvl, out_lvl)) = self.pick_level(level_infos).await {
            level_start = &level_infos[start_lvl as usize];
            in_level = start_lvl;
            out_level = out_lvl;
        } else {
            info!("Picker(level): picked no level");
            return None;
        }

        // Pick selected level files.
        if level_start.files.is_empty() {
            return None;
        }

        let mut files = level_start.files.clone();
        files.sort_by(Self::compare_column_file);
        let picking_files: Vec<Arc<ColumnFile>> =
            Self::pick_files(files, storage_opt.max_compact_size).await;
        info!(
            "Picker(level): Picked files: [ {} ]",
            ColumnFiles(&picking_files)
        );
        if picking_files.is_empty() {
            return None;
        }

        // Run compaction and send them to the next level, even if picked only 1 file,.
        Some(CompactReq {
            compact_task,
            version,
            files: picking_files,
            in_level,
            out_level,
        })
    }

    /// Weight of file number of a level to be picked.
    fn level_weight_file_num(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 0.8,
            2 => 0.4,
            3 => 0.2,
            4 => 0.1,
            _ => 0.0,
        }
    }

    /// Weight of the ramaining size of a level to be picked.
    fn level_weight_remaining_size(level: LevelId) -> f64 {
        match level {
            0 => 1.0,
            1 => 1.0,
            2 => 1.0,
            3 => 1.0,
            4 => 1.0,
            _ => 0.0,
        }
    }

    fn compare_column_file(a: &Arc<ColumnFile>, b: &Arc<ColumnFile>) -> std::cmp::Ordering {
        match a.time_range().min_ts.cmp(&b.time_range().min_ts) {
            std::cmp::Ordering::Equal => a.size().cmp(&b.size()),
            ord => ord,
        }
    }

    async fn pick_level(&self, levels: &[LevelInfo]) -> Option<(LevelId, LevelId)> {
        // - Level max_size (level closer to max_size
        //     has more possibility to run compact)
        //   - (level.max_size - level.cur_size) as numerator
        // - Level running compactions (level running compaction
        //     has less possibility to run compact)
        //   - compacting_files as deniminator
        // - Level weight (higher level that file is too large
        //     has less possibility to run compact)
        //   - level_weight as numerator
        // - Level file_count (after all, level has more files
        //     has more possibility to run compact)
        //   - level.files.len() as numerator

        if levels.is_empty() {
            return None;
        }

        // Level score context: Vec<(level, level_size, compacting_files in level, level_weight, level_score)>
        let mut level_scores: Vec<(LevelId, u64, usize, f64, f64)> =
            Vec::with_capacity(levels.len());
        for lvl in levels.iter() {
            // Ignore level 0 (delta files)
            if lvl.level == 0 || lvl.cur_size == 0 || lvl.files.len() <= 1 {
                continue;
            }
            let mut compacting_files = 0_usize;
            for file in lvl.files.iter() {
                if file.is_compacting().await {
                    compacting_files += 1;
                }
            }

            // let level_file_num_weight = (lvl.files.len() - compacting_files) as f64
            //     * Self::level_weight_file_num(lvl.level);
            // let level_remaining_size_weight = lvl.max_size.checked_sub(lvl.cur_size).unwrap_or(1)
            //     as f64
            //     * Self::level_weight_remaining_size(lvl.level);
            // let level_score = 10e6 * (level_file_num_weight / level_remaining_size_weight);

            let level_score: f64 = (lvl.files.len() - compacting_files) as f64
                * Self::level_weight_file_num(lvl.level);

            level_scores.push((lvl.level, lvl.cur_size, compacting_files, 0.0, level_score));
        }

        if level_scores.is_empty() {
            return None;
        }
        level_scores.sort_by(|(_, _, _, _, score_a), (_, _, _, _, score_b)| {
            score_a.partial_cmp(score_b).expect("a NaN score").reverse()
        });

        debug!(
            "Picker(level), level scores: [ {} ]",
            level_scores
                .iter()
                .map(|lc| format!("{{ Level-{}: {} }}", lc.0, lc.4))
                .collect::<Vec<String>>()
                .join(", ")
        );

        level_scores.first().cloned().map(|(level, _, _, _, _)| {
            if level == 4 {
                (level, level)
            } else {
                (level, level + 1)
            }
        })
    }

    async fn pick_files(
        src_files: Vec<Arc<ColumnFile>>,
        max_compact_size: u64,
    ) -> Vec<Arc<ColumnFile>> {
        let mut dst_files = Vec::with_capacity(src_files.len());

        let mut picking_file_size = 0_u64;
        for file in src_files.iter() {
            if !file.mark_compacting().await {
                continue;
            }
            picking_file_size += file.size();
            dst_files.push(file.clone());

            if picking_file_size >= max_compact_size {
                // Picked file size >= max_compact_size, try break picking files.
                break;
            }
        }

        dst_files
    }
}

/// For the given time_range of a file, return recommended level to compact in.
/// If it returns 0, it means there is no files overlapped with the given time range.
fn adviced_out_level(time_range: &TimeRange, levels: &[LevelInfo; 5]) -> LevelId {
    if time_range.min_ts > levels[1].time_range.max_ts || levels[1].time_range.is_none() {
        // If lv-1 is (+∞，-∞), compact to lv-1.
        // If thte range is newer than level-1, return 0 to tell the caller that
        // there is no other level overlapped with the range.
        return 0;
    }
    if time_range.min_ts > levels[1].time_range.min_ts {
        // If the range is overlapped with levle-1, compact with level-1 files.
        return 1;
    }
    if time_range.max_ts <= levels[1].time_range.min_ts
        && time_range.min_ts >= levels[3].time_range.max_ts
        || levels[2].time_range.is_none()
    {
        return 2;
    }
    if time_range.max_ts <= levels[2].time_range.min_ts
        && time_range.min_ts >= levels[4].time_range.max_ts
        || levels[3].time_range.is_none()
    {
        return 3;
    }
    if time_range.max_ts <= levels[3].time_range.min_ts || levels[4].time_range.is_none() {
        return 4;
    }
    // it should never happen
    1
}

#[derive(Debug)]
struct DeltaCompactionPicker;

// todo: get file timerange after remove tombstone file
impl DeltaCompactionPicker {
    async fn delta_file_last_remained_time_range(file: &ColumnFile) -> Result<TimeRange> {
        let tomb_path = file.tombstone_path();
        let tomb_trs = match TsmTombstoneCache::load(tomb_path).await? {
            Some(tomb_cache) => tomb_cache.all_excluded().clone(),
            None => return Ok(*file.time_range()),
        };
        match file.time_range().exclude_time_ranges(&tomb_trs) {
            Some(trs) => {
                if let Some(last_tr) = trs.time_ranges().last() {
                    Ok(last_tr)
                } else {
                    Ok(TimeRange::none())
                }
            }
            None => Ok(TimeRange::none()),
        }
    }

    async fn pick_compaction(
        &self,
        compact_task: CompactTask,
        version: Arc<Version>,
    ) -> Option<CompactReq> {
        let pick_timestamp = chrono::Utc::now().timestamp_nanos();
        debug!(
            "Picker(delta) [{pick_timestamp}]: version: [ {} ]",
            LevelInfos(version.levels_info())
        );

        let lv0 = &version.levels_info()[0];
        let lv14 = &version.levels_info()[1..];
        let mut picked_time_range = TimeRange::none();
        let mut picked_compacting_wlocks = Vec::new();
        let mut picked_files = vec![];
        for l0_file in lv0.files.iter() {
            let mut l0_file_compacting = l0_file.write_lock_compacting().await;
            if *l0_file_compacting {
                continue;
            }

            let l0_file_remained_tr_last =
                match Self::delta_file_last_remained_time_range(l0_file).await {
                    Ok(trs) => trs,
                    Err(e) => {
                        let path = l0_file.tombstone_path();
                        error!(
                        "Picker(delta) [{pick_timestamp}]: failed to load tombstone file '{}': {e}",
                        path.display()
                    );
                        continue;
                    }
                };

            let adviced_out_level =
                adviced_out_level(&l0_file_remained_tr_last, version.levels_info());
            if picked_time_range.is_none() {
                // First cycle to pick l0_file
                picked_time_range = l0_file_remained_tr_last;
            }
            // For well ordered lv0-files, merged to level 1
            if 0 == adviced_out_level {
                *l0_file_compacting = true;
                picked_compacting_wlocks.push(l0_file_compacting);
                picked_files.push(l0_file.clone());
                picked_time_range.merge(l0_file.time_range());
                if picked_files.len() >= version.storage_opt.compact_trigger_file_num as usize {
                    info!(
                        "Picker(delta) [{pick_timestamp}]: picked level_0 files({})",
                        ColumnFiles(&picked_files)
                    );
                    return Some(CompactReq {
                        compact_task,
                        version: version.clone(),
                        files: picked_files,
                        in_level: 0,
                        out_level: 1,
                    });
                }
                continue;
            }

            // Find the first file in level1-4 that overlaps with lv0-file
            for lv in lv14 {
                if lv.time_range.overlaps(&picked_time_range) {
                    for lv_file in lv.files.iter() {
                        let mut lv_file_compacting = lv_file.write_lock_compacting().await;
                        if *lv_file_compacting {
                            continue;
                        }
                        if lv_file.time_range().overlaps(&picked_time_range) {
                            *lv_file_compacting = true;
                            *l0_file_compacting = true;
                            info!(
                                "Picker(delta) [{pick_timestamp}]: picked two level files: level_0 files({}), level_0 file: {l0_file}, level file: {lv_file}",
                                ColumnFiles(&picked_files)
                            );
                            return Some(CompactReq {
                                compact_task,
                                version: version.clone(),
                                files: vec![l0_file.clone(), lv_file.clone()],
                                in_level: 0,
                                out_level: lv.level(),
                            });
                        }
                    }
                }
            }

            // No file in level1-4 overlaps with lv0-file, compact lv0-file to adviced out-level.
            if adviced_out_level > 1 {
                *l0_file_compacting = true;
                info!(
                    "Picker(delta) [{pick_timestamp}]: picked level_0 files({}), level_0 file: {l0_file}",
                    ColumnFiles(&picked_files)
                );
                return Some(CompactReq {
                    compact_task,
                    version: version.clone(),
                    files: vec![l0_file.clone()],
                    in_level: 0,
                    out_level: adviced_out_level,
                });
            }
        }

        for mut wlock in picked_compacting_wlocks {
            *wlock = false;
        }

        info!(
            "Picker(delta) [{pick_timestamp}]: picked nothing, level_0 files({})",
            ColumnFiles(&picked_files)
        );
        None
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use models::predicate::domain::TimeRange;

    use super::adviced_out_level;
    use crate::compaction::picker::{DeltaCompactionPicker, LevelCompactionPicker};
    use crate::compaction::test::create_options;
    use crate::compaction::CompactTask;
    use crate::file_utils::{make_delta_file_name, make_tsm_file_name};
    use crate::kv_option::StorageOptions;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::test::{write_to_tsm, write_to_tsm_tombstone};
    use crate::tsm::{TsmTombstoneCache, TOMBSTONE_FILE_SUFFIX};

    /// The sketch of a version of a vnode.
    #[derive(Debug)]
    pub struct VersionSketch {
        pub id: u32,
        pub dir: PathBuf,
        pub tenant_database: Arc<String>,
        pub levels: [LevelSketch; 5],
        pub tombstone_map: HashMap<u64, TimeRange>,
        pub max_level_ts: i64,
    }

    impl VersionSketch {
        fn new<P: AsRef<Path>>(dir: P, tenant_database: Arc<String>, vnode_id: u32) -> Self {
            let levels = [
                LevelSketch(0, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(1, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(2, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(3, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(4, (i64::MAX, i64::MIN), vec![]),
            ];
            Self {
                id: vnode_id,
                dir: dir.as_ref().to_path_buf(),
                tenant_database,
                levels,
                tombstone_map: HashMap::new(),
                max_level_ts: i64::MIN,
            }
        }

        fn add(mut self, level: usize, file: FileSketch) -> Self {
            self.max_level_ts = self.max_level_ts.max(file.1 .1);
            let level_sketch = &mut self.levels[level];
            level_sketch.1 .0 = level_sketch.1 .0.min(file.1 .0);
            level_sketch.1 .1 = level_sketch.1 .1.max(file.1 .1);
            level_sketch.2.push(file);
            self
        }

        fn add_t(mut self, level: usize, file: FileSketch, tomb_all_excluded: (i64, i64)) -> Self {
            self.tombstone_map.insert(file.0, tomb_all_excluded.into());
            self = self.add(level, file);
            self
        }

        async fn to_version(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let mut level_infos =
                LevelInfo::init_levels(self.tenant_database.clone(), self.id, storage_opt.clone());
            for (level, level_sketch) in self.levels.iter().enumerate() {
                let level_dir = if level == 0 {
                    storage_opt.delta_dir(self.tenant_database.as_str(), self.id)
                } else {
                    storage_opt.tsm_dir(self.tenant_database.as_str(), self.id)
                };
                level_sketch
                    .to_level_info(&mut level_infos[level], &level_dir, level as u32)
                    .await;
            }

            Version::new(
                self.id,
                self.tenant_database.clone(),
                storage_opt,
                1,
                level_infos,
                self.max_level_ts,
                Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
            )
        }

        async fn to_version_with_tsm(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let version = self.to_version(storage_opt).await;
            self.make_tsm_files(&version).await;
            version
        }

        async fn make_tsm_files(&self, version: &Version) {
            let tsm_dir = version
                .storage_opt()
                .tsm_dir(self.tenant_database.as_str(), self.id);
            let delta_dir = version
                .storage_opt()
                .delta_dir(self.tenant_database.as_str(), self.id);
            let _ = std::fs::remove_dir_all(&tsm_dir);
            let _ = std::fs::remove_dir_all(&delta_dir);

            let tsm_data = &HashMap::new();
            for level_sketch in self.levels.iter() {
                for file_sketch in level_sketch.2.iter() {
                    let tsm_path = if level_sketch.0 == 0 {
                        make_delta_file_name(&delta_dir, file_sketch.0)
                    } else {
                        make_tsm_file_name(&tsm_dir, file_sketch.0)
                    };
                    write_to_tsm(&tsm_path, tsm_data, false).await.unwrap();

                    if let Some(tr) = self.tombstone_map.get(&file_sketch.0) {
                        let tombstone_path = tsm_path.with_extension(TOMBSTONE_FILE_SUFFIX);
                        let tomb = TsmTombstoneCache::with_all_excluded(*tr);
                        write_to_tsm_tombstone(tombstone_path, &tomb).await;
                    }
                }
            }
        }
    }

    /// The sketch of a level, contains a tuple of
    /// `level, (min_ts, max_ts), files`.
    #[derive(Debug, Clone)]
    pub struct LevelSketch(pub u32, pub (i64, i64), pub Vec<FileSketch>);

    impl LevelSketch {
        async fn to_level_info(
            &self,
            level_info: &mut LevelInfo,
            level_dir: impl AsRef<Path>,
            level: u32,
        ) {
            let mut level_cur_size = 0_u64;
            let mut files = Vec::with_capacity(self.2.len());
            for file_sketch in self.2.iter() {
                level_cur_size += file_sketch.2;
                let file = file_sketch.to_column_file(&level_dir, level).await;
                files.push(Arc::new(file));
            }
            level_info.files = files;
            level_info.cur_size = level_cur_size;
            level_info.time_range = self.1.into();
        }
    }

    /// The sketch of column file, contains `a tuple of
    /// file_id, (min_ts, max_ts), size, being_compact`.
    #[derive(Debug, Clone)]
    pub struct FileSketch(pub u64, pub (i64, i64), pub u64, pub bool);

    impl FileSketch {
        async fn to_column_file(&self, file_dir: impl AsRef<Path>, level: u32) -> ColumnFile {
            let path = if level == 0 {
                make_delta_file_name(file_dir, self.0)
            } else {
                make_tsm_file_name(file_dir, self.0)
            };
            let col = ColumnFile::new(self.0, level, self.1.into(), self.2, path);
            if self.3 {
                col.mark_compacting().await;
            }
            col
        }
    }

    #[tokio::test]
    async fn test_generate_version() {
        let dir = "/tmp/test/pick/test_generate_version";
        let storage_opt = create_options(dir.to_string()).storage.clone();
        let vnode_sketch = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(6, (1, 10), 50, false))
            .add(0, FileSketch(7, (790, 800), 50, false))
            .add(1, FileSketch(1, (701, 800), 100, false))
            .add(1, FileSketch(2, (601, 700), 100, false))
            .add(2, FileSketch(3, (501, 600), 200, false))
            .add(3, FileSketch(4, (301, 500), 300, false))
            .add(4, FileSketch(5, (1, 300), 400, false));
        assert_eq!(vnode_sketch.id, 1);
        assert_eq!(vnode_sketch.tenant_database.as_str(), "dba");
        assert_eq!(vnode_sketch.max_level_ts, 800);
        let levels_sketch = vnode_sketch.levels.clone();
        assert_eq!(levels_sketch[0].0, 0);
        assert_eq!(levels_sketch[0].1, (1, 800));
        assert_eq!(levels_sketch[0].2.len(), 2);
        assert_eq!(levels_sketch[1].0, 1);
        assert_eq!(levels_sketch[1].1, (601, 800));
        assert_eq!(levels_sketch[1].2.len(), 2);
        assert_eq!(levels_sketch[2].0, 2);
        assert_eq!(levels_sketch[2].1, (501, 600));
        assert_eq!(levels_sketch[2].2.len(), 1);
        assert_eq!(levels_sketch[3].0, 3);
        assert_eq!(levels_sketch[3].1, (301, 500));
        assert_eq!(levels_sketch[3].2.len(), 1);
        assert_eq!(levels_sketch[4].0, 4);
        assert_eq!(levels_sketch[4].1, (1, 300));
        assert_eq!(levels_sketch[4].2.len(), 1);

        let version = vnode_sketch.to_version(storage_opt.clone()).await;
        assert_eq!(version.tf_id(), 1);
        assert_eq!(version.database().as_str(), "dba");
        assert_eq!(version.levels_info().len(), levels_sketch.len());
        let tsm_dir = storage_opt.tsm_dir("dba", 1);
        let delta_dir = storage_opt.delta_dir("dba", 1);
        for (version_level, level_sketch) in version.levels_info().iter().zip(levels_sketch.iter())
        {
            assert_eq!(version_level.database.as_str(), "dba");
            assert_eq!(version_level.tsf_id, 1);
            assert_eq!(version_level.level, level_sketch.0);
            assert_eq!(
                version_level.cur_size,
                level_sketch.2.iter().map(|f| f.2).sum::<u64>()
            );
            assert_eq!(version_level.time_range, level_sketch.1.into());
            assert_eq!(version_level.files.len(), level_sketch.2.len());
            for (version_file, file_sketch) in version_level.files.iter().zip(level_sketch.2.iter())
            {
                assert_eq!(version_file.file_id(), file_sketch.0);
                assert_eq!(version_file.level(), level_sketch.0);
                assert_eq!(version_file.time_range(), &(file_sketch.1.into()));
                assert_eq!(version_file.size(), file_sketch.2);
                assert_eq!(version_file.is_compacting().await, file_sketch.3);
                if level_sketch.0 == 0 {
                    assert_eq!(
                        version_file.file_path(),
                        &make_delta_file_name(&delta_dir, file_sketch.0)
                    );
                } else {
                    assert_eq!(
                        version_file.file_path(),
                        &make_tsm_file_name(&tsm_dir, file_sketch.0)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_pick_normal_compaction() {
        let dir = "/tmp/test/pick/normal_compaction";
        // Some files in Level 1 will be picked and compact to Level 2.
        let opt = create_options(dir.to_string());

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(11, (1, 1000), 1000, false))
            .add(0, FileSketch(12, (33010, 34000), 1000, false))
            .add(1, FileSketch(7, (34001, 35000), 1000, false))
            .add(1, FileSketch(8, (35001, 36000), 1000, false))
            .add(1, FileSketch(9, (34501, 35500), 1000, true))
            .add(1, FileSketch(10, (35001, 36000), 1000, true))
            .add(2, FileSketch(5, (30001, 32000), 1000, false))
            .add(2, FileSketch(6, (32001, 34000), 1000, false))
            .add(3, FileSketch(3, (20001, 25000), 1000, false))
            .add(3, FileSketch(4, (25001, 30000), 1000, false))
            .add(4, FileSketch(1, (1, 10000), 1000, false))
            .add(4, FileSketch(2, (10001, 20000), 1000, false))
            .to_version(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Normal(0);
        let compact_req = LevelCompactionPicker
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        assert_eq!(compact_req.files.len(), 2);
        assert_eq!(compact_req.out_level, 2);
    }

    // Test picker for delta compaction that tsm files overlaps with some delta files.
    #[tokio::test]
    async fn test_pick_delta_compaction_with_tsm() {
        let dir = "/tmp/test/pick/delta_compaction_with_tsm";
        let opt = create_options(dir.to_string());

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add_t(0, FileSketch(11, (1, 600), 100, false), (401, 500)) // 3. Overlaps with lv2#6, picked.
            .add(0, FileSketch(12, (100, 600), 10, false)) // 4. Overlaps with lv2-6, picked.
            .add_t(0, FileSketch(13, (301, 500), 10, false), (401, 500)) // 5. Not overlaps with lv2#6 because of tombstone, picker stops.
            .add(0, FileSketch(14, (1, 500), 10, false))
            .add(1, FileSketch(7, (601, 650), 100, false))
            .add(1, FileSketch(8, (651, 700), 100, false))
            .add(1, FileSketch(9, (701, 750), 100, false))
            .add(1, FileSketch(10, (751, 800), 100, false))
            .add(2, FileSketch(5, (401, 500), 200, false)) // 1. Not overlaps with lv0#11 because of tombstone, continue picker
            .add(2, FileSketch(6, (501, 600), 200, false)) // 2. Overlaps with lv0#11 because of tombstone, picked.
            .add(3, FileSketch(3, (201, 300), 300, false))
            .add(3, FileSketch(4, (301, 400), 300, false))
            .add(4, FileSketch(1, (1, 100), 400, false))
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_some());
        assert_eq!(lv14_file.unwrap().file_id(), 6);
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 11);
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.out_time_range(), (501, 600).into());
    }

    #[tokio::test]
    async fn test_adviced_out_level() {
        let dir = "/tmp/test/pick/test_adviced_out_level";
        let opt = create_options(dir.to_string());

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(1, FileSketch(1, (701, 800), 100, false)) // Level#1, 701~800
            .add(2, FileSketch(2, (401, 600), 200, false)) // Level#2, 401~600
            .add(3, FileSketch(3, (201, 400), 300, false)) // level#3, 201, 400
            .add(4, FileSketch(4, (1, 200), 400, false)) // level#4, 1~200
            .to_version_with_tsm(opt.storage.clone())
            .await;
        let levels = version.levels_info();

        assert_eq!(adviced_out_level(&TimeRange::new(801, 802), levels), 0);
        assert_eq!(adviced_out_level(&TimeRange::new(800, 801), levels), 1);
        assert_eq!(adviced_out_level(&TimeRange::new(700, 701), levels), 2);
        assert_eq!(adviced_out_level(&TimeRange::new(601, 602), levels), 2);
        assert_eq!(adviced_out_level(&TimeRange::new(-1, 0), levels), 4);
    }

    // Test picker for delta compaction that tsm files doesn't overlap with any delta file.
    #[tokio::test]
    async fn test_pick_delta_compaction_without_tsm_1() {
        let dir = "/tmp/test/pick/delta_compaction_without_tsm_1";
        let opt = create_options(dir.to_string());

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(9, (601, 650), 10, false)) // Not overlaps with any of lv1-4.
            .add(0, FileSketch(10, (651, 700), 10, false)) // Not overlaps with any of lv1-4.
            .add(1, FileSketch(7, (701, 750), 100, false)) // Level#1, 701~800
            .add(1, FileSketch(8, (751, 800), 100, false))
            .add(2, FileSketch(5, (401, 500), 200, false)) // Level#2, 401~600
            .add(2, FileSketch(6, (501, 600), 200, false))
            .add(3, FileSketch(3, (201, 300), 300, false)) // level#3, 201, 400
            .add(3, FileSketch(4, (301, 400), 300, false))
            .add(4, FileSketch(1, (1, 100), 400, false)) // level#4, 1~200
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 9);
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.out_time_range(), (401, 600).into());
    }

    // Test picker for delta compaction that tsm files doesn't overlap with any delta file.
    #[tokio::test]
    async fn test_pick_delta_compaction_without_tsm_2() {
        let dir = "/tmp/test/pick/delta_compaction_without_tsm_2";
        let opt = create_options(dir.to_string());

        let version = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(9, (601, 650), 100, false)) // Not overlaps with any lv1-4 files.
            .add(0, FileSketch(10, (-100, -1), 10, false)) // Not overlaps with any lv1-4 files.
            .add(0, FileSketch(11, (-100, -1), 10, false)) // Not overlaps with any lv1-4 files.
            .add(1, FileSketch(7, (701, 750), 100, false)) // Level#1, 701~800
            .add(1, FileSketch(8, (751, 800), 100, false))
            .add(2, FileSketch(5, (401, 500), 200, false)) // Level#2, 401~600
            .add(2, FileSketch(6, (501, 600), 200, false))
            .add(3, FileSketch(3, (201, 300), 300, false)) // level#3, 201, 400
            .add(3, FileSketch(4, (301, 400), 300, false))
            .add(4, FileSketch(1, (1, 100), 400, false)) // level#4, 1~200
            .add(4, FileSketch(2, (101, 200), 400, false))
            .to_version_with_tsm(opt.storage.clone())
            .await;

        let compact_task = CompactTask::Delta(0);
        let compact_req = DeltaCompactionPicker
            .pick_compaction(compact_task, Arc::new(version))
            .await
            .unwrap();
        let (lv0_files, lv14_file) = compact_req.split_delta_and_level_files();
        assert!(lv14_file.is_none());
        assert_eq!(lv0_files.len(), 1);
        assert_eq!(lv0_files[0].file_id(), 9);
        assert_eq!(compact_req.out_level, 2);
        assert_eq!(compact_req.out_time_range(), (401, 600).into());
    }
}
