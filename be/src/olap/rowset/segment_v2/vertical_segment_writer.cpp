// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/rowset/segment_v2/vertical_segment_writer.h"

#include <gen_cpp/segment_v2.pb.h>
#include <parallel_hashmap/phmap.h>

#include <cassert>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "inverted_index_fs_directory.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/base_tablet.h"
#include "olap/data_dir.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/primary_key_index.h"
#include "olap/row_cursor.h"                   // RowCursor // IWYU pragma: keep
#include "olap/rowset/rowset_writer_context.h" // RowsetWriterContext
#include "olap/rowset/segment_creator.h"
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/segment_loader.h"
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "service/point_query_executor.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/key_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/io/reader_buffer.h"
#include "vec/jsonb/serialize.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

using namespace ErrorCode;

static const char* k_segment_magic = "D0R1";
static const uint32_t k_segment_magic_length = 4;

inline std::string vertical_segment_writer_mem_tracker_name(uint32_t segment_id) {
    return "VerticalSegmentWriter:Segment-" + std::to_string(segment_id);
}

VerticalSegmentWriter::VerticalSegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                                             TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                                             DataDir* data_dir,
                                             const VerticalSegmentWriterOptions& opts,
                                             IndexFileWriter* index_file_writer)
        : _segment_id(segment_id),
          _tablet_schema(std::move(tablet_schema)),
          _tablet(std::move(tablet)),
          _data_dir(data_dir),
          _opts(opts),
          _file_writer(file_writer),
          _index_file_writer(index_file_writer),
          _mem_tracker(std::make_unique<MemTracker>(
                  vertical_segment_writer_mem_tracker_name(segment_id))),
          _mow_context(std::move(opts.mow_ctx)),
          _block_aggregator(*this) {
    CHECK_NOTNULL(file_writer);
    _num_sort_key_columns = _tablet_schema->num_key_columns();
    _num_short_key_columns = _tablet_schema->num_short_key_columns();
    if (!_is_mow_with_cluster_key()) {
        DCHECK(_num_sort_key_columns >= _num_short_key_columns)
                << ", table_id=" << _tablet_schema->table_id()
                << ", num_key_columns=" << _num_sort_key_columns
                << ", num_short_key_columns=" << _num_short_key_columns
                << ", cluster_key_columns=" << _tablet_schema->cluster_key_uids().size();
    }
    for (size_t cid = 0; cid < _num_sort_key_columns; ++cid) {
        const auto& column = _tablet_schema->column(cid);
        _key_coders.push_back(get_key_coder(column.type()));
        _key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
    }
    // encode the sequence id into the primary key index
    if (_is_mow()) {
        if (_tablet_schema->has_sequence_col()) {
            const auto& column = _tablet_schema->column(_tablet_schema->sequence_col_idx());
            _seq_coder = get_key_coder(column.type());
        }
        // encode the rowid into the primary key index
        if (_is_mow_with_cluster_key()) {
            const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
            _rowid_coder = get_key_coder(type_info->type());
            // primary keys
            _primary_key_coders.swap(_key_coders);
            // cluster keys
            _key_coders.clear();
            _key_index_size.clear();
            _num_sort_key_columns = _tablet_schema->cluster_key_uids().size();
            for (auto cid : _tablet_schema->cluster_key_uids()) {
                const auto& column = _tablet_schema->column_by_uid(cid);
                _key_coders.push_back(get_key_coder(column.type()));
                _key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
            }
        }
    }
}

VerticalSegmentWriter::~VerticalSegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
}

void VerticalSegmentWriter::_init_column_meta(ColumnMetaPB* meta, uint32_t column_id,
                                              const TabletColumn& column) {
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(cast_set<int32_t>(column.length()));
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i));
    }
    // add sparse column to footer
    for (uint32_t i = 0; i < column.num_sparse_columns(); i++) {
        _init_column_meta(meta->add_sparse_columns(), -1, column.sparse_column_at(i));
    }
    meta->set_result_is_nullable(column.get_result_is_nullable());
    meta->set_function_name(column.get_aggregation_name());
    meta->set_be_exec_version(column.get_be_exec_version());
}

Status VerticalSegmentWriter::_create_column_writer(uint32_t cid, const TabletColumn& column,
                                                    const TabletSchemaSPtr& tablet_schema) {
    ColumnWriterOptions opts;
    opts.meta = _footer.add_columns();

    _init_column_meta(opts.meta, cid, column);

    // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
    // except for columns whose type don't support zone map.
    opts.need_zone_map = column.is_key() || tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opts.need_bloom_filter = column.is_bf_column();
    if (opts.need_bloom_filter) {
        opts.bf_options.fpp =
                tablet_schema->has_bf_fpp() ? tablet_schema->bloom_filter_fpp() : 0.05;
    }
    auto* tablet_index = tablet_schema->get_ngram_bf_index(column.unique_id());
    if (tablet_index) {
        opts.need_bloom_filter = true;
        opts.is_ngram_bf_index = true;
        //narrow convert from int32_t to uint8_t and uint16_t which is dangerous
        auto gram_size = tablet_index->get_gram_size();
        auto gram_bf_size = tablet_index->get_gram_bf_size();
        if (gram_size > 256 || gram_size < 1) {
            return Status::NotSupported("Do not support ngram bloom filter for ngram_size: ",
                                        gram_size);
        }
        if (gram_bf_size > 65535 || gram_bf_size < 64) {
            return Status::NotSupported("Do not support ngram bloom filter for bf_size: ",
                                        gram_bf_size);
        }
        opts.gram_size = cast_set<uint8_t>(gram_size);
        opts.gram_bf_size = cast_set<uint16_t>(gram_bf_size);
    }

    opts.need_bitmap_index = column.has_bitmap_index();
    bool skip_inverted_index = false;
    if (_opts.rowset_ctx != nullptr) {
        // skip write inverted index for index compaction column
        skip_inverted_index =
                _opts.rowset_ctx->columns_to_do_index_compaction.contains(column.unique_id());
    }
    // skip write inverted index on load if skip_write_index_on_load is true
    if (_opts.write_type == DataWriteType::TYPE_DIRECT &&
        tablet_schema->skip_write_index_on_load()) {
        skip_inverted_index = true;
    }
    if (const auto& index = tablet_schema->inverted_index(column);
        index != nullptr && !skip_inverted_index) {
        opts.inverted_index = index;
        opts.need_inverted_index = true;
        DCHECK(_index_file_writer != nullptr);
        opts.index_file_writer = _index_file_writer;
        // TODO support multiple inverted index
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE, type_name)          \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opts.need_zone_map = false;                           \
        opts.need_bloom_filter = false;                       \
        opts.need_bitmap_index = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(STRUCT, "struct")
    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY, "array")
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB, "jsonb")
    DISABLE_INDEX_IF_FIELD_TYPE(AGG_STATE, "agg_state")
    DISABLE_INDEX_IF_FIELD_TYPE(MAP, "map")
    DISABLE_INDEX_IF_FIELD_TYPE(BITMAP, "object")
    DISABLE_INDEX_IF_FIELD_TYPE(HLL, "hll")
    DISABLE_INDEX_IF_FIELD_TYPE(QUANTILE_STATE, "quantile_state")
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT, "variant")

#undef DISABLE_INDEX_IF_FIELD_TYPE

#undef CHECK_FIELD_TYPE

    int64_t storage_page_size = _tablet_schema->storage_page_size();
    // storage_page_size must be between 4KB and 10MB.
    if (storage_page_size >= 4096 && storage_page_size <= 10485760) {
        opts.data_page_size = storage_page_size;
    }
    opts.dict_page_size = _tablet_schema->storage_dict_page_size();
    DBUG_EXECUTE_IF("VerticalSegmentWriter._create_column_writer.storage_page_size", {
        auto table_id = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size", "table_id",
                INT_MIN);
        auto target_data_page_size = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size",
                "storage_page_size", INT_MIN);
        if (table_id == INT_MIN || target_data_page_size == INT_MIN) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Debug point parameters missing: either 'table_id' or 'storage_page_size' not "
                    "set.");
        }
        if (table_id == _tablet_schema->table_id() &&
            opts.data_page_size != target_data_page_size) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Mismatch in 'storage_page_size': expected size does not match the current "
                    "data page size. "
                    "Expected: " +
                    std::to_string(target_data_page_size) +
                    ", Actual: " + std::to_string(opts.data_page_size) + ".");
        }
    })
    if (column.is_row_store_column()) {
        // smaller page size for row store column
        auto page_size = _tablet_schema->row_store_page_size();
        opts.data_page_size =
                (page_size > 0) ? page_size : segment_v2::ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;
    }

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _file_writer, &writer));
    RETURN_IF_ERROR(writer->init());
    _column_writers.push_back(std::move(writer));

    _olap_data_convertor->add_column_data_convertor(column);
    return Status::OK();
};

Status VerticalSegmentWriter::init() {
    DCHECK(_column_writers.empty());
    if (_opts.compression_type == UNKNOWN_COMPRESSION) {
        _opts.compression_type = _tablet_schema->compression_type();
    }
    _olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    _olap_data_convertor->reserve(_tablet_schema->num_columns());
    _column_writers.reserve(_tablet_schema->columns().size());
    // we don't need the short key index for unique key merge on write table.
    if (_is_mow()) {
        size_t seq_col_length = 0;
        if (_tablet_schema->has_sequence_col()) {
            seq_col_length =
                    _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
        }
        size_t rowid_length = 0;
        if (_is_mow_with_cluster_key()) {
            rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
            _short_key_index_builder.reset(
                    new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
        }
        _primary_key_index_builder.reset(
                new PrimaryKeyIndexBuilder(_file_writer, seq_col_length, rowid_length));
        RETURN_IF_ERROR(_primary_key_index_builder->init());
    } else {
        _short_key_index_builder.reset(
                new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    }
    return Status::OK();
}

void VerticalSegmentWriter::_maybe_invalid_row_cache(const std::string& key) const {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache && _tablet_schema->has_row_store_for_all_columns() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({_opts.rowset_ctx->tablet_id, key});
    }
}

void VerticalSegmentWriter::_serialize_block_to_row_column(vectorized::Block& block) {
    if (block.rows() == 0) {
        return;
    }
    MonotonicStopWatch watch;
    watch.start();
    int row_column_id = 0;
    for (int i = 0; i < _tablet_schema->num_columns(); ++i) {
        if (_tablet_schema->column(i).is_row_store_column()) {
            auto* row_store_column = static_cast<vectorized::ColumnString*>(
                    block.get_by_position(i).column->assume_mutable_ref().assume_mutable().get());
            row_store_column->clear();
            vectorized::DataTypeSerDeSPtrs serdes =
                    vectorized::create_data_type_serdes(block.get_data_types());
            std::unordered_set<int> row_store_cids_set(_tablet_schema->row_columns_uids().begin(),
                                                       _tablet_schema->row_columns_uids().end());
            vectorized::JsonbSerializeUtil::block_to_jsonb(
                    *_tablet_schema, block, *row_store_column,
                    cast_set<int>(_tablet_schema->num_columns()), serdes, row_store_cids_set);
            break;
        }
    }

    VLOG_DEBUG << "serialize , num_rows:" << block.rows() << ", row_column_id:" << row_column_id
               << ", total_byte_size:" << block.allocated_bytes() << ", serialize_cost(us)"
               << watch.elapsed_time() / 1000;
}

Status VerticalSegmentWriter::_probe_key_for_mow(
        std::string key, std::size_t segment_pos, bool have_input_seq_column, bool have_delete_sign,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
        bool& has_default_or_nullable, std::vector<bool>& use_default_or_null_flag,
        const std::function<void(const RowLocation& loc)>& found_cb,
        const std::function<Status()>& not_found_cb, PartialUpdateStats& stats) {
    RowLocation loc;
    // save rowset shared ptr so this rowset wouldn't delete
    RowsetSharedPtr rowset;
    auto st = _tablet->lookup_row_key(
            key, _tablet_schema.get(), have_input_seq_column, specified_rowsets, &loc,
            cast_set<uint32_t>(_mow_context->max_version), segment_caches, &rowset);
    if (st.is<KEY_NOT_FOUND>()) {
        if (!have_delete_sign) {
            RETURN_IF_ERROR(not_found_cb());
        }
        ++stats.num_rows_new_added;
        has_default_or_nullable = true;
        use_default_or_null_flag.emplace_back(true);
        return Status::OK();
    }
    if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
        LOG(WARNING) << "failed to lookup row key, error: " << st;
        return st;
    }

    // 1. if the delete sign is marked, it means that the value columns of the row will not
    //    be read. So we don't need to read the missing values from the previous rows.
    // 2. the one exception is when there are sequence columns in the table, we need to read
    //    the sequence columns, otherwise it may cause the merge-on-read based compaction
    //    policy to produce incorrect results

    // 3. In flexible partial update, we may delete the existing rows before if there exists
    //    insert after delete in one load. In this case, the insert should also be treated
    //    as newly inserted rows, note that the sequence column value is filled in
    //    BlockAggregator::aggregate_for_insert_after_delete() if this row doesn't specify the sequence column
    if (st.is<KEY_ALREADY_EXISTS>() || (have_delete_sign && !_tablet_schema->has_sequence_col()) ||
        (_opts.rowset_ctx->partial_update_info->is_flexible_partial_update() &&
         _mow_context->delete_bitmap->contains(
                 {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id))) {
        has_default_or_nullable = true;
        use_default_or_null_flag.emplace_back(true);
    } else {
        // partial update should not contain invisible columns
        use_default_or_null_flag.emplace_back(false);
        _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
        found_cb(loc);
    }

    if (st.is<KEY_ALREADY_EXISTS>()) {
        // although we need to mark delete current row, we still need to read missing columns
        // for this row, we need to ensure that each column is aligned
        _mow_context->delete_bitmap->add(
                {_opts.rowset_ctx->rowset_id, _segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                cast_set<uint32_t>(segment_pos));
        ++stats.num_rows_deleted;
    } else {
        _mow_context->delete_bitmap->add(
                {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id);
        ++stats.num_rows_updated;
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_partial_update_preconditions_check(size_t row_pos,
                                                                  bool is_flexible_update) {
    if (!_is_mow()) {
        auto msg = fmt::format(
                "Can only do partial update on merge-on-write unique table, but found: "
                "keys_type={}, _opts.enable_unique_key_merge_on_write={}, tablet_id={}",
                _tablet_schema->keys_type(), _opts.enable_unique_key_merge_on_write,
                _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    if (_opts.rowset_ctx->partial_update_info == nullptr) {
        auto msg =
                fmt::format("partial_update_info should not be nullptr, please check, tablet_id={}",
                            _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    if (!is_flexible_update) {
        if (!_opts.rowset_ctx->partial_update_info->is_fixed_partial_update()) {
            auto msg = fmt::format(
                    "in fixed partial update code, but update_mode={}, please check, tablet_id={}",
                    _opts.rowset_ctx->partial_update_info->update_mode(), _tablet->tablet_id());
            DCHECK(false) << msg;
            return Status::InternalError<false>(msg);
        }
    } else {
        if (!_opts.rowset_ctx->partial_update_info->is_flexible_partial_update()) {
            auto msg = fmt::format(
                    "in flexible partial update code, but update_mode={}, please check, "
                    "tablet_id={}",
                    _opts.rowset_ctx->partial_update_info->update_mode(), _tablet->tablet_id());
            DCHECK(false) << msg;
            return Status::InternalError<false>(msg);
        }
    }
    if (row_pos != 0) {
        auto msg = fmt::format("row_pos should be 0, but found {}, tablet_id={}", row_pos,
                               _tablet->tablet_id());
        DCHECK(false) << msg;
        return Status::InternalError<false>(msg);
    }
    return Status::OK();
}

// for partial update, we should do following steps to fill content of block:
// 1. set block data to data convertor, and get all key_column's converted slice
// 2. get pk of input block, and read missing columns
//       2.1 first find key location{rowset_id, segment_id, row_id}
//       2.2 build read plan to read by batch
//       2.3 fill block
// 3. set columns to data convertor and then write all columns
Status VerticalSegmentWriter::_append_block_with_partial_content(RowsInBlock& data,
                                                                 vectorized::Block& full_block) {
    DBUG_EXECUTE_IF("_append_block_with_partial_content.block", DBUG_BLOCK);

    RETURN_IF_ERROR(_partial_update_preconditions_check(data.row_pos, false));
    // create full block and fill with input columns
    full_block = _tablet_schema->create_block();
    const auto& including_cids = _opts.rowset_ctx->partial_update_info->update_cids;
    size_t input_id = 0;
    for (auto i : including_cids) {
        full_block.replace_by_position(i, data.block->get_by_position(input_id++).column);
    }
    RETURN_IF_ERROR(_olap_data_convertor->set_source_content_with_specifid_columns(
            &full_block, data.row_pos, data.num_rows, including_cids));

    bool have_input_seq_column = false;
    // write including columns
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns;
    vectorized::IOlapColumnDataAccessor* seq_column = nullptr;
    size_t segment_start_pos;
    for (auto cid : including_cids) {
        // here we get segment column row num before append data.
        segment_start_pos = _column_writers[cid]->get_next_rowid();
        // olap data convertor alway start from id = 0
        auto [status, column] = _olap_data_convertor->convert_column_data(cid);
        if (!status.ok()) {
            return status;
        }
        if (cid < _num_sort_key_columns) {
            key_columns.push_back(column);
        } else if (_tablet_schema->has_sequence_col() &&
                   cid == _tablet_schema->sequence_col_idx()) {
            seq_column = column;
            have_input_seq_column = true;
        }
        RETURN_IF_ERROR(_column_writers[cid]->append(column->get_nullmap(), column->get_data(),
                                                     data.num_rows));
    }

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(data.num_rows);
    const auto* delete_signs =
            BaseTablet::get_delete_sign_column_data(full_block, data.row_pos + data.num_rows);

    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = _mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    FixedReadPlan read_plan;

    // locate rows in base data
    PartialUpdateStats stats;

    for (size_t block_pos = data.row_pos; block_pos < data.row_pos + data.num_rows; block_pos++) {
        // block   segment
        //   2   ->   0
        //   3   ->   1
        //   4   ->   2
        //   5   ->   3
        // here row_pos = 2, num_rows = 4.
        size_t delta_pos = block_pos - data.row_pos;
        size_t segment_pos = segment_start_pos + delta_pos;
        std::string key = _full_encode_keys(key_columns, delta_pos);
        _maybe_invalid_row_cache(key);
        if (have_input_seq_column) {
            _encode_seq_column(seq_column, delta_pos, &key);
        }
        // If the table have sequence column, and the include-cids don't contain the sequence
        // column, we need to update the primary key index builder at the end of this method.
        // At that time, we have a valid sequence column to encode the key with seq col.
        if (!_tablet_schema->has_sequence_col() || have_input_seq_column) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign = (delete_signs != nullptr && delete_signs[block_pos] != 0);

        auto not_found_cb = [&]() {
            return _opts.rowset_ctx->partial_update_info->handle_new_key(
                    *_tablet_schema, [&]() -> std::string {
                        return data.block->dump_one_line(block_pos,
                                                         cast_set<int>(_num_sort_key_columns));
                    });
        };
        auto update_read_plan = [&](const RowLocation& loc) {
            read_plan.prepare_to_read(loc, segment_pos);
        };
        RETURN_IF_ERROR(_probe_key_for_mow(std::move(key), segment_pos, have_input_seq_column,
                                           have_delete_sign, specified_rowsets, segment_caches,
                                           has_default_or_nullable, use_default_or_null_flag,
                                           update_read_plan, not_found_cb, stats));
    }
    CHECK_EQ(use_default_or_null_flag.size(), data.num_rows);

    if (config::enable_merge_on_write_correctness_check) {
        _tablet->add_sentinel_mark_to_delete_bitmap(_mow_context->delete_bitmap.get(),
                                                    _mow_context->rowset_ids);
    }

    // read to fill full_block
    RETURN_IF_ERROR(read_plan.fill_missing_columns(
            _opts.rowset_ctx, _rsid_to_rowset, *_tablet_schema, full_block,
            use_default_or_null_flag, has_default_or_nullable, segment_start_pos, data.block));

    // row column should be filled here
    // convert block to row store format
    _serialize_block_to_row_column(full_block);

    // convert missing columns and send to column writer
    const auto& missing_cids = _opts.rowset_ctx->partial_update_info->missing_cids;
    RETURN_IF_ERROR(_olap_data_convertor->set_source_content_with_specifid_columns(
            &full_block, data.row_pos, data.num_rows, missing_cids));
    for (auto cid : missing_cids) {
        auto [status, column] = _olap_data_convertor->convert_column_data(cid);
        if (!status.ok()) {
            return status;
        }
        if (_tablet_schema->has_sequence_col() && !have_input_seq_column &&
            cid == _tablet_schema->sequence_col_idx()) {
            DCHECK_EQ(seq_column, nullptr);
            seq_column = column;
        }
        RETURN_IF_ERROR(_column_writers[cid]->append(column->get_nullmap(), column->get_data(),
                                                     data.num_rows));
    }

    _num_rows_updated += stats.num_rows_updated;
    _num_rows_deleted += stats.num_rows_deleted;
    _num_rows_new_added += stats.num_rows_new_added;
    _num_rows_filtered += stats.num_rows_filtered;
    if (_tablet_schema->has_sequence_col() && !have_input_seq_column) {
        DCHECK_NE(seq_column, nullptr);
        if (_num_rows_written != data.row_pos ||
            _primary_key_index_builder->num_rows() != _num_rows_written) {
            return Status::InternalError(
                    "Correctness check failed, _num_rows_written: {}, row_pos: {}, primary key "
                    "index builder num rows: {}",
                    _num_rows_written, data.row_pos, _primary_key_index_builder->num_rows());
        }
        RETURN_IF_ERROR(_generate_primary_key_index(_key_coders, key_columns, seq_column,
                                                    data.num_rows, false));
    }

    _num_rows_written += data.num_rows;
    DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
            << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
            << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

Status VerticalSegmentWriter::_append_block_with_flexible_partial_content(
        RowsInBlock& data, vectorized::Block& full_block) {
    RETURN_IF_ERROR(_partial_update_preconditions_check(data.row_pos, true));

    // data.block has the same schema with full_block
    DCHECK(data.block->columns() == _tablet_schema->num_columns());

    // create full block and fill with sort key columns
    full_block = _tablet_schema->create_block();

    auto segment_start_pos = _column_writers.front()->get_next_rowid();

    DCHECK(_tablet_schema->has_skip_bitmap_col());
    auto skip_bitmap_col_idx = _tablet_schema->skip_bitmap_col_idx();

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(data.num_rows);

    int32_t seq_map_col_unique_id = _opts.rowset_ctx->partial_update_info->sequence_map_col_uid();
    bool schema_has_sequence_col = _tablet_schema->has_sequence_col();

    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_flexible_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = _mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    // 1. aggregate duplicate rows in block
    RETURN_IF_ERROR(_block_aggregator.aggregate_for_flexible_partial_update(
            const_cast<vectorized::Block*>(data.block), data.num_rows, specified_rowsets,
            segment_caches));
    if (data.block->rows() != data.num_rows) {
        data.num_rows = data.block->rows();
        _olap_data_convertor->clear_source_content();
    }

    // 2. encode primary key columns
    // we can only encode primary key columns currently becasue all non-primary columns in flexible partial update
    // can have missing cells
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns {};
    RETURN_IF_ERROR(_block_aggregator.convert_pk_columns(const_cast<vectorized::Block*>(data.block),
                                                         data.row_pos, data.num_rows, key_columns));
    // 3. encode sequence column
    // We encode the seguence column even thought it may have invalid values in some rows because we need to
    // encode the value of sequence column in key for rows that have a valid value in sequence column during
    // lookup_raw_key. We will encode the sequence column again at the end of this method. At that time, we have
    // a valid sequence column to encode the key with seq col.
    vectorized::IOlapColumnDataAccessor* seq_column {nullptr};
    RETURN_IF_ERROR(_block_aggregator.convert_seq_column(const_cast<vectorized::Block*>(data.block),
                                                         data.row_pos, data.num_rows, seq_column));

    std::vector<BitmapValue>* skip_bitmaps = &(
            assert_cast<vectorized::ColumnBitmap*>(
                    data.block->get_by_position(skip_bitmap_col_idx).column->assume_mutable().get())
                    ->get_data());
    const auto* delete_signs =
            BaseTablet::get_delete_sign_column_data(*data.block, data.row_pos + data.num_rows);
    DCHECK(delete_signs != nullptr);

    for (std::size_t cid {0}; cid < _tablet_schema->num_key_columns(); cid++) {
        full_block.replace_by_position(cid, data.block->get_by_position(cid).column);
    }

    // 4. write primary key columns data
    for (std::size_t cid {0}; cid < _tablet_schema->num_key_columns(); cid++) {
        const auto& column = key_columns[cid];
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written);
        RETURN_IF_ERROR(_column_writers[cid]->append(column->get_nullmap(), column->get_data(),
                                                     data.num_rows));
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written + data.num_rows);
    }

    // 5. genreate read plan
    FlexibleReadPlan read_plan {_tablet_schema->has_row_store_for_all_columns()};
    PartialUpdateStats stats;
    RETURN_IF_ERROR(_generate_flexible_read_plan(
            read_plan, data, segment_start_pos, schema_has_sequence_col, seq_map_col_unique_id,
            skip_bitmaps, key_columns, seq_column, delete_signs, specified_rowsets, segment_caches,
            has_default_or_nullable, use_default_or_null_flag, stats));
    CHECK_EQ(use_default_or_null_flag.size(), data.num_rows);

    if (config::enable_merge_on_write_correctness_check) {
        _tablet->add_sentinel_mark_to_delete_bitmap(_mow_context->delete_bitmap.get(),
                                                    _mow_context->rowset_ids);
    }

    // 6. read according plan to fill full_block
    RETURN_IF_ERROR(read_plan.fill_non_primary_key_columns(
            _opts.rowset_ctx, _rsid_to_rowset, *_tablet_schema, full_block,
            use_default_or_null_flag, has_default_or_nullable, segment_start_pos, data.row_pos,
            data.block, skip_bitmaps));

    // TODO(bobhan1): should we replace the skip bitmap column with empty bitmaps to reduce storage occupation?
    // this column is not needed in read path for merge-on-write table

    // 7. fill row store column
    _serialize_block_to_row_column(full_block);

    // 8. encode and write all non-primary key columns(including sequence column if exists)
    for (auto cid = _tablet_schema->num_key_columns(); cid < _tablet_schema->num_columns(); cid++) {
        RETURN_IF_ERROR(_olap_data_convertor->set_source_content_with_specifid_column(
                full_block.get_by_position(cid), data.row_pos, data.num_rows,
                cast_set<uint32_t>(cid)));
        auto [status, column] = _olap_data_convertor->convert_column_data(cid);
        if (!status.ok()) {
            return status;
        }
        if (cid == _tablet_schema->sequence_col_idx()) {
            // should use the latest encoded sequence column to build the primary index
            seq_column = column;
        }
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written);
        RETURN_IF_ERROR(_column_writers[cid]->append(column->get_nullmap(), column->get_data(),
                                                     data.num_rows));
        DCHECK(_column_writers[cid]->get_next_rowid() == _num_rows_written + data.num_rows);
    }

    _num_rows_updated += stats.num_rows_updated;
    _num_rows_deleted += stats.num_rows_deleted;
    _num_rows_new_added += stats.num_rows_new_added;
    _num_rows_filtered += stats.num_rows_filtered;

    if (_num_rows_written != data.row_pos ||
        _primary_key_index_builder->num_rows() != _num_rows_written) {
        return Status::InternalError(
                "Correctness check failed, _num_rows_written: {}, row_pos: {}, primary key "
                "index builder num rows: {}",
                _num_rows_written, data.row_pos, _primary_key_index_builder->num_rows());
    }

    // 9. build primary key index
    RETURN_IF_ERROR(_generate_primary_key_index(_key_coders, key_columns, seq_column, data.num_rows,
                                                false));

    _num_rows_written += data.num_rows;
    DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
            << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
            << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_encoded_default_seq_value(const TabletSchema& tablet_schema,
                                                                  const PartialUpdateInfo& info,
                                                                  std::string* encoded_value) {
    const auto& seq_column = tablet_schema.column(tablet_schema.sequence_col_idx());
    auto block = tablet_schema.create_block_by_cids(
            {cast_set<uint32_t>(tablet_schema.sequence_col_idx())});
    if (seq_column.has_default_value()) {
        auto idx = tablet_schema.sequence_col_idx() - tablet_schema.num_key_columns();
        const auto& default_value = info.default_values[idx];
        vectorized::ReadBuffer rb(const_cast<char*>(default_value.c_str()), default_value.size());
        RETURN_IF_ERROR(block.get_by_position(0).type->from_string(
                rb, block.get_by_position(0).column->assume_mutable().get()));

    } else {
        block.get_by_position(0).column->assume_mutable()->insert_default();
    }
    DCHECK_EQ(block.rows(), 1);
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    olap_data_convertor->add_column_data_convertor(seq_column);
    olap_data_convertor->set_source_content(&block, 0, 1);
    auto [status, column] = olap_data_convertor->convert_column_data(0);
    if (!status.ok()) {
        return status;
    }
    // include marker
    _encode_seq_column(column, 0, encoded_value);
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_flexible_read_plan(
        FlexibleReadPlan& read_plan, RowsInBlock& data, size_t segment_start_pos,
        bool schema_has_sequence_col, int32_t seq_map_col_unique_id,
        std::vector<BitmapValue>* skip_bitmaps,
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
        vectorized::IOlapColumnDataAccessor* seq_column, const signed char* delete_signs,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
        bool& has_default_or_nullable, std::vector<bool>& use_default_or_null_flag,
        PartialUpdateStats& stats) {
    int32_t delete_sign_col_unique_id =
            _tablet_schema->column(_tablet_schema->delete_sign_idx()).unique_id();
    int32_t seq_col_unique_id =
            (_tablet_schema->has_sequence_col()
                     ? _tablet_schema->column(_tablet_schema->sequence_col_idx()).unique_id()
                     : -1);
    for (size_t block_pos = data.row_pos; block_pos < data.row_pos + data.num_rows; block_pos++) {
        size_t delta_pos = block_pos - data.row_pos;
        size_t segment_pos = segment_start_pos + delta_pos;
        auto& skip_bitmap = skip_bitmaps->at(block_pos);

        std::string key = _full_encode_keys(key_columns, delta_pos);
        _maybe_invalid_row_cache(key);
        bool row_has_sequence_col =
                (schema_has_sequence_col && !skip_bitmap.contains(seq_col_unique_id));
        if (row_has_sequence_col) {
            _encode_seq_column(seq_column, delta_pos, &key);
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (!skip_bitmap.contains(delete_sign_col_unique_id) && delete_signs[block_pos] != 0);

        auto not_found_cb = [&]() {
            return _opts.rowset_ctx->partial_update_info->handle_new_key(
                    *_tablet_schema,
                    [&]() -> std::string {
                        return data.block->dump_one_line(block_pos,
                                                         cast_set<int>(_num_sort_key_columns));
                    },
                    &skip_bitmap);
        };
        auto update_read_plan = [&](const RowLocation& loc) {
            read_plan.prepare_to_read(loc, segment_pos, skip_bitmap);
        };

        RETURN_IF_ERROR(_probe_key_for_mow(std::move(key), segment_pos, row_has_sequence_col,
                                           have_delete_sign, specified_rowsets, segment_caches,
                                           has_default_or_nullable, use_default_or_null_flag,
                                           update_read_plan, not_found_cb, stats));
    }
    return Status::OK();
}

Status VerticalSegmentWriter::batch_block(const vectorized::Block* block, size_t row_pos,
                                          size_t num_rows) {
    if (_opts.rowset_ctx->partial_update_info &&
        _opts.rowset_ctx->partial_update_info->is_partial_update() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT &&
        !_opts.rowset_ctx->is_transient_rowset_writer) {
        if (_opts.rowset_ctx->partial_update_info->is_flexible_partial_update()) {
            if (block->columns() != _tablet_schema->num_columns()) {
                return Status::InvalidArgument(
                        "illegal flexible partial update block columns, block columns = {}, "
                        "tablet_schema columns = {}",
                        block->dump_structure(), _tablet_schema->dump_structure());
            }
        } else {
            if (block->columns() < _tablet_schema->num_key_columns() ||
                block->columns() >= _tablet_schema->num_columns()) {
                return Status::InvalidArgument(fmt::format(
                        "illegal partial update block columns: {}, num key columns: {}, total "
                        "schema columns: {}",
                        block->columns(), _tablet_schema->num_key_columns(),
                        _tablet_schema->num_columns()));
            }
        }
    } else if (block->columns() != _tablet_schema->num_columns()) {
        return Status::InvalidArgument(
                "illegal block columns, block columns = {}, tablet_schema columns = {}",
                block->dump_structure(), _tablet_schema->dump_structure());
    }
    _batched_blocks.emplace_back(block, row_pos, num_rows);
    return Status::OK();
}

// for variant type, we should do following steps to fill content of block:
// 1. set block data to data convertor, and get all flattened columns from variant subcolumns
// 2. get sparse columns from previous sparse columns stripped in OlapColumnDataConvertorVariant
// 3. merge current columns info(contains extracted columns) with previous merged_tablet_schema
//    which will be used to contruct the new schema for rowset
Status VerticalSegmentWriter::_append_block_with_variant_subcolumns(RowsInBlock& data) {
    if (_tablet_schema->num_variant_columns() == 0) {
        return Status::OK();
    }
    size_t column_id = _tablet_schema->num_columns();
    for (int i = 0; i < _tablet_schema->columns().size(); ++i) {
        if (!_tablet_schema->columns()[i]->is_variant_type()) {
            continue;
        }
        if (_flush_schema == nullptr) {
            _flush_schema = std::make_shared<TabletSchema>();
            // deep copy
            _flush_schema->copy_from(*_tablet_schema);
        }
        auto column_ref = data.block->get_by_position(i).column;
        const vectorized::ColumnVariant& object_column = assert_cast<vectorized::ColumnVariant&>(
                remove_nullable(column_ref)->assume_mutable_ref());
        const TabletColumnPtr& parent_column = _tablet_schema->columns()[i];

        // generate column info by entry info
        auto generate_column_info = [&](const auto& entry) {
            const std::string& column_name =
                    parent_column->name_lower_case() + "." + entry->path.get_path();
            const vectorized::DataTypePtr& final_data_type_from_object =
                    entry->data.get_least_common_type();
            vectorized::PathInDataBuilder full_path_builder;
            auto full_path = full_path_builder.append(parent_column->name_lower_case(), false)
                                     .append(entry->path.get_parts(), false)
                                     .build();
            return vectorized::schema_util::get_column_by_type(
                    final_data_type_from_object, column_name,
                    vectorized::schema_util::ExtraInfo {
                            .unique_id = -1,
                            .parent_unique_id = parent_column->unique_id(),
                            .path_info = full_path});
        };

        CHECK(object_column.is_finalized());
        // common extracted columns
        for (const auto& entry :
             vectorized::schema_util::get_sorted_subcolumns(object_column.get_subcolumns())) {
            if (entry->path.empty()) {
                // already handled by parent column
                continue;
            }
            CHECK(entry->data.is_finalized());
            uint32_t current_column_id = cast_set<uint32_t>(column_id++);
            TabletColumn tablet_column = generate_column_info(entry);
            vectorized::schema_util::inherit_column_attributes(*parent_column, tablet_column,
                                                               _flush_schema);
            RETURN_IF_ERROR(_create_column_writer(current_column_id /*unused*/, tablet_column,
                                                  _flush_schema));
            RETURN_IF_ERROR(_olap_data_convertor->set_source_content_with_specifid_column(
                    {entry->data.get_finalized_column_ptr()->get_ptr(),
                     entry->data.get_least_common_type(), tablet_column.name()},
                    data.row_pos, data.num_rows, current_column_id));
            // convert column data from engine format to storage layer format
            auto [status, column] = _olap_data_convertor->convert_column_data(current_column_id);
            if (!status.ok()) {
                return status;
            }
            RETURN_IF_ERROR(_column_writers[current_column_id]->append(
                    column->get_nullmap(), column->get_data(), data.num_rows));
            _flush_schema->append_column(tablet_column);
            _olap_data_convertor->clear_source_content();
        }
        // sparse_columns
        for (const auto& entry : vectorized::schema_util::get_sorted_subcolumns(
                     object_column.get_sparse_subcolumns())) {
            TabletColumn sparse_tablet_column = generate_column_info(entry);
            _flush_schema->mutable_column_by_uid(parent_column->unique_id())
                    .append_sparse_column(sparse_tablet_column);

            // add sparse column to footer
            auto* column_pb = _footer.mutable_columns(i);
            _init_column_meta(column_pb->add_sparse_columns(), -1, sparse_tablet_column);
        }
    }

    // Update rowset schema, tablet's tablet schema will be updated when build Rowset
    // Eg. flush schema:    A(int),    B(float),  C(int), D(int)
    // ctx.tablet_schema:  A(bigint), B(double)
    // => update_schema:   A(bigint), B(double), C(int), D(int)
    std::lock_guard<std::mutex> lock(*(_opts.rowset_ctx->schema_lock));
    if (_opts.rowset_ctx->merged_tablet_schema == nullptr) {
        _opts.rowset_ctx->merged_tablet_schema = _opts.rowset_ctx->tablet_schema;
    }
    TabletSchemaSPtr update_schema;
    bool check_schema_size = true;
    RETURN_IF_ERROR(vectorized::schema_util::get_least_common_schema(
            {_opts.rowset_ctx->merged_tablet_schema, _flush_schema}, nullptr, update_schema,
            check_schema_size));
    CHECK_GE(update_schema->num_columns(), _flush_schema->num_columns())
            << "Rowset merge schema columns count is " << update_schema->num_columns()
            << ", but flush_schema is larger " << _flush_schema->num_columns()
            << " update_schema: " << update_schema->dump_structure()
            << " flush_schema: " << _flush_schema->dump_structure();
    _opts.rowset_ctx->merged_tablet_schema.swap(update_schema);
    VLOG_DEBUG << "dump block " << data.block->dump_data();
    VLOG_DEBUG << "dump rs schema: " << _opts.rowset_ctx->merged_tablet_schema->dump_full_schema();
    VLOG_DEBUG << "rowset : " << _opts.rowset_ctx->rowset_id << ", seg id : " << _segment_id;
    return Status::OK();
}

Status VerticalSegmentWriter::write_batch() {
    if (_opts.rowset_ctx->partial_update_info &&
        _opts.rowset_ctx->partial_update_info->is_partial_update() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT &&
        !_opts.rowset_ctx->is_transient_rowset_writer) {
        bool is_flexible_partial_update =
                _opts.rowset_ctx->partial_update_info->is_flexible_partial_update();
        for (uint32_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
            RETURN_IF_ERROR(
                    _create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
        }
        vectorized::Block full_block;
        for (auto& data : _batched_blocks) {
            if (is_flexible_partial_update) {
                RETURN_IF_ERROR(_append_block_with_flexible_partial_content(data, full_block));
            } else {
                RETURN_IF_ERROR(_append_block_with_partial_content(data, full_block));
            }
        }
        for (auto& data : _batched_blocks) {
            RowsInBlock full_rows_block {&full_block, data.row_pos, data.num_rows};
            RETURN_IF_ERROR(_append_block_with_variant_subcolumns(full_rows_block));
        }
        for (auto& column_writer : _column_writers) {
            RETURN_IF_ERROR(column_writer->finish());
            RETURN_IF_ERROR(column_writer->write_data());
        }
        return Status::OK();
    }
    // Row column should be filled here when it's a directly write from memtable
    // or it's schema change write(since column data type maybe changed, so we should reubild)
    if (_opts.write_type == DataWriteType::TYPE_DIRECT ||
        _opts.write_type == DataWriteType::TYPE_SCHEMA_CHANGE) {
        for (auto& data : _batched_blocks) {
            // TODO: maybe we should pass range to this method
            _serialize_block_to_row_column(*const_cast<vectorized::Block*>(data.block));
        }
    }

    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns;
    vectorized::IOlapColumnDataAccessor* seq_column = nullptr;
    // the key is cluster key column unique id
    std::map<uint32_t, vectorized::IOlapColumnDataAccessor*> cid_to_column;
    for (uint32_t cid = 0; cid < _tablet_schema->num_columns(); ++cid) {
        RETURN_IF_ERROR(_create_column_writer(cid, _tablet_schema->column(cid), _tablet_schema));
        for (auto& data : _batched_blocks) {
            RETURN_IF_ERROR(_olap_data_convertor->set_source_content_with_specifid_columns(
                    data.block, data.row_pos, data.num_rows, std::vector<uint32_t> {cid}));

            // convert column data from engine format to storage layer format
            auto [status, column] = _olap_data_convertor->convert_column_data(cid);
            if (!status.ok()) {
                return status;
            }
            if (cid < _tablet_schema->num_key_columns()) {
                key_columns.push_back(column);
            }
            if (_tablet_schema->has_sequence_col() && cid == _tablet_schema->sequence_col_idx()) {
                seq_column = column;
            }
            auto column_unique_id = _tablet_schema->column(cid).unique_id();
            if (_is_mow_with_cluster_key() &&
                std::find(_tablet_schema->cluster_key_uids().begin(),
                          _tablet_schema->cluster_key_uids().end(),
                          column_unique_id) != _tablet_schema->cluster_key_uids().end()) {
                cid_to_column[column_unique_id] = column;
            }
            RETURN_IF_ERROR(_column_writers[cid]->append(column->get_nullmap(), column->get_data(),
                                                         data.num_rows));
            _olap_data_convertor->clear_source_content();
        }
        if (_data_dir != nullptr &&
            _data_dir->reach_capacity_limit(_column_writers[cid]->estimate_buffer_size())) {
            return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit.",
                                                            _data_dir->path_hash());
        }
        RETURN_IF_ERROR(_column_writers[cid]->finish());
        RETURN_IF_ERROR(_column_writers[cid]->write_data());
    }

    for (auto& data : _batched_blocks) {
        _olap_data_convertor->set_source_content(data.block, data.row_pos, data.num_rows);
        RETURN_IF_ERROR(_generate_key_index(data, key_columns, seq_column, cid_to_column));
        _olap_data_convertor->clear_source_content();
        _num_rows_written += data.num_rows;
    }

    if (_opts.write_type == DataWriteType::TYPE_DIRECT ||
        _opts.write_type == DataWriteType::TYPE_SCHEMA_CHANGE) {
        size_t original_writers_cnt = _column_writers.size();
        // handle variant dynamic sub columns
        for (auto& data : _batched_blocks) {
            RETURN_IF_ERROR(_append_block_with_variant_subcolumns(data));
        }
        for (size_t i = original_writers_cnt; i < _column_writers.size(); ++i) {
            RETURN_IF_ERROR(_column_writers[i]->finish());
            RETURN_IF_ERROR(_column_writers[i]->write_data());
        }
    }

    _batched_blocks.clear();
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_key_index(
        RowsInBlock& data, std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
        vectorized::IOlapColumnDataAccessor* seq_column,
        std::map<uint32_t, vectorized::IOlapColumnDataAccessor*>& cid_to_column) {
    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;
    // We build a short key index every `_opts.num_rows_per_block` rows. Specifically, we
    // build a short key index using 1st rows for first block and `_short_key_row_pos - _row_count`
    // for next blocks.
    if (_short_key_row_pos == 0 && _num_rows_written == 0) {
        short_key_pos.push_back(0);
    }
    while (_short_key_row_pos + _opts.num_rows_per_block < _num_rows_written + data.num_rows) {
        _short_key_row_pos += _opts.num_rows_per_block;
        short_key_pos.push_back(_short_key_row_pos - _num_rows_written);
    }
    if (_is_mow_with_cluster_key()) {
        // 1. generate primary key index
        RETURN_IF_ERROR(_generate_primary_key_index(_primary_key_coders, key_columns, seq_column,
                                                    data.num_rows, true));
        // 2. generate short key index (use cluster key)
        std::vector<vectorized::IOlapColumnDataAccessor*> short_key_columns;
        for (const auto& cid : _tablet_schema->cluster_key_uids()) {
            short_key_columns.push_back(cid_to_column[cid]);
        }
        RETURN_IF_ERROR(_generate_short_key_index(short_key_columns, data.num_rows, short_key_pos));
    } else if (_is_mow()) {
        RETURN_IF_ERROR(_generate_primary_key_index(_key_coders, key_columns, seq_column,
                                                    data.num_rows, false));
    } else { // other tables
        RETURN_IF_ERROR(_generate_short_key_index(key_columns, data.num_rows, short_key_pos));
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_primary_key_index(
        const std::vector<const KeyCoder*>& primary_key_coders,
        const std::vector<vectorized::IOlapColumnDataAccessor*>& primary_key_columns,
        vectorized::IOlapColumnDataAccessor* seq_column, size_t num_rows, bool need_sort) {
    if (!need_sort) { // mow table without cluster key
        std::string last_key;
        for (size_t pos = 0; pos < num_rows; pos++) {
            // use _key_coders
            std::string key = _full_encode_keys(primary_key_columns, pos);
            _maybe_invalid_row_cache(key);
            if (_tablet_schema->has_sequence_col()) {
                _encode_seq_column(seq_column, pos, &key);
            }
            DCHECK(key.compare(last_key) > 0)
                    << "found duplicate key or key is not sorted! current key: " << key
                    << ", last key: " << last_key;
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
            last_key = std::move(key);
        }
    } else { // mow table with cluster key
        // 1. generate primary keys in memory
        std::vector<std::string> primary_keys;
        for (uint32_t pos = 0; pos < num_rows; pos++) {
            std::string key = _full_encode_keys(primary_key_coders, primary_key_columns, pos);
            _maybe_invalid_row_cache(key);
            if (_tablet_schema->has_sequence_col()) {
                _encode_seq_column(seq_column, pos, &key);
            }
            _encode_rowid(pos, &key);
            primary_keys.emplace_back(std::move(key));
        }
        // 2. sort primary keys
        std::sort(primary_keys.begin(), primary_keys.end());
        // 3. write primary keys index
        std::string last_key;
        for (const auto& key : primary_keys) {
            DCHECK(key.compare(last_key) > 0)
                    << "found duplicate key or key is not sorted! current key: " << key
                    << ", last key: " << last_key;
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
            last_key = key;
        }
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_generate_short_key_index(
        std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t num_rows,
        const std::vector<size_t>& short_key_pos) {
    // use _key_coders
    _set_min_key(_full_encode_keys(key_columns, 0));
    _set_max_key(_full_encode_keys(key_columns, num_rows - 1));
    DCHECK(Slice(_max_key.data(), _max_key.size())
                   .compare(Slice(_min_key.data(), _min_key.size())) >= 0)
            << "key is not sorted! min key: " << _min_key << ", max key: " << _max_key;

    key_columns.resize(_num_short_key_columns);
    std::string last_key;
    for (const auto pos : short_key_pos) {
        std::string key = _encode_keys(key_columns, pos);
        DCHECK(key.compare(last_key) >= 0)
                << "key is not sorted! current key: " << key << ", last key: " << last_key;
        RETURN_IF_ERROR(_short_key_index_builder->add_item(key));
        last_key = std::move(key);
    }
    return Status::OK();
}

void VerticalSegmentWriter::_encode_rowid(const uint32_t rowid, std::string* encoded_keys) {
    encoded_keys->push_back(KEY_NORMAL_MARKER);
    _rowid_coder->full_encode_ascending(&rowid, encoded_keys);
}

std::string VerticalSegmentWriter::_full_encode_keys(
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos) {
    assert(_key_index_size.size() == _num_sort_key_columns);
    if (!(key_columns.size() == _num_sort_key_columns &&
          _key_coders.size() == _num_sort_key_columns)) {
        LOG_INFO("key_columns.size()={}, _key_coders.size()={}, _num_sort_key_columns={}, ",
                 key_columns.size(), _key_coders.size(), _num_sort_key_columns);
    }
    assert(key_columns.size() == _num_sort_key_columns &&
           _key_coders.size() == _num_sort_key_columns);
    return _full_encode_keys(_key_coders, key_columns, pos);
}

std::string VerticalSegmentWriter::_full_encode_keys(
        const std::vector<const KeyCoder*>& key_coders,
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos) {
    assert(key_columns.size() == key_coders.size());

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            ++cid;
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        DCHECK(key_coders[cid] != nullptr);
        key_coders[cid]->full_encode_ascending(field, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

void VerticalSegmentWriter::_encode_seq_column(
        const vectorized::IOlapColumnDataAccessor* seq_column, size_t pos,
        std::string* encoded_keys) {
    const auto* field = seq_column->get_data_at(pos);
    // To facilitate the use of the primary key index, encode the seq column
    // to the minimum value of the corresponding length when the seq column
    // is null
    if (UNLIKELY(!field)) {
        encoded_keys->push_back(KEY_NULL_FIRST_MARKER);
        size_t seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length();
        encoded_keys->append(seq_col_length, KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KEY_NORMAL_MARKER);
    _seq_coder->full_encode_ascending(field, encoded_keys);
}

std::string VerticalSegmentWriter::_encode_keys(
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos) {
    assert(key_columns.size() == _num_short_key_columns);

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            ++cid;
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        _key_coders[cid]->encode_ascending(field, _key_index_size[cid], &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
uint64_t VerticalSegmentWriter::_estimated_remaining_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    if (_is_mow_with_cluster_key()) {
        size += _primary_key_index_builder->size() + _short_key_index_builder->size();
    } else if (_is_mow()) {
        size += _primary_key_index_builder->size();
    } else {
        size += _short_key_index_builder->size();
    }

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
}

Status VerticalSegmentWriter::finalize_columns_index(uint64_t* index_size) {
    uint64_t index_start = _file_writer->bytes_appended();
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_bitmap_index());
    RETURN_IF_ERROR(_write_inverted_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());

    *index_size = _file_writer->bytes_appended() - index_start;
    if (_is_mow_with_cluster_key()) {
        RETURN_IF_ERROR(_write_short_key_index());
        *index_size = _file_writer->bytes_appended() - index_start;
        RETURN_IF_ERROR(_write_primary_key_index());
        *index_size += _primary_key_index_builder->disk_size();
    } else if (_is_mow()) {
        RETURN_IF_ERROR(_write_primary_key_index());
        // IndexedColumnWriter write data pages mixed with segment data, we should use
        // the stat from primary key index builder.
        *index_size += _primary_key_index_builder->disk_size();
    } else {
        RETURN_IF_ERROR(_write_short_key_index());
        *index_size = _file_writer->bytes_appended() - index_start;
    }

    // reset all column writers and data_conveter
    clear();

    return Status::OK();
}

Status VerticalSegmentWriter::finalize_footer(uint64_t* segment_file_size) {
    RETURN_IF_ERROR(_write_footer());
    // finish
    RETURN_IF_ERROR(_file_writer->close(true));
    *segment_file_size = _file_writer->bytes_appended();
    if (*segment_file_size == 0) {
        return Status::Corruption("Bad segment, file size = 0");
    }
    return Status::OK();
}

Status VerticalSegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    MonotonicStopWatch timer;
    timer.start();
    // check disk capacity
    if (_data_dir != nullptr &&
        _data_dir->reach_capacity_limit((int64_t)_estimated_remaining_size())) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit.",
                                                        _data_dir->path_hash());
    }
    _row_count = _num_rows_written;
    _num_rows_written = 0;
    // write index
    RETURN_IF_ERROR(finalize_columns_index(index_size));
    // write footer
    RETURN_IF_ERROR(finalize_footer(segment_file_size));

    if (timer.elapsed_time() > 5000000000L) {
        LOG(INFO) << "segment flush consumes a lot time_ns " << timer.elapsed_time()
                  << ", segmemt_size " << *segment_file_size;
    }
    return Status::OK();
}

void VerticalSegmentWriter::clear() {
    for (auto& column_writer : _column_writers) {
        column_writer.reset();
    }
    _column_writers.clear();
    _olap_data_convertor.reset();
}

// write ordinal index after data has been written
Status VerticalSegmentWriter::_write_ordinal_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_zone_map() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_bitmap_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bitmap_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_inverted_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_inverted_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_bloom_filter_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status VerticalSegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_short_key_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_file_writer, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

Status VerticalSegmentWriter::_write_primary_key_index() {
    CHECK_EQ(_primary_key_index_builder->num_rows(), _row_count);
    return _primary_key_index_builder->finalize(_footer.mutable_primary_key_index_meta());
}

Status VerticalSegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, cast_set<uint32_t>(footer_buf.size()));
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    std::vector<Slice> slices {footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status VerticalSegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_file_writer->appendv(&slices[0], slices.size()));
    return Status::OK();
}

Slice VerticalSegmentWriter::min_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_min_key.data(), _min_key.size())
                                                   : _primary_key_index_builder->min_key();
}
Slice VerticalSegmentWriter::max_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_max_key.data(), _max_key.size())
                                                   : _primary_key_index_builder->max_key();
}

void VerticalSegmentWriter::_set_min_max_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
    if (key.compare(_max_key) > 0) {
        _max_key.clear();
        _max_key.append(key.get_data(), key.get_size());
    }
}

void VerticalSegmentWriter::_set_min_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
}

void VerticalSegmentWriter::_set_max_key(const Slice& key) {
    _max_key.clear();
    _max_key.append(key.get_data(), key.get_size());
}

inline bool VerticalSegmentWriter::_is_mow() {
    return _tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write;
}

inline bool VerticalSegmentWriter::_is_mow_with_cluster_key() {
    return _is_mow() && !_tablet_schema->cluster_key_uids().empty();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
