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

#include "olap/rowset/segment_v2/column_reader.h"

#include <assert.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <set>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/decimal12.h"
#include "olap/inverted_index_parser.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "olap/types.h" // for TypeInfo
#include "olap/wrapper_field.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "util/binary_cast.hpp"
#include "util/bitmap.h"
#include "util/block_compression.h"
#include "util/rle_encoding.h" // for RleDecoder
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/runtime/vdatetime_value.h" //for VecDateTime

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

inline bool read_as_string(PrimitiveType type) {
    return type == PrimitiveType::TYPE_STRING || type == PrimitiveType::INVALID_TYPE ||
           type == PrimitiveType::TYPE_BITMAP || type == PrimitiveType::TYPE_FIXED_LENGTH_OBJECT;
}

Status ColumnReader::create_array(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                  const io::FileReaderSPtr& file_reader,
                                  std::unique_ptr<ColumnReader>* reader) {
    DCHECK(meta.children_columns_size() == 2 || meta.children_columns_size() == 3);

    std::unique_ptr<ColumnReader> item_reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(0),
                                         meta.children_columns(0).num_rows(), file_reader,
                                         &item_reader));

    std::unique_ptr<ColumnReader> offset_reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(1),
                                         meta.children_columns(1).num_rows(), file_reader,
                                         &offset_reader));

    std::unique_ptr<ColumnReader> null_reader;
    if (meta.is_nullable()) {
        RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(2),
                                             meta.children_columns(2).num_rows(), file_reader,
                                             &null_reader));
    }

    // The num rows of the array reader equals to the num rows of the length reader.
    uint64_t array_num_rows = meta.children_columns(1).num_rows();
    std::unique_ptr<ColumnReader> array_reader(
            new ColumnReader(opts, meta, array_num_rows, file_reader));
    //  array reader do not need to init
    array_reader->_sub_readers.resize(meta.children_columns_size());
    array_reader->_sub_readers[0] = std::move(item_reader);
    array_reader->_sub_readers[1] = std::move(offset_reader);
    if (meta.is_nullable()) {
        array_reader->_sub_readers[2] = std::move(null_reader);
    }
    array_reader->_meta_type = FieldType::OLAP_FIELD_TYPE_ARRAY;
    *reader = std::move(array_reader);
    return Status::OK();
}

Status ColumnReader::create_map(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                const io::FileReaderSPtr& file_reader,
                                std::unique_ptr<ColumnReader>* reader) {
    // map reader now has 3 sub readers for key, value, offsets(scalar), null(scala)
    DCHECK(meta.children_columns_size() == 3 || meta.children_columns_size() == 4);
    std::unique_ptr<ColumnReader> key_reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(0),
                                         meta.children_columns(0).num_rows(), file_reader,
                                         &key_reader));
    std::unique_ptr<ColumnReader> val_reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(1),
                                         meta.children_columns(1).num_rows(), file_reader,
                                         &val_reader));
    std::unique_ptr<ColumnReader> offset_reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(2),
                                         meta.children_columns(2).num_rows(), file_reader,
                                         &offset_reader));
    std::unique_ptr<ColumnReader> null_reader;
    if (meta.is_nullable()) {
        RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(3),
                                             meta.children_columns(3).num_rows(), file_reader,
                                             &null_reader));
    }

    // The num rows of the map reader equals to the num rows of the length reader.
    uint64_t map_num_rows = meta.children_columns(2).num_rows();
    std::unique_ptr<ColumnReader> map_reader(
            new ColumnReader(opts, meta, map_num_rows, file_reader));
    map_reader->_sub_readers.resize(meta.children_columns_size());

    map_reader->_sub_readers[0] = std::move(key_reader);
    map_reader->_sub_readers[1] = std::move(val_reader);
    map_reader->_sub_readers[2] = std::move(offset_reader);
    if (meta.is_nullable()) {
        map_reader->_sub_readers[3] = std::move(null_reader);
    }
    map_reader->_meta_type = FieldType::OLAP_FIELD_TYPE_MAP;
    *reader = std::move(map_reader);
    return Status::OK();
}

Status ColumnReader::create_struct(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                   uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                                   std::unique_ptr<ColumnReader>* reader) {
    // not support empty struct
    DCHECK(meta.children_columns_size() >= 1);
    // create struct column reader
    std::unique_ptr<ColumnReader> struct_reader(
            new ColumnReader(opts, meta, num_rows, file_reader));
    struct_reader->_sub_readers.reserve(meta.children_columns_size());
    // now we support struct column can add the children columns according to the schema-change behavior
    for (int i = 0; i < meta.children_columns_size(); i++) {
        std::unique_ptr<ColumnReader> sub_reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(i),
                                             meta.children_columns(i).num_rows(), file_reader,
                                             &sub_reader));
        struct_reader->_sub_readers.push_back(std::move(sub_reader));
    }
    struct_reader->_meta_type = FieldType::OLAP_FIELD_TYPE_STRUCT;
    *reader = std::move(struct_reader);
    return Status::OK();
}

Status ColumnReader::create_agg_state(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                      uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                                      std::unique_ptr<ColumnReader>* reader) {
    if (!meta.has_function_name()) { // meet old version ColumnMetaPB
        std::unique_ptr<ColumnReader> reader_local(
                new ColumnReader(opts, meta, num_rows, file_reader));
        RETURN_IF_ERROR(reader_local->init(&meta));
        *reader = std::move(reader_local);
        return Status::OK();
    }

    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(meta);
    const auto* agg_state_type = assert_cast<const vectorized::DataTypeAggState*>(data_type.get());
    agg_state_type->check_function_compatibility(opts.be_exec_version);
    auto type = agg_state_type->get_serialized_type()->get_primitive_type();

    if (read_as_string(type)) {
        std::unique_ptr<ColumnReader> reader_local(
                new ColumnReader(opts, meta, num_rows, file_reader));
        RETURN_IF_ERROR(reader_local->init(&meta));
        *reader = std::move(reader_local);
        return Status::OK();
    } else if (type == PrimitiveType::TYPE_MAP) {
        return create_map(opts, meta, file_reader, reader);
    } else if (type == PrimitiveType::TYPE_ARRAY) {
        return create_array(opts, meta, file_reader, reader);
    } else if (type == PrimitiveType::TYPE_STRUCT) {
        return create_struct(opts, meta, num_rows, file_reader, reader);
    }

    return Status::InternalError("Not supported type: {}, serialized type: {}",
                                 agg_state_type->get_name(), int(type));
}

Status ColumnReader::create(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                            uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                            std::unique_ptr<ColumnReader>* reader) {
    if (is_scalar_type((FieldType)meta.type())) {
        std::unique_ptr<ColumnReader> reader_local(
                new ColumnReader(opts, meta, num_rows, file_reader));
        RETURN_IF_ERROR(reader_local->init(&meta));
        *reader = std::move(reader_local);
        return Status::OK();
    } else {
        auto type = (FieldType)meta.type();
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_AGG_STATE: {
            return create_agg_state(opts, meta, num_rows, file_reader, reader);
        }
        case FieldType::OLAP_FIELD_TYPE_STRUCT: {
            return create_struct(opts, meta, num_rows, file_reader, reader);
        }
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            return create_array(opts, meta, file_reader, reader);
        }
        case FieldType::OLAP_FIELD_TYPE_MAP: {
            return create_map(opts, meta, file_reader, reader);
        }
        case FieldType::OLAP_FIELD_TYPE_VARIANT: {
            // Read variant only root data using a single ColumnReader
            std::unique_ptr<ColumnReader> reader_local(
                    new ColumnReader(opts, meta, num_rows, file_reader));
            RETURN_IF_ERROR(reader_local->init(&meta));
            *reader = std::move(reader_local);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnReader: {}",
                                        std::to_string(int(type)));
        }
    }
}

ColumnReader::ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                           uint64_t num_rows, io::FileReaderSPtr file_reader)
        : _use_index_page_cache(!config::disable_storage_page_cache),
          _opts(opts),
          _num_rows(num_rows),
          _file_reader(std::move(file_reader)),
          _dict_encoding_type(UNKNOWN_DICT_ENCODING) {
    _meta_length = meta.length();
    _meta_type = (FieldType)meta.type();
    if (_meta_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        _meta_children_column_type = (FieldType)meta.children_columns(0).type();
    }
    _meta_is_nullable = meta.is_nullable();
    _meta_dict_page = meta.dict_page();
    _meta_compression = meta.compression();
}

ColumnReader::~ColumnReader() = default;

int64_t ColumnReader::get_metadata_size() const {
    return sizeof(ColumnReader) + (_segment_zone_map ? _segment_zone_map->ByteSizeLong() : 0);
}

Status ColumnReader::init(const ColumnMetaPB* meta) {
    _type_info = get_type_info(meta);

    if (meta->has_be_exec_version()) {
        _be_exec_version = meta->be_exec_version();
    }

    if (_type_info == nullptr) {
        return Status::NotSupported("unsupported typeinfo, type={}", meta->type());
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info.get(), meta->encoding(), &_encoding_info));

    for (int i = 0; i < meta->indexes_size(); i++) {
        const auto& index_meta = meta->indexes(i);
        switch (index_meta.type()) {
        case ORDINAL_INDEX:
            _ordinal_index.reset(
                    new OrdinalIndexReader(_file_reader, _num_rows, index_meta.ordinal_index()));
            break;
        case ZONE_MAP_INDEX:
            _segment_zone_map =
                    std::make_unique<ZoneMapPB>(index_meta.zone_map_index().segment_zone_map());
            _zone_map_index.reset(new ZoneMapIndexReader(
                    _file_reader, index_meta.zone_map_index().page_zone_maps()));
            break;
        case BITMAP_INDEX:
            _bitmap_index.reset(new BitmapIndexReader(_file_reader, index_meta.bitmap_index()));
            break;
        case BLOOM_FILTER_INDEX:
            _bloom_filter_index.reset(
                    new BloomFilterIndexReader(_file_reader, index_meta.bloom_filter_index()));
            break;
        default:
            return Status::Corruption("Bad file {}: invalid column index type {}",
                                      _file_reader->path().native(), index_meta.type());
        }
    }
    update_metadata_size();

    // ArrayColumnWriter writes a single empty array and flushes. In this scenario,
    // the item writer doesn't write any data and the corresponding ordinal index is empty.
    if (_ordinal_index == nullptr && !is_empty()) {
        return Status::Corruption("Bad file {}: missing ordinal index for column {}",
                                  _file_reader->path().native(), meta->column_id());
    }

    return Status::OK();
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_load_bitmap_index(_use_index_page_cache, _opts.kept_in_memory));
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::new_index_iterator(std::shared_ptr<IndexFileReader> index_file_reader,
                                        const TabletIndex* index_meta,
                                        const StorageReadOptions& read_options,
                                        std::unique_ptr<IndexIterator>* iterator) {
    RETURN_IF_ERROR(_ensure_index_loaded(std::move(index_file_reader), index_meta));
    {
        std::shared_lock<std::shared_mutex> rlock(_load_index_lock);
        if (_index_reader) {
            RETURN_IF_ERROR(_index_reader->new_iterator(read_options.io_ctx, read_options.stats,
                                                        read_options.runtime_state, iterator));
        }
    }
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                               PageHandle* handle, Slice* page_body, PageFooterPB* footer,
                               BlockCompressionCodec* codec) const {
    iter_opts.sanity_check();
    PageReadOptions opts(iter_opts.io_ctx);
    opts.verify_checksum = _opts.verify_checksum;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.kept_in_memory = _opts.kept_in_memory;
    opts.type = iter_opts.type;
    opts.file_reader = iter_opts.file_reader;
    opts.page_pointer = pp;
    opts.codec = codec;
    opts.stats = iter_opts.stats;
    opts.encoding_info = _encoding_info;

    // index page should not pre decode
    if (iter_opts.type == INDEX_PAGE) opts.pre_decode = false;
    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::get_row_ranges_by_zone_map(
        const AndBlockColumnPredicate* col_predicates,
        const std::vector<const ColumnPredicate*>* delete_predicates, RowRanges* row_ranges,
        const ColumnIteratorOptions& iter_opts) {
    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(
            _get_filtered_pages(col_predicates, delete_predicates, &page_indexes, iter_opts));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges, iter_opts));
    return Status::OK();
}

Status ColumnReader::next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) const {
    if (_segment_zone_map == nullptr) {
        return Status::InternalError("segment zonemap not exist");
    }
    // TODO: this work to get min/max value seems should only do once
    FieldType type = _type_info->type();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta_length));
    RETURN_IF_ERROR(
            _parse_zone_map_skip_null(*_segment_zone_map, min_value.get(), max_value.get()));

    dst->reserve(*n);
    bool is_string = is_olap_string_type(type);
    if (max_value->is_null()) {
        assert_cast<vectorized::ColumnNullable&>(*dst).insert_default();
    } else {
        if (is_string) {
            auto sv = (StringRef*)max_value->cell_ptr();
            dst->insert_data(sv->data, sv->size);
        } else {
            dst->insert_many_fix_len_data(static_cast<const char*>(max_value->cell_ptr()), 1);
        }
    }

    auto size = *n - 1;
    if (min_value->is_null()) {
        assert_cast<vectorized::ColumnNullable&>(*dst).insert_many_defaults(size);
    } else {
        if (is_string) {
            auto sv = (StringRef*)min_value->cell_ptr();
            dst->insert_data_repeatedly(sv->data, sv->size, size);
        } else {
            // TODO: the work may cause performance problem, opt latter
            for (int i = 0; i < size; ++i) {
                dst->insert_many_fix_len_data(static_cast<const char*>(min_value->cell_ptr()), 1);
            }
        }
    }

    return Status::OK();
}

bool ColumnReader::match_condition(const AndBlockColumnPredicate* col_predicates) const {
    if (_zone_map_index == nullptr) {
        return true;
    }
    FieldType type = _type_info->type();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta_length));
    RETURN_FALSE_IF_ERROR(_parse_zone_map(*_segment_zone_map, min_value.get(), max_value.get()));

    return _zone_map_match_condition(*_segment_zone_map, min_value.get(), max_value.get(),
                                     col_predicates);
}

bool ColumnReader::prune_predicates_by_zone_map(std::vector<ColumnPredicate*>& predicates,
                                                const int column_id) const {
    if (_zone_map_index == nullptr) {
        return false;
    }

    FieldType type = _type_info->type();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta_length));
    RETURN_FALSE_IF_ERROR(_parse_zone_map(*_segment_zone_map, min_value.get(), max_value.get()));

    auto pruned = false;
    for (auto it = predicates.begin(); it != predicates.end();) {
        auto predicate = *it;
        if (predicate->column_id() == column_id &&
            predicate->is_always_true({min_value.get(), max_value.get()})) {
            pruned = true;
            it = predicates.erase(it);
        } else {
            ++it;
        }
    }
    return pruned;
}

Status ColumnReader::_parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                     WrapperField* max_value_container) const {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        RETURN_IF_ERROR(min_value_container->from_string(zone_map.min()));
        RETURN_IF_ERROR(max_value_container->from_string(zone_map.max()));
    }
    // for compatible original Cond eval logic
    if (zone_map.has_null()) {
        // for compatible, if exist null, original logic treat null as min
        min_value_container->set_null();
        if (!zone_map.has_not_null()) {
            // for compatible OlapCond's 'is not null'
            max_value_container->set_null();
        }
    }
    return Status::OK();
}

Status ColumnReader::_parse_zone_map_skip_null(const ZoneMapPB& zone_map,
                                               WrapperField* min_value_container,
                                               WrapperField* max_value_container) const {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        RETURN_IF_ERROR(min_value_container->from_string(zone_map.min()));
        RETURN_IF_ERROR(max_value_container->from_string(zone_map.max()));
    } else {
        min_value_container->set_null();
        max_value_container->set_null();
    }
    return Status::OK();
}

bool ColumnReader::_zone_map_match_condition(const ZoneMapPB& zone_map,
                                             WrapperField* min_value_container,
                                             WrapperField* max_value_container,
                                             const AndBlockColumnPredicate* col_predicates) const {
    if (!zone_map.has_not_null() && !zone_map.has_null()) {
        return false; // no data in this zone
    }

    if (zone_map.pass_all() || min_value_container == nullptr || max_value_container == nullptr) {
        return true;
    }

    return col_predicates->evaluate_and({min_value_container, max_value_container});
}

Status ColumnReader::_get_filtered_pages(
        const AndBlockColumnPredicate* col_predicates,
        const std::vector<const ColumnPredicate*>* delete_predicates,
        std::vector<uint32_t>* page_indexes, const ColumnIteratorOptions& iter_opts) {
    RETURN_IF_ERROR(_load_zone_map_index(_use_index_page_cache, _opts.kept_in_memory, iter_opts));

    FieldType type = _type_info->type();
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps();
    int32_t page_size = _zone_map_index->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta_length));
    for (int32_t i = 0; i < page_size; ++i) {
        if (zone_maps[i].pass_all()) {
            page_indexes->push_back(i);
        } else {
            RETURN_IF_ERROR(_parse_zone_map(zone_maps[i], min_value.get(), max_value.get()));
            if (_zone_map_match_condition(zone_maps[i], min_value.get(), max_value.get(),
                                          col_predicates)) {
                bool should_read = true;
                if (delete_predicates != nullptr) {
                    for (auto del_pred : *delete_predicates) {
                        // TODO: Both `min_value` and `max_value` should be 0 or neither should be 0.
                        //  So nullable only need to judge once.
                        if (min_value != nullptr && max_value != nullptr &&
                            del_pred->evaluate_del({min_value.get(), max_value.get()})) {
                            should_read = false;
                            break;
                        }
                    }
                }
                if (should_read) {
                    page_indexes->push_back(i);
                }
            }
        }
    }
    VLOG(1) << "total-pages: " << page_size << " not-filtered-pages: " << page_indexes->size()
            << " filtered-percent:"
            << 1.0 - (static_cast<double>(page_indexes->size()) / (page_size * 1.0));
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes,
                                           RowRanges* row_ranges,
                                           const ColumnIteratorOptions& iter_opts) {
    row_ranges->clear();
    RETURN_IF_ERROR(_load_ordinal_index(_use_index_page_cache, _opts.kept_in_memory, iter_opts));
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                                    RowRanges* row_ranges,
                                                    const ColumnIteratorOptions& iter_opts) {
    RETURN_IF_ERROR(_load_ordinal_index(_use_index_page_cache, _opts.kept_in_memory, iter_opts));
    RETURN_IF_ERROR(
            _load_bloom_filter_index(_use_index_page_cache, _opts.kept_in_memory, iter_opts));
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter, iter_opts.stats));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<uint32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index->seek_at_or_before(from);
        while (idx < to && iter.valid()) {
            page_ids.insert(iter.page_index());
            idx = iter.last_ordinal() + 1;
            iter.next();
        }
    }
    for (auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (col_predicates->evaluate_and(bf.get())) {
            bf_row_ranges.add(RowRange(_ordinal_index->get_first_ordinal(pid),
                                       _ordinal_index->get_last_ordinal(pid) + 1));
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges);
    return Status::OK();
}

Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory,
                                         const ColumnIteratorOptions& iter_opts) {
    if (!_ordinal_index) {
        return Status::InternalError("ordinal_index not inited");
    }
    return _ordinal_index->load(use_page_cache, kept_in_memory, iter_opts.stats);
}

Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory,
                                          const ColumnIteratorOptions& iter_opts) {
    if (_zone_map_index != nullptr) {
        return _zone_map_index->load(use_page_cache, kept_in_memory, iter_opts.stats);
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    if (_bitmap_index != nullptr) {
        return _bitmap_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::_load_index(std::shared_ptr<IndexFileReader> index_file_reader,
                                 const TabletIndex* index_meta) {
    std::unique_lock<std::shared_mutex> wlock(_load_index_lock);

    if (_index_reader != nullptr && index_meta &&
        _index_reader->get_index_id() == index_meta->index_id()) {
        return Status::OK();
    }

    bool should_analyzer =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(index_meta->properties());

    FieldType type;
    if (_meta_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        type = _meta_children_column_type;
    } else {
        type = _type_info->type();
    }

    if (is_string_type(type)) {
        if (should_analyzer) {
            try {
                _index_reader = FullTextIndexReader::create_shared(index_meta, index_file_reader);
            } catch (const CLuceneError& e) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "create FullTextIndexReader error: {}", e.what());
            }
        } else {
            try {
                _index_reader =
                        StringTypeInvertedIndexReader::create_shared(index_meta, index_file_reader);
            } catch (const CLuceneError& e) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "create StringTypeInvertedIndexReader error: {}", e.what());
            }
        }
    } else if (is_numeric_type(type)) {
        try {
            _index_reader = BkdIndexReader::create_shared(index_meta, index_file_reader);
        } catch (const CLuceneError& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "create BkdIndexReader error: {}", e.what());
        }
    } else {
        _index_reader.reset();
    }
    // TODO: move has null to inverted_index_reader's query function
    //bool has_null = true;
    //RETURN_IF_ERROR(index_file_reader->has_null(index_meta, &has_null));
    //_inverted_index->set_has_null(has_null);
    return Status::OK();
}

bool ColumnReader::has_bloom_filter_index(bool ngram) const {
    if (_bloom_filter_index == nullptr) return false;

    if (ngram) {
        return _bloom_filter_index->algorithm() == BloomFilterAlgorithmPB::NGRAM_BLOOM_FILTER;
    } else {
        return _bloom_filter_index->algorithm() != BloomFilterAlgorithmPB::NGRAM_BLOOM_FILTER;
    }
}

Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory,
                                              const ColumnIteratorOptions& iter_opts) {
    if (_bloom_filter_index != nullptr) {
        return _bloom_filter_index->load(use_page_cache, kept_in_memory, iter_opts.stats);
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter,
                                       const ColumnIteratorOptions& iter_opts) {
    RETURN_IF_ERROR(_load_ordinal_index(_use_index_page_cache, _opts.kept_in_memory, iter_opts));
    *iter = _ordinal_index->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to ordinal {}, ", ordinal);
    }
    return Status::OK();
}

Status ColumnReader::new_iterator(ColumnIterator** iterator, const TabletColumn* tablet_column) {
    if (is_empty()) {
        *iterator = new EmptyFileColumnIterator();
        return Status::OK();
    }
    if (is_scalar_type((FieldType)_meta_type)) {
        *iterator = new FileColumnIterator(this);
        return Status::OK();
    } else {
        auto type = (FieldType)_meta_type;
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_AGG_STATE: {
            return new_agg_state_iterator(iterator);
        }
        case FieldType::OLAP_FIELD_TYPE_STRUCT: {
            return new_struct_iterator(iterator, tablet_column);
        }
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            return new_array_iterator(iterator, tablet_column);
        }
        case FieldType::OLAP_FIELD_TYPE_MAP: {
            return new_map_iterator(iterator, tablet_column);
        }
        case FieldType::OLAP_FIELD_TYPE_VARIANT: {
            *iterator = new VariantRootColumnIterator(new FileColumnIterator(this));
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type to create iterator: {}",
                                        std::to_string(int(type)));
        }
    }
}

Status ColumnReader::new_agg_state_iterator(ColumnIterator** iterator) {
    *iterator = new FileColumnIterator(this);
    return Status::OK();
}

Status ColumnReader::new_array_iterator(ColumnIterator** iterator,
                                        const TabletColumn* tablet_column) {
    ColumnIterator* item_iterator = nullptr;
    RETURN_IF_ERROR(_sub_readers[0]->new_iterator(
            &item_iterator, tablet_column && tablet_column->get_subtype_count() > 0
                                    ? &tablet_column->get_sub_column(0)
                                    : nullptr));

    ColumnIterator* offset_iterator = nullptr;
    RETURN_IF_ERROR(_sub_readers[1]->new_iterator(&offset_iterator, nullptr));
    auto* ofcIter =
            new OffsetFileColumnIterator(reinterpret_cast<FileColumnIterator*>(offset_iterator));

    ColumnIterator* null_iterator = nullptr;
    if (is_nullable()) {
        RETURN_IF_ERROR(_sub_readers[2]->new_iterator(&null_iterator, nullptr));
    }
    *iterator = new ArrayFileColumnIterator(this, ofcIter, item_iterator, null_iterator);
    return Status::OK();
}

Status ColumnReader::new_map_iterator(ColumnIterator** iterator,
                                      const TabletColumn* tablet_column) {
    ColumnIterator* key_iterator = nullptr;
    RETURN_IF_ERROR(_sub_readers[0]->new_iterator(
            &key_iterator, tablet_column && tablet_column->get_subtype_count() > 1
                                   ? &tablet_column->get_sub_column(0)
                                   : nullptr));
    ColumnIterator* val_iterator = nullptr;
    RETURN_IF_ERROR(_sub_readers[1]->new_iterator(
            &val_iterator, tablet_column && tablet_column->get_subtype_count() > 1
                                   ? &tablet_column->get_sub_column(1)
                                   : nullptr));
    ColumnIterator* offsets_iterator = nullptr;
    RETURN_IF_ERROR(_sub_readers[2]->new_iterator(&offsets_iterator, nullptr));
    auto* ofcIter =
            new OffsetFileColumnIterator(reinterpret_cast<FileColumnIterator*>(offsets_iterator));

    ColumnIterator* null_iterator = nullptr;
    if (is_nullable()) {
        RETURN_IF_ERROR(_sub_readers[3]->new_iterator(&null_iterator, nullptr));
    }
    *iterator = new MapFileColumnIterator(this, null_iterator, ofcIter, key_iterator, val_iterator);
    return Status::OK();
}

Status ColumnReader::new_struct_iterator(ColumnIterator** iterator,
                                         const TabletColumn* tablet_column) {
    std::vector<ColumnIterator*> sub_column_iterators;
    size_t child_size = is_nullable() ? _sub_readers.size() - 1 : _sub_readers.size();
    size_t tablet_column_size = tablet_column ? tablet_column->get_sub_columns().size() : 0;
    sub_column_iterators.reserve(child_size);

    ColumnIterator* sub_column_iterator;
    for (uint64_t i = 0; i < child_size; i++) {
        RETURN_IF_ERROR(_sub_readers[i]->new_iterator(
                &sub_column_iterator, tablet_column ? &tablet_column->get_sub_column(i) : nullptr));
        sub_column_iterators.push_back(sub_column_iterator);
    }

    // create default_iterator for schema-change behavior which increase column
    for (size_t i = child_size; i < tablet_column_size; i++) {
        TabletColumn column = tablet_column->get_sub_column(i);
        std::unique_ptr<ColumnIterator> it;
        RETURN_IF_ERROR(Segment::new_default_iterator(column, &it));
        sub_column_iterators.push_back(it.get());
        it.release();
    }

    ColumnIterator* null_iterator = nullptr;
    if (is_nullable()) {
        RETURN_IF_ERROR(_sub_readers[child_size]->new_iterator(&null_iterator, nullptr));
    }
    *iterator = new StructFileColumnIterator(this, null_iterator, sub_column_iterators);
    return Status::OK();
}

///====================== MapFileColumnIterator ============================////
MapFileColumnIterator::MapFileColumnIterator(ColumnReader* reader, ColumnIterator* null_iterator,
                                             OffsetFileColumnIterator* offsets_iterator,
                                             ColumnIterator* key_iterator,
                                             ColumnIterator* val_iterator)
        : _map_reader(reader) {
    _key_iterator.reset(key_iterator);
    _val_iterator.reset(val_iterator);
    _offsets_iterator.reset(offsets_iterator);

    if (_map_reader->is_nullable()) {
        _null_iterator.reset(null_iterator);
    }
}

Status MapFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_key_iterator->init(opts));
    RETURN_IF_ERROR(_val_iterator->init(opts));
    RETURN_IF_ERROR(_offsets_iterator->init(opts));
    if (_map_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    return Status::OK();
}

Status MapFileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    if (_map_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->seek_to_ordinal(ord));
    }
    RETURN_IF_ERROR(_offsets_iterator->seek_to_ordinal(ord));
    // here to use offset info
    ordinal_t offset = 0;
    RETURN_IF_ERROR(_offsets_iterator->_peek_one_offset(&offset));
    RETURN_IF_ERROR(_key_iterator->seek_to_ordinal(offset));
    RETURN_IF_ERROR(_val_iterator->seek_to_ordinal(offset));
    return Status::OK();
}

Status MapFileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                         bool* has_null) {
    const auto* column_map = vectorized::check_and_get_column<vectorized::ColumnMap>(
            dst->is_nullable() ? static_cast<vectorized::ColumnNullable&>(*dst).get_nested_column()
                               : *dst);
    auto column_offsets_ptr = column_map->get_offsets_column().assume_mutable();
    bool offsets_has_null = false;
    ssize_t start = column_offsets_ptr->size();
    RETURN_IF_ERROR(_offsets_iterator->next_batch(n, column_offsets_ptr, &offsets_has_null));
    if (*n == 0) {
        return Status::OK();
    }
    auto& column_offsets =
            static_cast<vectorized::ColumnArray::ColumnOffsets&>(*column_offsets_ptr);
    RETURN_IF_ERROR(_offsets_iterator->_calculate_offsets(start, column_offsets));
    DCHECK(column_offsets.get_data().back() >= column_offsets.get_data()[start - 1]);
    size_t num_items =
            column_offsets.get_data().back() - column_offsets.get_data()[start - 1]; // -1 is valid
    auto key_ptr = column_map->get_keys().assume_mutable();
    auto val_ptr = column_map->get_values().assume_mutable();

    if (num_items > 0) {
        size_t num_read = num_items;
        bool key_has_null = false;
        bool val_has_null = false;
        RETURN_IF_ERROR(_key_iterator->next_batch(&num_read, key_ptr, &key_has_null));
        RETURN_IF_ERROR(_val_iterator->next_batch(&num_read, val_ptr, &val_has_null));
        DCHECK(num_read == num_items);
    }

    if (dst->is_nullable()) {
        size_t num_read = *n;
        auto null_map_ptr =
                static_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column_ptr();
        // in not-null to null linked-schemachange mode,
        // actually we do not change dat data include meta in footer,
        // so may dst from changed meta which is nullable but old data is not nullable,
        // if so, we should set null_map to all null by default
        if (_null_iterator) {
            bool null_signs_has_null = false;
            RETURN_IF_ERROR(
                    _null_iterator->next_batch(&num_read, null_map_ptr, &null_signs_has_null));
        } else {
            auto& null_map = assert_cast<vectorized::ColumnUInt8&>(*null_map_ptr);
            null_map.insert_many_vals(0, num_read);
        }
        DCHECK(num_read == *n);
    }
    return Status::OK();
}

Status MapFileColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                             vectorized::MutableColumnPtr& dst) {
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(seek_to_ordinal(rowids[i]));
        size_t num_read = 1;
        RETURN_IF_ERROR(next_batch(&num_read, dst, nullptr));
        DCHECK(num_read == 1);
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

StructFileColumnIterator::StructFileColumnIterator(
        ColumnReader* reader, ColumnIterator* null_iterator,
        std::vector<ColumnIterator*>& sub_column_iterators)
        : _struct_reader(reader) {
    _sub_column_iterators.resize(sub_column_iterators.size());
    for (size_t i = 0; i < sub_column_iterators.size(); i++) {
        _sub_column_iterators[i].reset(sub_column_iterators[i]);
    }
    if (_struct_reader->is_nullable()) {
        _null_iterator.reset(null_iterator);
    }
}

Status StructFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    for (auto& column_iterator : _sub_column_iterators) {
        RETURN_IF_ERROR(column_iterator->init(opts));
    }
    if (_struct_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    return Status::OK();
}

Status StructFileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                            bool* has_null) {
    const auto* column_struct = vectorized::check_and_get_column<vectorized::ColumnStruct>(
            dst->is_nullable() ? static_cast<vectorized::ColumnNullable&>(*dst).get_nested_column()
                               : *dst);
    for (size_t i = 0; i < column_struct->tuple_size(); i++) {
        size_t num_read = *n;
        auto sub_column_ptr = column_struct->get_column(i).assume_mutable();
        bool column_has_null = false;
        RETURN_IF_ERROR(
                _sub_column_iterators[i]->next_batch(&num_read, sub_column_ptr, &column_has_null));
        DCHECK(num_read == *n);
    }

    if (dst->is_nullable()) {
        size_t num_read = *n;
        auto null_map_ptr =
                static_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column_ptr();
        // in not-null to null linked-schemachange mode,
        // actually we do not change dat data include meta in footer,
        // so may dst from changed meta which is nullable but old data is not nullable,
        // if so, we should set null_map to all null by default
        if (_null_iterator) {
            bool null_signs_has_null = false;
            RETURN_IF_ERROR(
                    _null_iterator->next_batch(&num_read, null_map_ptr, &null_signs_has_null));
        } else {
            auto& null_map = assert_cast<vectorized::ColumnUInt8&>(*null_map_ptr);
            null_map.insert_many_vals(0, num_read);
        }
        DCHECK(num_read == *n);
    }

    return Status::OK();
}

Status StructFileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    for (auto& column_iterator : _sub_column_iterators) {
        RETURN_IF_ERROR(column_iterator->seek_to_ordinal(ord));
    }
    if (_struct_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status StructFileColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                vectorized::MutableColumnPtr& dst) {
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(seek_to_ordinal(rowids[i]));
        size_t num_read = 1;
        RETURN_IF_ERROR(next_batch(&num_read, dst, nullptr));
        DCHECK(num_read == 1);
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
Status OffsetFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_offset_iterator->init(opts));
    return Status::OK();
}

Status OffsetFileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                            bool* has_null) {
    RETURN_IF_ERROR(_offset_iterator->next_batch(n, dst, has_null));
    return Status::OK();
}

Status OffsetFileColumnIterator::_peek_one_offset(ordinal_t* offset) {
    if (_offset_iterator->get_current_page()->has_remaining()) {
        PageDecoder* offset_page_decoder = _offset_iterator->get_current_page()->data_decoder.get();
        vectorized::MutableColumnPtr offset_col = vectorized::ColumnOffset64::create();
        size_t n = 1;
        RETURN_IF_ERROR(offset_page_decoder->peek_next_batch(&n, offset_col)); // not null
        DCHECK(offset_col->size() == 1);
        *offset = assert_cast<const vectorized::ColumnOffset64*>(offset_col.get())->get_element(0);
    } else {
        *offset = _offset_iterator->get_current_page()->next_array_item_ordinal;
    }
    return Status::OK();
}

/**
 *  first_storage_offset read from page should smaller than next_storage_offset which here call _peek_one_offset from page,
    and first_column_offset is keep in memory data which is different dimension with (first_storage_offset and next_storage_offset)
     eg. step1. read page: first_storage_offset = 16382
         step2. read page below with _peek_one_offset(&last_offset): last_offset = 16387
         step3. first_offset = 126 which is calculate in column offsets
         for loop column offsets element in size
            we can calculate from first_storage_offset to next_storage_offset one by one to fill with offsets_data in memory column offsets
 * @param start
 * @param column_offsets
 * @return
 */
Status OffsetFileColumnIterator::_calculate_offsets(
        ssize_t start, vectorized::ColumnArray::ColumnOffsets& column_offsets) {
    ordinal_t next_storage_offset = 0;
    RETURN_IF_ERROR(_peek_one_offset(&next_storage_offset));

    // calculate real offsets
    auto& offsets_data = column_offsets.get_data();
    ordinal_t first_column_offset = offsets_data[start - 1]; // -1 is valid
    ordinal_t first_storage_offset = offsets_data[start];
    DCHECK(next_storage_offset >= first_storage_offset);
    for (ssize_t i = start; i < offsets_data.size() - 1; ++i) {
        offsets_data[i] = first_column_offset + (offsets_data[i + 1] - first_storage_offset);
    }
    // last offset
    offsets_data[offsets_data.size() - 1] =
            first_column_offset + (next_storage_offset - first_storage_offset);
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
ArrayFileColumnIterator::ArrayFileColumnIterator(ColumnReader* reader,
                                                 OffsetFileColumnIterator* offset_reader,
                                                 ColumnIterator* item_iterator,
                                                 ColumnIterator* null_iterator)
        : _array_reader(reader) {
    _offset_iterator.reset(offset_reader);
    _item_iterator.reset(item_iterator);
    if (_array_reader->is_nullable()) {
        _null_iterator.reset(null_iterator);
    }
}

Status ArrayFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_offset_iterator->init(opts));
    RETURN_IF_ERROR(_item_iterator->init(opts));
    if (_array_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    return Status::OK();
}

Status ArrayFileColumnIterator::_seek_by_offsets(ordinal_t ord) {
    // using offsets info
    ordinal_t offset = 0;
    RETURN_IF_ERROR(_offset_iterator->_peek_one_offset(&offset));
    RETURN_IF_ERROR(_item_iterator->seek_to_ordinal(offset));
    return Status::OK();
}

Status ArrayFileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(_offset_iterator->seek_to_ordinal(ord));
    if (_array_reader->is_nullable()) {
        RETURN_IF_ERROR(_null_iterator->seek_to_ordinal(ord));
    }
    return _seek_by_offsets(ord);
}

Status ArrayFileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                           bool* has_null) {
    const auto* column_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            dst->is_nullable() ? static_cast<vectorized::ColumnNullable&>(*dst).get_nested_column()
                               : *dst);

    bool offsets_has_null = false;
    auto column_offsets_ptr = column_array->get_offsets_column().assume_mutable();
    ssize_t start = column_offsets_ptr->size();
    RETURN_IF_ERROR(_offset_iterator->next_batch(n, column_offsets_ptr, &offsets_has_null));
    if (*n == 0) {
        return Status::OK();
    }
    auto& column_offsets =
            static_cast<vectorized::ColumnArray::ColumnOffsets&>(*column_offsets_ptr);
    RETURN_IF_ERROR(_offset_iterator->_calculate_offsets(start, column_offsets));
    size_t num_items =
            column_offsets.get_data().back() - column_offsets.get_data()[start - 1]; // -1 is valid
    auto column_items_ptr = column_array->get_data().assume_mutable();
    if (num_items > 0) {
        size_t num_read = num_items;
        bool items_has_null = false;
        RETURN_IF_ERROR(_item_iterator->next_batch(&num_read, column_items_ptr, &items_has_null));
        DCHECK(num_read == num_items);
    }

    if (dst->is_nullable()) {
        auto null_map_ptr =
                static_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column_ptr();
        size_t num_read = *n;
        // in not-null to null linked-schemachange mode,
        // actually we do not change dat data include meta in footer,
        // so may dst from changed meta which is nullable but old data is not nullable,
        // if so, we should set null_map to all null by default
        if (_null_iterator) {
            bool null_signs_has_null = false;
            RETURN_IF_ERROR(
                    _null_iterator->next_batch(&num_read, null_map_ptr, &null_signs_has_null));
        } else {
            auto& null_map = assert_cast<vectorized::ColumnUInt8&>(*null_map_ptr);
            null_map.insert_many_vals(0, num_read);
        }
        DCHECK(num_read == *n);
    }

    return Status::OK();
}

Status ArrayFileColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                               vectorized::MutableColumnPtr& dst) {
    for (size_t i = 0; i < count; ++i) {
        // TODO(cambyzju): now read array one by one, need optimize later
        RETURN_IF_ERROR(seek_to_ordinal(rowids[i]));
        size_t num_read = 1;
        RETURN_IF_ERROR(next_batch(&num_read, dst, nullptr));
        DCHECK(num_read == 1);
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {}

Status FileColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    if (!_opts.use_page_cache) {
        _reader->disable_index_meta_cache();
    }
    RETURN_IF_ERROR(get_block_compression_codec(_reader->get_compression(), &_compress_codec));
    if (config::enable_low_cardinality_optimize &&
        opts.io_ctx.reader_type == ReaderType::READER_QUERY &&
        _reader->encoding_info()->encoding() == DICT_ENCODING) {
        auto dict_encoding_type = _reader->get_dict_encoding_type();
        // Only if the column is a predicate column, then we need check the all dict encoding flag
        // because we could rewrite the predciate to accelarate query speed. But if it is not a
        // predicate column, then it is useless. And it has a bad impact on cold read(first time read)
        // because it will load the column's ordinal index and zonemap index and maybe other indices.
        // it has bad impact on primary key query. For example, select * from table where pk = 1, and
        // the table has 2000 columns.
        if (dict_encoding_type == ColumnReader::UNKNOWN_DICT_ENCODING && opts.is_predicate_column) {
            RETURN_IF_ERROR(seek_to_ordinal(_reader->num_rows() - 1));
            _is_all_dict_encoding = _page.is_dict_encoding;
            _reader->set_dict_encoding_type(_is_all_dict_encoding
                                                    ? ColumnReader::ALL_DICT_ENCODING
                                                    : ColumnReader::PARTIAL_DICT_ENCODING);
        } else {
            _is_all_dict_encoding = dict_encoding_type == ColumnReader::ALL_DICT_ENCODING;
        }
    }
    return Status::OK();
}

FileColumnIterator::~FileColumnIterator() = default;

Status FileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (!_page || !_page.contains(ord) || !_page_iter.valid()) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter, _opts));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    RETURN_IF_ERROR(_seek_to_pos_in_page(&_page, ord - _page.first_ordinal));
    _current_ordinal = ord;
    return Status::OK();
}

Status FileColumnIterator::seek_to_page_start() {
    return seek_to_ordinal(_page.first_ordinal);
}

Status FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) const {
    if (page->offset_in_page == offset_in_page) {
        // fast path, do nothing
        return Status::OK();
    }

    ordinal_t pos_in_data = offset_in_page;
    if (_page.has_null) {
        ordinal_t offset_in_data = 0;
        ordinal_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data,
                                                  cast_set<int>(page->null_bitmap.size), 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    RETURN_IF_ERROR(page->data_decoder->seek_to_position_in_page(pos_in_data));
    page->offset_in_page = offset_in_page;
    return Status::OK();
}

Status FileColumnIterator::next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) {
    return _reader->next_batch_of_zone_map(n, dst);
}

Status FileColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                      bool* has_null) {
    size_t curr_size = dst->byte_size();
    dst->reserve(*n);
    size_t remaining = *n;
    *has_null = false;
    while (remaining > 0) {
        if (!_page.has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page.remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_page.has_null) {
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page.null_decoder.GetNextRun(&is_null, nrows_to_read);
                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_page.data_decoder->next_batch(&num_rows, dst));
                    DCHECK_EQ(this_run, num_rows);
                } else {
                    *has_null = true;
                    const auto* null_col =
                            vectorized::check_and_get_column<vectorized::ColumnNullable>(dst.get());
                    if (null_col != nullptr) {
                        const_cast<vectorized::ColumnNullable*>(null_col)->insert_many_defaults(
                                this_run);
                    } else {
                        return Status::InternalError("unexpected column type in column reader");
                    }
                }

                nrows_to_read -= this_run;
                _page.offset_in_page += this_run;
                _current_ordinal += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page.data_decoder->next_batch(&nrows_to_read, dst));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            _page.offset_in_page += nrows_to_read;
            _current_ordinal += nrows_to_read;
        }
        remaining -= nrows_in_page;
    }
    *n -= remaining;
    _opts.stats->bytes_read += (dst->byte_size() - curr_size) + BitmapSize(*n);
    return Status::OK();
}

Status FileColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                          vectorized::MutableColumnPtr& dst) {
    size_t remaining = count;
    size_t total_read_count = 0;
    size_t nrows_to_read = 0;
    while (remaining > 0) {
        RETURN_IF_ERROR(seek_to_ordinal(rowids[total_read_count]));

        // number of rows to be read from this page
        nrows_to_read = std::min(remaining, _page.remaining());

        if (_page.has_null) {
            size_t already_read = 0;
            while ((nrows_to_read - already_read) > 0) {
                bool is_null = false;
                size_t this_run = std::min(nrows_to_read - already_read, _page.remaining());
                if (UNLIKELY(this_run == 0)) {
                    break;
                }
                this_run = _page.null_decoder.GetNextRun(&is_null, this_run);
                size_t offset = total_read_count + already_read;
                size_t this_read_count = 0;
                rowid_t current_ordinal_in_page =
                        cast_set<uint32_t>(_page.offset_in_page + _page.first_ordinal);
                for (size_t i = 0; i < this_run; ++i) {
                    if (rowids[offset + i] - current_ordinal_in_page >= this_run) {
                        break;
                    }
                    this_read_count++;
                }

                auto origin_index = _page.data_decoder->current_index();
                if (this_read_count > 0) {
                    if (is_null) {
                        const auto* null_col =
                                vectorized::check_and_get_column<vectorized::ColumnNullable>(
                                        dst.get());
                        if (UNLIKELY(null_col == nullptr)) {
                            return Status::InternalError("unexpected column type in column reader");
                        }

                        const_cast<vectorized::ColumnNullable*>(null_col)->insert_many_defaults(
                                this_read_count);
                    } else {
                        size_t read_count = this_read_count;

                        // ordinal in nullable columns' data buffer maybe be not continuously(the data doesn't contain null value),
                        // so we need use `page_start_off_in_decoder` to calculate the actual offset in `data_decoder`
                        size_t page_start_off_in_decoder =
                                _page.first_ordinal + _page.offset_in_page - origin_index;
                        RETURN_IF_ERROR(_page.data_decoder->read_by_rowids(
                                &rowids[offset], page_start_off_in_decoder, &read_count, dst));
                        DCHECK_EQ(read_count, this_read_count);
                    }
                }

                if (!is_null) {
                    RETURN_IF_ERROR(
                            _page.data_decoder->seek_to_position_in_page(origin_index + this_run));
                }

                already_read += this_read_count;
                _page.offset_in_page += this_run;
                DCHECK(_page.offset_in_page <= _page.num_rows);
            }

            nrows_to_read = already_read;
            total_read_count += nrows_to_read;
            remaining -= nrows_to_read;
        } else {
            RETURN_IF_ERROR(_page.data_decoder->read_by_rowids(
                    &rowids[total_read_count], _page.first_ordinal, &nrows_to_read, dst));
            total_read_count += nrows_to_read;
            remaining -= nrows_to_read;
        }
    }
    return Status::OK();
}

Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_data_page(_page_iter));
    RETURN_IF_ERROR(_seek_to_pos_in_page(&_page, 0));
    *eos = false;
    return Status::OK();
}

Status FileColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    _opts.type = DATA_PAGE;
    RETURN_IF_ERROR(
            _reader->read_page(_opts, iter.page(), &handle, &page_body, &footer, _compress_codec));
    // parse data page
    RETURN_IF_ERROR(ParsedPage::create(std::move(handle), page_body, footer.data_page_footer(),
                                       _reader->encoding_info(), iter.page(), iter.page_index(),
                                       &_page));

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    if (_reader->encoding_info()->encoding() == DICT_ENCODING) {
        auto dict_page_decoder = reinterpret_cast<BinaryDictPageDecoder*>(_page.data_decoder.get());
        if (dict_page_decoder->is_dict_encoding()) {
            if (_dict_decoder == nullptr) {
                RETURN_IF_ERROR(_read_dict_data());
                CHECK_NOTNULL(_dict_decoder);
            }

            dict_page_decoder->set_dict_decoder(_dict_decoder.get(), _dict_word_info.get());
        }
    }
    return Status::OK();
}

Status FileColumnIterator::_read_dict_data() {
    CHECK_EQ(_reader->encoding_info()->encoding(), DICT_ENCODING);
    // read dictionary page
    Slice dict_data;
    PageFooterPB dict_footer;
    _opts.type = INDEX_PAGE;
    RETURN_IF_ERROR(_reader->read_page(_opts, _reader->get_dict_page_pointer(), &_dict_page_handle,
                                       &dict_data, &dict_footer, _compress_codec));
    // ignore dict_footer.dict_page_footer().encoding() due to only
    // PLAIN_ENCODING is supported for dict page right now
    _dict_decoder =
            std::make_unique<BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>>(dict_data);
    RETURN_IF_ERROR(_dict_decoder->init());

    auto* pd_decoder =
            (BinaryPlainPageDecoder<FieldType::OLAP_FIELD_TYPE_VARCHAR>*)_dict_decoder.get();
    _dict_word_info.reset(new StringRef[pd_decoder->_num_elems]);
    RETURN_IF_ERROR(pd_decoder->get_dict_word_info(_dict_word_info.get()));
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(
        const AndBlockColumnPredicate* col_predicates,
        const std::vector<const ColumnPredicate*>* delete_predicates, RowRanges* row_ranges) {
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_zone_map(col_predicates, delete_predicates,
                                                            row_ranges, _opts));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(
        const AndBlockColumnPredicate* col_predicates, RowRanges* row_ranges) {
    if ((col_predicates->can_do_bloom_filter(false) && _reader->has_bloom_filter_index(false)) ||
        (col_predicates->can_do_bloom_filter(true) && _reader->has_bloom_filter_index(true))) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_bloom_filter(col_predicates, row_ranges, _opts));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_dict(const AndBlockColumnPredicate* col_predicates,
                                                  RowRanges* row_ranges) {
    if (!_is_all_dict_encoding) {
        return Status::OK();
    }

    if (!_dict_decoder) {
        RETURN_IF_ERROR(_read_dict_data());
        CHECK_NOTNULL(_dict_decoder);
    }

    if (!col_predicates->evaluate_and(_dict_word_info.get(), _dict_decoder->count())) {
        row_ranges->clear();
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            _type_size = _type_info->size();
            _mem_value.resize(_type_size);
            Status s = Status::OK();
            // If char length is 10, but default value is 'a' , it's length is 1
            // not fill 0 to the ending, because segment iterator will shrink the tail 0 char
            if (_type_info->type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                _type_info->type() == FieldType::OLAP_FIELD_TYPE_HLL ||
                _type_info->type() == FieldType::OLAP_FIELD_TYPE_BITMAP ||
                _type_info->type() == FieldType::OLAP_FIELD_TYPE_STRING ||
                _type_info->type() == FieldType::OLAP_FIELD_TYPE_CHAR) {
                ((Slice*)_mem_value.data())->size = _default_value.length();
                ((Slice*)_mem_value.data())->data = _default_value.data();
            } else if (_type_info->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                if (_default_value != "[]") {
                    return Status::NotSupported("Array default {} is unsupported", _default_value);
                } else {
                    ((Slice*)_mem_value.data())->size = _default_value.length();
                    ((Slice*)_mem_value.data())->data = _default_value.data();
                }
            } else if (_type_info->type() == FieldType::OLAP_FIELD_TYPE_STRUCT) {
                return Status::NotSupported("STRUCT default type is unsupported");
            } else if (_type_info->type() == FieldType::OLAP_FIELD_TYPE_MAP) {
                return Status::NotSupported("MAP default type is unsupported");
            } else {
                s = _type_info->from_string(_mem_value.data(), _default_value, _precision, _scale);
            }
            if (!s.ok()) {
                return s;
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError(
                "invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

void DefaultValueColumnIterator::insert_default_data(const TypeInfo* type_info, size_t type_size,
                                                     void* mem_value,
                                                     vectorized::MutableColumnPtr& dst, size_t n) {
    dst = dst->convert_to_predicate_column_if_dictionary();

    switch (type_info->type()) {
    case FieldType::OLAP_FIELD_TYPE_BITMAP:
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        dst->insert_many_defaults(n);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        vectorized::Int64 int64;
        char* data_ptr = (char*)&int64;
        size_t data_len = sizeof(int64);

        assert(type_size ==
               sizeof(FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType)); //uint24_t
        std::string str = FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::to_string(mem_value);

        VecDateTimeValue value;
        value.from_date_str(str.c_str(), str.length());
        value.cast_to_date();

        int64 = binary_cast<VecDateTimeValue, vectorized::Int64>(value);
        dst->insert_data_repeatedly(data_ptr, data_len, n);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        vectorized::Int64 int64;
        char* data_ptr = (char*)&int64;
        size_t data_len = sizeof(int64);

        assert(type_size ==
               sizeof(FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>::CppType)); //int64_t
        std::string str =
                FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>::to_string(mem_value);

        VecDateTimeValue value;
        value.from_date_str(str.c_str(), str.length());
        value.to_datetime();

        int64 = binary_cast<VecDateTimeValue, vectorized::Int64>(value);
        dst->insert_data_repeatedly(data_ptr, data_len, n);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        vectorized::Int128 int128;
        char* data_ptr = (char*)&int128;
        size_t data_len = sizeof(int128);

        assert(type_size ==
               sizeof(FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>::CppType)); //decimal12_t
        decimal12_t* d = (decimal12_t*)mem_value;
        int128 = DecimalV2Value(d->integer, d->fraction).value();
        dst->insert_data_repeatedly(data_ptr, data_len, n);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE: {
        char* data_ptr = ((Slice*)mem_value)->data;
        size_t data_len = ((Slice*)mem_value)->size;
        dst->insert_data_repeatedly(data_ptr, data_len, n);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        if (dst->is_nullable()) {
            static_cast<vectorized::ColumnNullable&>(*dst).insert_not_null_elements(n);
        } else {
            dst->insert_many_defaults(n);
        }
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_VARIANT: {
        dst->insert_many_defaults(n);
        break;
    }
    default: {
        char* data_ptr = (char*)mem_value;
        size_t data_len = type_size;
        dst->insert_data_repeatedly(data_ptr, data_len, n);
    }
    }
}

Status DefaultValueColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                              bool* has_null) {
    *has_null = _is_default_value_null;
    _insert_many_default(dst, *n);
    return Status::OK();
}

Status DefaultValueColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                  vectorized::MutableColumnPtr& dst) {
    _insert_many_default(dst, count);
    return Status::OK();
}

void DefaultValueColumnIterator::_insert_many_default(vectorized::MutableColumnPtr& dst, size_t n) {
    if (_is_default_value_null) {
        dst->insert_many_defaults(n);
    } else {
        insert_default_data(_type_info.get(), _type_size, _mem_value.data(), dst, n);
    }
}

Status VariantRootColumnIterator::_process_root_column(
        vectorized::MutableColumnPtr& dst, vectorized::MutableColumnPtr& root_column,
        const vectorized::DataTypePtr& most_common_type) {
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    // fill nullmap
    if (root_column->is_nullable() && dst->is_nullable()) {
        vectorized::ColumnUInt8& dst_null_map =
                assert_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column();
        vectorized::ColumnUInt8& src_null_map =
                assert_cast<vectorized::ColumnNullable&>(*root_column).get_null_map_column();
        dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
    }

    // add root column to a tmp object column
    auto tmp = vectorized::ColumnVariant::create(true, false);
    auto& tmp_obj = assert_cast<vectorized::ColumnVariant&>(*tmp);
    tmp_obj.add_sub_column({}, std::move(root_column), most_common_type);

    // merge tmp object column to dst
    obj.insert_range_from(*tmp, 0, tmp->size());

    // finalize object if needed
    if (!obj.is_finalized()) {
        obj.finalize();
    }

#ifndef NDEBUG
    obj.check_consistency();
#endif

    return Status::OK();
}

Status VariantRootColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                             bool* has_null) {
    // read root column
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    auto most_common_type = obj.get_most_common_type();
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->next_batch(n, root_column, has_null));

    return _process_root_column(dst, root_column, most_common_type);
}

Status VariantRootColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                 vectorized::MutableColumnPtr& dst) {
    // read root column
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    auto most_common_type = obj.get_most_common_type();
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->read_by_rowids(rowids, count, root_column));

    return _process_root_column(dst, root_column, most_common_type);
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
    bool has_null = false;
    return next_batch(n, dst, &has_null);
}

static void fill_nested_with_defaults(vectorized::MutableColumnPtr& dst,
                                      vectorized::MutableColumnPtr& sibling_column, size_t nrows) {
    const auto* sibling_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            remove_nullable(sibling_column->get_ptr()).get());
    const auto* dst_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            remove_nullable(dst->get_ptr()).get());
    if (!dst_array || !sibling_array) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Expected array column, but met %s and %s", dst->get_name(),
                               sibling_column->get_name());
    }
    auto new_nested =
            dst_array->get_data_ptr()->clone_resized(sibling_array->get_data_ptr()->size());
    auto new_array = make_nullable(vectorized::ColumnArray::create(
            new_nested->assume_mutable(), sibling_array->get_offsets_ptr()->assume_mutable()));
    dst->insert_range_from(*new_array, 0, new_array->size());
#ifndef NDEBUG
    if (!dst_array->has_equal_offsets(*sibling_array)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Expected same array offsets");
    }
#endif
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                               bool* has_null) {
    if (_sibling_iter) {
        vectorized::MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->next_batch(n, sibling_column, has_null));
        fill_nested_with_defaults(dst, sibling_column, *n);
    } else {
        dst->insert_many_defaults(*n);
    }
    return Status::OK();
}

Status DefaultNestedColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                   vectorized::MutableColumnPtr& dst) {
    if (_sibling_iter) {
        vectorized::MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->read_by_rowids(rowids, count, sibling_column));
        fill_nested_with_defaults(dst, sibling_column, count);
    } else {
        dst->insert_many_defaults(count);
    }
    return Status::OK();
}

Status RowIdColumnIteratorV2::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                         bool* has_null) {
    auto* string_column = assert_cast<vectorized::ColumnString*>(dst.get());

    for (uint32_t i = 0; i < *n; ++i) {
        uint32_t row_id = _current_rowid + i;
        GlobalRowLoacationV2 location(_version, _backend_id, _file_id, row_id);
        string_column->insert_data(reinterpret_cast<const char*>(&location),
                                   sizeof(GlobalRowLoacationV2));
    }
    _current_rowid += *n;
    return Status::OK();
}

Status RowIdColumnIteratorV2::read_by_rowids(const rowid_t* rowids, const size_t count,
                                             vectorized::MutableColumnPtr& dst) {
    auto* string_column = assert_cast<vectorized::ColumnString*>(dst.get());

    for (size_t i = 0; i < count; ++i) {
        uint32_t row_id = rowids[i];
        GlobalRowLoacationV2 location(_version, _backend_id, _file_id, row_id);
        string_column->insert_data(reinterpret_cast<const char*>(&location),
                                   sizeof(GlobalRowLoacationV2));
    }
    return Status::OK();
}
#include "common/compile_check_end.h"

} // namespace doris::segment_v2
