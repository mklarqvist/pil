#include "table_meta.h"

namespace pil {

// ColumnStoreMetaData
ColumnStoreMetaData::ColumnStoreMetaData() :
        have_segmental_stats(false), file_offset(0), last_modified(0),
        n_records(0), n_elements(0), n_null(0), uncompressed_size(0), compressed_size(0),
        stats_surrogate_min(0), stats_surrogate_max(0)
{
    memset(md5_checksum, 0, 16);
}

int ColumnStoreMetaData::ComputeChecksum(std::shared_ptr<ColumnStore> cstore) {
    if(cstore.get() == nullptr) return(-1);
    Digest::GenerateMd5(cstore->mutable_data(), cstore->uncompressed_size, md5_checksum);
    return(1);
}

void ColumnStoreMetaData::Set(std::shared_ptr<ColumnStore> cstore) {
    if(cstore.get() == nullptr) return;

    n_records = cstore->n_records;
    n_elements = cstore->n_elements;
    n_null = cstore->n_null;
    uncompressed_size = cstore->uncompressed_size;
    compressed_size   = cstore->compressed_size;
}


int ColumnStoreMetaData::Serialize(std::ostream& stream) {
    stream.write(reinterpret_cast<char*>(&have_segmental_stats), sizeof(bool));
    stream.write(reinterpret_cast<char*>(&file_offset), sizeof(uint64_t));
    stream.write(reinterpret_cast<char*>(&last_modified), sizeof(uint64_t));
    stream.write(reinterpret_cast<char*>(&n_records), sizeof(uint32_t));
    stream.write(reinterpret_cast<char*>(&n_elements), sizeof(uint32_t));
    stream.write(reinterpret_cast<char*>(&n_null), sizeof(uint32_t));
    stream.write(reinterpret_cast<char*>(&uncompressed_size), sizeof(uint32_t));
    stream.write(reinterpret_cast<char*>(&compressed_size), sizeof(uint32_t));
    stream.write(reinterpret_cast<char*>(&stats_surrogate_min), sizeof(uint64_t));
    stream.write(reinterpret_cast<char*>(&stats_surrogate_max), sizeof(uint64_t));
    stream.write(reinterpret_cast<char*>(md5_checksum), 16);
    return(stream.good());
}



}
