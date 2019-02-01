#include "pil.h"
#include "memory_pool.h"
#include "allocator.h"
#include "columnstore.h"
#include "table.h"

#include <iostream>

int main(void){
    /*
    // MemoryPool tests.
    std::unique_ptr<pil::MemoryPool> pool = pil::MemoryPool::CreateDefault();
    // Allocator test
    pil::stl_allocator<uint64_t> allocator(pool.get());
    uint64_t* dat64 = allocator.allocate(1000);
    std::cerr << "allocated=" << allocator.pool_->bytes_allocated() << ",max_memory=" << allocator.pool_->max_memory() << std::endl;




    allocator.deallocate(dat64, 1000);
    std::cerr << "allocated=" << allocator.pool_->bytes_allocated() << ",max_memory=" << allocator.pool_->max_memory() << std::endl;

    pil::MemoryPool* pool2 = pil::default_memory_pool();
    pil::ColumnStore cs(pool2);
    pil::ColumnStoreBuilder<int32_t>* intbuilder = static_cast<pil::ColumnStoreBuilder<int32_t>*>(&cs);
    //cs.buffer
    //std::cerr << intbuilder->buffer->size() << "/" << intbuilder->buffer->capacity() << std::endl;
    for(int i = 0; i < 240; ++i)
        intbuilder->Append(45);



    const int32_t* vals = intbuilder->data();
    for(int i = 0; i < intbuilder->n; ++i){
        std::cerr << i << ": " << vals[i] << std::endl;
    }

    std::cerr << intbuilder->front() << ", " << intbuilder->back() << std::endl;

    // test columnset
    pil::ColumnSet cset(pool2);
    pil::ColumnSetBuilder<int32_t>* csetbuilder = static_cast<pil::ColumnSetBuilder<int32_t>*>(&cset);

    csetbuilder->Append(45);

    int32_t* addvals = new int32_t[4];
    addvals[0] = 1; addvals[1] = 2; addvals[2] = 3; addvals[3] = 4;
    csetbuilder->Append(addvals, 4);
    delete[] addvals;

    std::vector<int32_t> vecvals = {1,3,2};
    csetbuilder->Append(vecvals);

    std::vector<int64_t> clengths = csetbuilder->ColumnLengths();
    for(int i = 0; i < clengths.size(); ++i)
        std::cerr << "column-" << i << ": " << clengths[i] << std::endl;
    */

    pil::Table table;

    pil::RecordBuilder rbuild;
    rbuild.Add<float>("AF", pil::PIL_TYPE_FLOAT, 45);
    rbuild.Add<double>("DP", pil::PIL_TYPE_DOUBLE, 21);
    rbuild.Add<int32_t>("POS", pil::PIL_TYPE_INT32, 21511);

    int32_t* addvals = new int32_t[4];
    addvals[0] = 1; addvals[1] = 2; addvals[2] = 3; addvals[3] = 4;
    rbuild.Add<int32_t>("RAND", pil::PIL_TYPE_INT32, addvals, 4);
    delete[] addvals;

    std::vector<float> vecvals2 = {1,3,2,11515,151231,1312,131,141515,114212};
    rbuild.Add<float>("FIELD123-rand_4", pil::PIL_TYPE_UINT32, vecvals2);

    rbuild.PrintDebug();
    std::cerr << "after printdebug" << std::endl;

    table.Append(rbuild);

    std::cerr << "after first append" << std::endl;

    for(int i = 0; i < 500; ++i){
        rbuild.Add<float>("AF", pil::PIL_TYPE_FLOAT, 45);
        rbuild.Add<int32_t>("RAND", pil::PIL_TYPE_INT32, 21);
        table.Append(rbuild);
    }

    for(int i = 0; i < 75; ++i){
        rbuild.Add<int32_t>("RAND", pil::PIL_TYPE_INT32, 201);
        rbuild.Add<double>("NEWONE", pil::PIL_TYPE_DOUBLE, 21.15);
        table.Append(rbuild);
    }

    for(int i = 0; i < 5000; ++i){
        rbuild.Add<float>("AF", pil::PIL_TYPE_FLOAT, 45);
        rbuild.Add<int32_t>("RAND", pil::PIL_TYPE_INT32, 21);
        table.Append(rbuild);
    }

    std::cerr << "stacks:" << std::endl;
    for(int i = 0; i < table._seg_stack.size(); ++i){
        std::cerr << table._seg_stack[i]->global_id << ": " << table._seg_stack[i]->size() << "->" << table._seg_stack[i]->size_bytes() << " columns/b. first=" << table._seg_stack[i]->columnset.columns.front()->n << std::endl;
    }

    std::cerr << "recordbatches:" << std::endl;
    for(int i = 0; i < table.record_batches.size(); ++i){
        std::cerr << table.record_batches[i]->n_rec << ": " << table.record_batches[i]->patterns.GetMemoryUsage() << std::endl;
        std::cerr << "n=" << table.record_batches[i]->dict.size() << ": ";
        for(int j = 0; j < table.record_batches[i]->dict.size(); ++j) {
            std::cerr << table.record_batches[i]->dict[j] << ",";
        }
        std::cerr << std::endl;
        /*
        uint32_t* vals = reinterpret_cast<uint32_t*>(table.record_batches[i]->patterns.columns[0]->buffer->mutable_data());
        for(int j = 0; j < table.record_batches[i]->n_rec; ++j){
            std::cerr << vals[j] << ",";
        }
        std::cerr << std::endl;
        */
    }

    return(1);
}
