#include "pil.h"
#include "memory_pool.h"
#include "allocator.h"
#include "columnstore.h"
#include "table.h"

#include "encoder.h"

#include <fstream>
#include <iostream>
#include <sstream>

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
    /*
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
    */


    // Set to 1 for FASTQ test
    if(0) {
        std::ifstream ss;
        //ss.open("/Users/Mivagallery/Desktop/ERR194146.fastq");
        ss.open("/media/mdrk/NVMe/NA12878J_HiSeqX_R1_50mil.fastq", std::ios::ate | std::ios::in);
        if(ss.good() == false){
            std::cerr << "not good: " << ss.badbit << std::endl;
            return 1;
        }
        uint64_t file_size = ss.tellg();
        ss.seekg(0);

        table.out_stream.open("/media/mdrk/NVMe/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/Users/Mivagallery/Desktop/test.pil", std::ios::binary | std::ios::out);
        if(table.out_stream.good() == false) {
            std::cerr << "failed to open output file" << std::endl;
            return 1;
        }

        std::string line;
        uint32_t ltype = 0;

        pil::BaseBitEncoder enc;

        std::vector<pil::PIL_COMPRESSION_TYPE> ctypes;
        ctypes.push_back(pil::PIL_COMPRESS_RC_QUAL);
        ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        table.SetField("QUAL", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);
        ctypes.clear();
        ctypes.push_back(pil::PIL_COMPRESS_RC_BASES);
        //ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        table.SetField("BASES", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);

        // We control wether we create a Tensor-model or Column-split-model ColumnStore
        // by using either Add (split-model) or AddArray (Tensor-model).
        while(std::getline(ss, line)){
            if(ltype == 1) {
                //int rec = enc.Encode(reinterpret_cast<const uint8_t*>(line.data()), line.size());
                rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(line.data()), line.size());
                //rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(enc.data()->mutable_data()), rec);
            }

            if(ltype == 3) {
                rbuild.AddArray<uint8_t>("QUAL", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(line.data()), line.size());
                //std::cerr << line << std::endl;
            }

            ++ltype;
            if(ltype == 4){
                table.Append(rbuild);
                ltype = 0;
            }
        }
        table.FinalizeBatch();

        table.Describe(std::cerr);
    }

    // Set to 1 for SAM test
    if(1) {
        std::ifstream ss;
        //ss.open("/Users/Mivagallery/Desktop/ERR194146.fastq");
        ss.open("/media/mdrk/NVMe/NA12878D_HiSeqX.bam.head.txt", std::ios::ate | std::ios::in);
        if(ss.good() == false){
            std::cerr << "not good: " << ss.badbit << std::endl;
            return 1;
        }
        uint64_t file_size = ss.tellg();
        ss.seekg(0);

        table.out_stream.open("/media/mdrk/NVMe/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/Users/Mivagallery/Desktop/test.pil", std::ios::binary | std::ios::out);
        if(table.out_stream.good() == false) {
            std::cerr << "failed to open output file" << std::endl;
            return 1;
        }

        std::string line;

        pil::BaseBitEncoder enc;

        std::vector<pil::PIL_COMPRESSION_TYPE> ctypes;
        ctypes.push_back(pil::PIL_COMPRESS_RC_QUAL);
        ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        table.SetField("QUAL", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);
        ctypes.clear();
        ctypes.push_back(pil::PIL_COMPRESS_RC_BASES);
        //ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        table.SetField("BASES", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);

        // We control wether we create a Tensor-model or Column-split-model ColumnStore
        // by using either Add (split-model) or AddArray (Tensor-model).
        while(std::getline(ss, line)){
            std::stringstream ss(line);
            std::string s;
            uint32_t l = 0;
            while (std::getline(ss, s, '\t')) {
                if(l == 9) {
                    //std::cout << s << std::endl;
                    //int rec = enc.Encode(reinterpret_cast<const uint8_t*>(line.data()), line.size());
                    rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
                    //rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(enc.data()->mutable_data()), rec);
                }

                if(l == 10) {
                    //std::cout << s << std::endl;
                    rbuild.AddArray<uint8_t>("QUAL", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
                    //std::cerr << line << std::endl;
                }

                ++l;
            }
            table.Append(rbuild);
        }

    }

    if(0){
        std::vector<float> vecvals2 = {1};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        vecvals2 = {1,2};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        vecvals2 = {1,2,3};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        vecvals2 = {1,2};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        vecvals2 = {1};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        std::vector<uint32_t> vecvals3 = {1};
        rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals3);
        table.Append(rbuild);

        vecvals2 = {1};
        rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
        table.Append(rbuild);

        std::vector<uint32_t> vecvals4 = {1};
        rbuild.Add<uint32_t>("FIELD3", pil::PIL_TYPE_UINT32, vecvals4);
        table.Append(rbuild);

        std::vector<uint32_t> vecvals2504;
        for(int i = 0; i < 5008; ++i) vecvals2504.push_back(i);
        rbuild.Add<uint32_t>("FIELD21", pil::PIL_TYPE_UINT32, vecvals2504);
        table.Append(rbuild);

        for(int j = 0; j < 10000; ++j) {
            vecvals2504.clear();
            vecvals2 = {1,2};
            rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
            for(int i = 0; i < 5008; ++i) vecvals2504.push_back(i);
            rbuild.Add<uint32_t>("FIELD21", pil::PIL_TYPE_UINT32, vecvals2504);
            table.Append(rbuild);
        }

        std::cerr << "cset n=" << table._seg_stack.size() << std::endl;
        for(int i = 0; i < table._seg_stack.size(); ++i){
            std::cerr << i << ": " << table._seg_stack[i]->columns.size() << " cols. first n= " << table._seg_stack[i]->columns.front()->n << std::endl;
        }

        std::cerr << "stacks:" << std::endl;
        for(int i = 0; i < table._seg_stack.size(); ++i){
            std::cerr << table._seg_stack[i]->size() << "->" << table._seg_stack[i]->GetMemoryUsage() << std::endl;
        }


        /*
        uint32_t* vals = reinterpret_cast<uint32_t*>(table.record_batch->patterns.columns[0]->buffer->mutable_data());
        for(int j = 0; j < table.record_batch->n_rec; ++j){
            std::cerr << vals[j] << ",";
        }
        std::cerr << std::endl;
        */
    }

    std::cerr << "memory in: " << table.c_in << " memory out: " << table.c_out << " -> " << (float)table.c_in / table.c_out << std::endl;

    return(1);
}
