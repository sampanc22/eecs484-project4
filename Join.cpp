#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {

	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

	// partition left relation
	for (uint disk_page_id = left_rel.first; disk_page_id < left_rel.second; disk_page_id++) {
		// load input buffer = last page
		mem->loadFromDisk(disk, disk_page_id, MEM_SIZE_IN_PAGE - 1); 
		Page *page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

		// foreach r in R put r in h1 output buffer
		for (uint i = 0; i < page->size(); i++) {
			Record record = page->get_record(i);
			uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page *bucket_page = mem->mem_page(bucket_id);

			// flush if bucket is full
			if (bucket_page->full()) {
				uint out_page = mem->flushToDisk(disk, bucket_id);
				partitions[bucket_id].add_left_rel_page(out_page);
			}

			bucket_page->loadRecord(record);
		}
	}

	// flush leftover pages that aren't full
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
		Page *page = mem->mem_page(i);
		if (!page->empty()) {
			uint out_page = mem->flushToDisk(disk, i);
			partitions[i].add_left_rel_page(out_page);
		}
	}

	// partition right relation
	for (uint disk_page_id = right_rel.first; disk_page_id < right_rel.second; disk_page_id++) {
		// load input buffer = last page
		mem->loadFromDisk(disk, disk_page_id, MEM_SIZE_IN_PAGE - 1); 
		Page *page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

		// foreach s in S put s in h1 output buffer
		for (uint i = 0; i < page->size(); i++) {
			Record record = page->get_record(i);
			uint bucket_id = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page *bucket_page = mem->mem_page(bucket_id);

			// flush if bucket is full
			if (bucket_page->full()) {
				uint out_page = mem->flushToDisk(disk, bucket_id);
				partitions[bucket_id].add_right_rel_page(out_page);
			}

			bucket_page->loadRecord(record);
		}
	}

	// flush leftover pages that aren't full
	for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
		Page *page = mem->mem_page(i);
		if (!page->empty()) {
			uint out_page = mem->flushToDisk(disk, i);
			partitions[i].add_right_rel_page(out_page);
		}
	}

	return partitions;
}


/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<uint> disk_pages; 

    mem->mem_page(MEM_SIZE_IN_PAGE - 1)->reset();

    // for k = 1 to B - 1
    for (uint k = 0; k < partitions.size(); k++) {
        auto left_rel_pages = partitions[k].get_left_rel();
        auto right_rel_pages = partitions[k].get_right_rel();

        if (left_rel_pages.empty() || right_rel_pages.empty()) continue;

        // get smaller relation
        vector<uint> R_pages, S_pages;
        bool left_is_smaller_rel = partitions[k].num_left_rel_record <= partitions[k].num_right_rel_record;
        if (left_is_smaller_rel) {
            R_pages = left_rel_pages;
            S_pages = right_rel_pages;
        } 
		else {
            R_pages = right_rel_pages;
            S_pages = left_rel_pages;
        }

        for (uint i = 1; i < MEM_SIZE_IN_PAGE - 1; i++) {
            mem->mem_page(i)->reset();
        }

        // foreach r in Rk put r in bucket h2
        for (auto disk_page_id : R_pages) {
            mem->loadFromDisk(disk, disk_page_id, 0);
            Page* input = mem->mem_page(0);

            for (uint i = 0; i < input->size(); i++) {
                Record r = input->get_record(i);
                uint bucket_id = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                mem->mem_page(1 + bucket_id)->loadRecord(r);
            }
        }

        // foreach s in Sk
        for (auto disk_page_id : S_pages) {
            mem->loadFromDisk(disk, disk_page_id, 0);
            Page* input = mem->mem_page(0);
            Page* output = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

            for (uint i = 0; i < input->size(); i++) {
                Record s = input->get_record(i);
                uint bucket_id = s.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                Page* bucket_page = mem->mem_page(1 + bucket_id);

				// foreach r in bucket h2
                for (uint j = 0; j < bucket_page->size(); j++) {
                    Record r = bucket_page->get_record(j);

                    // put r,s in output rel if equal
                    if (r == s) {
                        if (output->full()) {
                            uint flush_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
                            disk_pages.push_back(flush_id);
                        }
                        if (left_is_smaller_rel)
                            output->loadPair(r, s);
                        else
                            output->loadPair(s, r);
                    }
                }
            }
        }
    }

    // flush leftover output
    Page* output = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
    if (!output->empty()) {
        uint flush_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
        disk_pages.push_back(flush_id);
    }

    return disk_pages;
}