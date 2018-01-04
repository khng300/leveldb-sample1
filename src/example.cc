#include <multiple-store.h>

#include <cstdlib>
#include <iostream>
#include <set>

const uint64_t db_id = 0;
const uint64_t max_id = 1000;

int main() {
  std::set<uint64_t> got_ids;
  multiplestore::Options options;
  multiplestore::ReadOptions read_options;
  multiplestore::WriteOptions write_options;
  multiplestore::WriteBatch updates;
  multiplestore::MultiDB* multi_db;
  multiplestore::Status status;
  options.create_if_missing = true;
  status = multiplestore::MultiDB::Open(options, nullptr, "example.db", &multi_db);
  if (!status.ok()) {
    std::cerr << "Cannot open database "
              << "example.db"
              << " Reason: " << status.ToString() << std::endl;
  }

  for (uint64_t i = 0; i <= max_id; ++i) {
    std::string key(reinterpret_cast<char*>(&i), sizeof(uint64_t));
    std::string data(reinterpret_cast<char*>(&i), sizeof(uint64_t));
    updates.Put(db_id, key, data);
    got_ids.insert(i);
    std::cout << "Written ID: " << i << std::endl;
    if (!((i + 1) % 100)) {
      multi_db->Write(write_options, &updates);
      updates.Clear();
    }
  }
  multi_db->Write(write_options, &updates);
  updates.Clear();
  #if 1
  multiplestore::Iterator* iter = multi_db->NewIterator(read_options, db_id);
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::string key = iter->key().ToString(), value = iter->value().ToString();
    uint64_t j;
    memcpy(&j, key.data(), sizeof(uint64_t));
    std::cout << "Found ID: " << j << std::endl;
    iter->Next();
  }
  delete iter;
#endif
#if 1
  uint64_t i = 0;
  for (uint64_t j : got_ids) {
    multiplestore::Iterator* iter = multi_db->NewIterator(read_options, db_id);
    std::string key(reinterpret_cast<char*>(&j), sizeof(uint64_t));
    iter->Seek(key);
    if (iter->Valid()) {
      updates.Delete(db_id, key);
      // std::cout << "Freed ID: " << j << std::endl;
    } else {
      // std::cout << "Non-existing ID: " << j << std::endl;
    }
    delete iter;
    if (!((i + 1) % 100)) {
      multi_db->Write(write_options, &updates);
      updates.Clear();
    }
    ++i;
  }
  multi_db->Write(write_options, &updates);
  updates.Clear();
#endif
#if 1
  iter = multi_db->NewIterator(read_options, db_id);
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::string key = iter->key().ToString(), value = iter->value().ToString();
    uint64_t j;
    memcpy(&j, key.data(), sizeof(uint64_t));
    std::cout << "Found ID (Remaining): " << j << std::endl;
    iter->Next();
  }
  delete iter;
#endif
  delete multi_db;
  return EXIT_SUCCESS;
}
