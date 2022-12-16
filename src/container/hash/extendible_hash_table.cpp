//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      for(int i=0;i<num_buckets_;++i) {
        dir_.push_back(std::make_shared<Bucket>(bucket_size));
      }
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t bucket_index=IndexOf(key);
  return dir_[bucket_index]->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t bucket_index=IndexOf(key);
  return dir_[bucket_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  size_t bucket_index=IndexOf(key);
  V temp_value;
  if(dir_[bucket_index]->Find(key,temp_value)){
    return;
  }

  if(!dir_[bucket_index]->IsFull()){
    dir_[bucket_index]->Insert(key,value);
    return;
  }

  if(GetLocalDepth(bucket_index)==GetGlobalDepth()){
    ++global_depth_;
    dir_[bucket_index]->IncrementDepth();
    dir_.resize(1<<global_depth_);
    size_t mask=(1<<(global_depth_-1))-1;
    for(size_t i=1<<(global_depth_-1);i<dir_.size();++i){
      if((i&mask)==bucket_index){
        std::shared_ptr<Bucket> split_bucket=std::make_shared<Bucket>(bucket_size_,dir_[bucket_index]->GetDepth());
        RedistributeBucket(dir_[bucket_index],split_bucket,bucket_index);
        Insert(key,value);
      } else{
        dir_[i]=dir_[i&mask];
      }
    }
  } else{
    dir_[bucket_index]->IncrementDepth();
    std::shared_ptr<Bucket> split_bucket=std::make_shared<Bucket>(bucket_size_,dir_[bucket_index]->GetDepth());
    RedistributeBucket(dir_[bucket_index],split_bucket,bucket_index);
    Insert(key,value);
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> &src_bucket,std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> &dst_bucket,int index){
  int mask=1<<(src_bucket->GetDepth()-1);
  dir_[index|mask]=dst_bucket;
  std::vector<std::pair<K,V>> pairs_to_distribute;
  for(const auto &[k,v]:src_bucket->GetItems()){
    if(std::hash<K>()(k)&mask){
      pairs_to_distribute.push_back({k,v});
    } 
  }

  for(const auto &[k,v]:pairs_to_distribute){
    dst_bucket->Insert(k,v);
    src_bucket->Remove(k);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(const auto &[k,v]:list_){
    if(k==key){
      value=v;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for(auto iter=list_.begin();iter!=list_.end();++iter){
    if(iter->first==key){
      list_.erase(iter);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if(IsFull()){
    return false;
  }

  for(auto iter=list_.begin();iter!=list_.end();++iter){
    if(iter->first==key){
      iter->second=value;
      return true;
    }
  }

  list_.emplace_back(key,value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
