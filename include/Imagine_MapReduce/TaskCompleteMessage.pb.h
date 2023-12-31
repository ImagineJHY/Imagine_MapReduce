// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: TaskCompleteMessage.proto

#ifndef PROTOBUF_TaskCompleteMessage_2eproto__INCLUDED
#define PROTOBUF_TaskCompleteMessage_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 3000000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 3000000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/unknown_field_set.h>
#include "InternalType.pb.h"
// @@protoc_insertion_point(includes)

namespace Imagine_MapReduce {
namespace Internal {

// Internal implementation detail -- do not call these.
void protobuf_AddDesc_TaskCompleteMessage_2eproto();
void protobuf_AssignDesc_TaskCompleteMessage_2eproto();
void protobuf_ShutdownFile_TaskCompleteMessage_2eproto();

class TaskCompleteRequestMessage;
class TaskCompleteResponseMessage;

// ===================================================================

class TaskCompleteRequestMessage : public ::google::protobuf::Message /* @@protoc_insertion_point(class_definition:Imagine_MapReduce.Internal.TaskCompleteRequestMessage) */ {
 public:
  TaskCompleteRequestMessage();
  virtual ~TaskCompleteRequestMessage();

  TaskCompleteRequestMessage(const TaskCompleteRequestMessage& from);

  inline TaskCompleteRequestMessage& operator=(const TaskCompleteRequestMessage& from) {
    CopyFrom(from);
    return *this;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const TaskCompleteRequestMessage& default_instance();

  void Swap(TaskCompleteRequestMessage* other);

  // implements Message ----------------------------------------------

  inline TaskCompleteRequestMessage* New() const { return New(NULL); }

  TaskCompleteRequestMessage* New(::google::protobuf::Arena* arena) const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const TaskCompleteRequestMessage& from);
  void MergeFrom(const TaskCompleteRequestMessage& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* InternalSerializeWithCachedSizesToArray(
      bool deterministic, ::google::protobuf::uint8* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const {
    return InternalSerializeWithCachedSizesToArray(false, output);
  }
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  void InternalSwap(TaskCompleteRequestMessage* other);
  private:
  inline ::google::protobuf::Arena* GetArenaNoVirtual() const {
    return _internal_metadata_.arena();
  }
  inline void* MaybeArenaPtr() const {
    return _internal_metadata_.raw_arena_ptr();
  }
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional .Imagine_MapReduce.Internal.Identity send_identity_ = 1;
  void clear_send_identity_();
  static const int kSendIdentityFieldNumber = 1;
  ::Imagine_MapReduce::Internal::Identity send_identity_() const;
  void set_send_identity_(::Imagine_MapReduce::Internal::Identity value);

  // optional .Imagine_MapReduce.Internal.Identity recv_identity_ = 2;
  void clear_recv_identity_();
  static const int kRecvIdentityFieldNumber = 2;
  ::Imagine_MapReduce::Internal::Identity recv_identity_() const;
  void set_recv_identity_(::Imagine_MapReduce::Internal::Identity value);

  // optional string file_name_ = 3;
  void clear_file_name_();
  static const int kFileNameFieldNumber = 3;
  const ::std::string& file_name_() const;
  void set_file_name_(const ::std::string& value);
  void set_file_name_(const char* value);
  void set_file_name_(const char* value, size_t size);
  ::std::string* mutable_file_name_();
  ::std::string* release_file_name_();
  void set_allocated_file_name_(::std::string* file_name_);

  // optional uint32 split_id_ = 4;
  void clear_split_id_();
  static const int kSplitIdFieldNumber = 4;
  ::google::protobuf::uint32 split_id_() const;
  void set_split_id_(::google::protobuf::uint32 value);

  // optional uint32 split_num_ = 5;
  void clear_split_num_();
  static const int kSplitNumFieldNumber = 5;
  ::google::protobuf::uint32 split_num_() const;
  void set_split_num_(::google::protobuf::uint32 value);

  // optional string listen_ip_ = 6;
  void clear_listen_ip_();
  static const int kListenIpFieldNumber = 6;
  const ::std::string& listen_ip_() const;
  void set_listen_ip_(const ::std::string& value);
  void set_listen_ip_(const char* value);
  void set_listen_ip_(const char* value, size_t size);
  ::std::string* mutable_listen_ip_();
  ::std::string* release_listen_ip_();
  void set_allocated_listen_ip_(::std::string* listen_ip_);

  // optional string listen_port_ = 7;
  void clear_listen_port_();
  static const int kListenPortFieldNumber = 7;
  const ::std::string& listen_port_() const;
  void set_listen_port_(const ::std::string& value);
  void set_listen_port_(const char* value);
  void set_listen_port_(const char* value, size_t size);
  ::std::string* mutable_listen_port_();
  ::std::string* release_listen_port_();
  void set_allocated_listen_port_(::std::string* listen_port_);

  // repeated string file_list_ = 8;
  int file_list__size() const;
  void clear_file_list_();
  static const int kFileListFieldNumber = 8;
  const ::std::string& file_list_(int index) const;
  ::std::string* mutable_file_list_(int index);
  void set_file_list_(int index, const ::std::string& value);
  void set_file_list_(int index, const char* value);
  void set_file_list_(int index, const char* value, size_t size);
  ::std::string* add_file_list_();
  void add_file_list_(const ::std::string& value);
  void add_file_list_(const char* value);
  void add_file_list_(const char* value, size_t size);
  const ::google::protobuf::RepeatedPtrField< ::std::string>& file_list_() const;
  ::google::protobuf::RepeatedPtrField< ::std::string>* mutable_file_list_();

  // @@protoc_insertion_point(class_scope:Imagine_MapReduce.Internal.TaskCompleteRequestMessage)
 private:

  ::google::protobuf::internal::InternalMetadataWithArena _internal_metadata_;
  bool _is_default_instance_;
  int send_identity__;
  int recv_identity__;
  ::google::protobuf::internal::ArenaStringPtr file_name__;
  ::google::protobuf::uint32 split_id__;
  ::google::protobuf::uint32 split_num__;
  ::google::protobuf::internal::ArenaStringPtr listen_ip__;
  ::google::protobuf::internal::ArenaStringPtr listen_port__;
  ::google::protobuf::RepeatedPtrField< ::std::string> file_list__;
  mutable int _cached_size_;
  friend void  protobuf_AddDesc_TaskCompleteMessage_2eproto();
  friend void protobuf_AssignDesc_TaskCompleteMessage_2eproto();
  friend void protobuf_ShutdownFile_TaskCompleteMessage_2eproto();

  void InitAsDefaultInstance();
  static TaskCompleteRequestMessage* default_instance_;
};
// -------------------------------------------------------------------

class TaskCompleteResponseMessage : public ::google::protobuf::Message /* @@protoc_insertion_point(class_definition:Imagine_MapReduce.Internal.TaskCompleteResponseMessage) */ {
 public:
  TaskCompleteResponseMessage();
  virtual ~TaskCompleteResponseMessage();

  TaskCompleteResponseMessage(const TaskCompleteResponseMessage& from);

  inline TaskCompleteResponseMessage& operator=(const TaskCompleteResponseMessage& from) {
    CopyFrom(from);
    return *this;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const TaskCompleteResponseMessage& default_instance();

  void Swap(TaskCompleteResponseMessage* other);

  // implements Message ----------------------------------------------

  inline TaskCompleteResponseMessage* New() const { return New(NULL); }

  TaskCompleteResponseMessage* New(::google::protobuf::Arena* arena) const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const TaskCompleteResponseMessage& from);
  void MergeFrom(const TaskCompleteResponseMessage& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* InternalSerializeWithCachedSizesToArray(
      bool deterministic, ::google::protobuf::uint8* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const {
    return InternalSerializeWithCachedSizesToArray(false, output);
  }
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  void InternalSwap(TaskCompleteResponseMessage* other);
  private:
  inline ::google::protobuf::Arena* GetArenaNoVirtual() const {
    return _internal_metadata_.arena();
  }
  inline void* MaybeArenaPtr() const {
    return _internal_metadata_.raw_arena_ptr();
  }
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional .Imagine_MapReduce.Internal.Identity send_identity_ = 1;
  void clear_send_identity_();
  static const int kSendIdentityFieldNumber = 1;
  ::Imagine_MapReduce::Internal::Identity send_identity_() const;
  void set_send_identity_(::Imagine_MapReduce::Internal::Identity value);

  // optional .Imagine_MapReduce.Internal.Identity recv_identity_ = 2;
  void clear_recv_identity_();
  static const int kRecvIdentityFieldNumber = 2;
  ::Imagine_MapReduce::Internal::Identity recv_identity_() const;
  void set_recv_identity_(::Imagine_MapReduce::Internal::Identity value);

  // optional .Imagine_MapReduce.Internal.Status status_ = 3;
  void clear_status_();
  static const int kStatusFieldNumber = 3;
  ::Imagine_MapReduce::Internal::Status status_() const;
  void set_status_(::Imagine_MapReduce::Internal::Status value);

  // @@protoc_insertion_point(class_scope:Imagine_MapReduce.Internal.TaskCompleteResponseMessage)
 private:

  ::google::protobuf::internal::InternalMetadataWithArena _internal_metadata_;
  bool _is_default_instance_;
  int send_identity__;
  int recv_identity__;
  int status__;
  mutable int _cached_size_;
  friend void  protobuf_AddDesc_TaskCompleteMessage_2eproto();
  friend void protobuf_AssignDesc_TaskCompleteMessage_2eproto();
  friend void protobuf_ShutdownFile_TaskCompleteMessage_2eproto();

  void InitAsDefaultInstance();
  static TaskCompleteResponseMessage* default_instance_;
};
// ===================================================================


// ===================================================================

#if !PROTOBUF_INLINE_NOT_IN_HEADERS
// TaskCompleteRequestMessage

// optional .Imagine_MapReduce.Internal.Identity send_identity_ = 1;
inline void TaskCompleteRequestMessage::clear_send_identity_() {
  send_identity__ = 0;
}
inline ::Imagine_MapReduce::Internal::Identity TaskCompleteRequestMessage::send_identity_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.send_identity_)
  return static_cast< ::Imagine_MapReduce::Internal::Identity >(send_identity__);
}
inline void TaskCompleteRequestMessage::set_send_identity_(::Imagine_MapReduce::Internal::Identity value) {
  
  send_identity__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.send_identity_)
}

// optional .Imagine_MapReduce.Internal.Identity recv_identity_ = 2;
inline void TaskCompleteRequestMessage::clear_recv_identity_() {
  recv_identity__ = 0;
}
inline ::Imagine_MapReduce::Internal::Identity TaskCompleteRequestMessage::recv_identity_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.recv_identity_)
  return static_cast< ::Imagine_MapReduce::Internal::Identity >(recv_identity__);
}
inline void TaskCompleteRequestMessage::set_recv_identity_(::Imagine_MapReduce::Internal::Identity value) {
  
  recv_identity__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.recv_identity_)
}

// optional string file_name_ = 3;
inline void TaskCompleteRequestMessage::clear_file_name_() {
  file_name__.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline const ::std::string& TaskCompleteRequestMessage::file_name_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
  return file_name__.GetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_file_name_(const ::std::string& value) {
  
  file_name__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
}
inline void TaskCompleteRequestMessage::set_file_name_(const char* value) {
  
  file_name__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
}
inline void TaskCompleteRequestMessage::set_file_name_(const char* value, size_t size) {
  
  file_name__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
}
inline ::std::string* TaskCompleteRequestMessage::mutable_file_name_() {
  
  // @@protoc_insertion_point(field_mutable:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
  return file_name__.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline ::std::string* TaskCompleteRequestMessage::release_file_name_() {
  // @@protoc_insertion_point(field_release:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
  
  return file_name__.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_allocated_file_name_(::std::string* file_name_) {
  if (file_name_ != NULL) {
    
  } else {
    
  }
  file_name__.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), file_name_);
  // @@protoc_insertion_point(field_set_allocated:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_name_)
}

// optional uint32 split_id_ = 4;
inline void TaskCompleteRequestMessage::clear_split_id_() {
  split_id__ = 0u;
}
inline ::google::protobuf::uint32 TaskCompleteRequestMessage::split_id_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.split_id_)
  return split_id__;
}
inline void TaskCompleteRequestMessage::set_split_id_(::google::protobuf::uint32 value) {
  
  split_id__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.split_id_)
}

// optional uint32 split_num_ = 5;
inline void TaskCompleteRequestMessage::clear_split_num_() {
  split_num__ = 0u;
}
inline ::google::protobuf::uint32 TaskCompleteRequestMessage::split_num_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.split_num_)
  return split_num__;
}
inline void TaskCompleteRequestMessage::set_split_num_(::google::protobuf::uint32 value) {
  
  split_num__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.split_num_)
}

// optional string listen_ip_ = 6;
inline void TaskCompleteRequestMessage::clear_listen_ip_() {
  listen_ip__.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline const ::std::string& TaskCompleteRequestMessage::listen_ip_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
  return listen_ip__.GetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_listen_ip_(const ::std::string& value) {
  
  listen_ip__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
}
inline void TaskCompleteRequestMessage::set_listen_ip_(const char* value) {
  
  listen_ip__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
}
inline void TaskCompleteRequestMessage::set_listen_ip_(const char* value, size_t size) {
  
  listen_ip__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
}
inline ::std::string* TaskCompleteRequestMessage::mutable_listen_ip_() {
  
  // @@protoc_insertion_point(field_mutable:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
  return listen_ip__.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline ::std::string* TaskCompleteRequestMessage::release_listen_ip_() {
  // @@protoc_insertion_point(field_release:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
  
  return listen_ip__.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_allocated_listen_ip_(::std::string* listen_ip_) {
  if (listen_ip_ != NULL) {
    
  } else {
    
  }
  listen_ip__.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), listen_ip_);
  // @@protoc_insertion_point(field_set_allocated:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_ip_)
}

// optional string listen_port_ = 7;
inline void TaskCompleteRequestMessage::clear_listen_port_() {
  listen_port__.ClearToEmptyNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline const ::std::string& TaskCompleteRequestMessage::listen_port_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
  return listen_port__.GetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_listen_port_(const ::std::string& value) {
  
  listen_port__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), value);
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
}
inline void TaskCompleteRequestMessage::set_listen_port_(const char* value) {
  
  listen_port__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), ::std::string(value));
  // @@protoc_insertion_point(field_set_char:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
}
inline void TaskCompleteRequestMessage::set_listen_port_(const char* value, size_t size) {
  
  listen_port__.SetNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(),
      ::std::string(reinterpret_cast<const char*>(value), size));
  // @@protoc_insertion_point(field_set_pointer:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
}
inline ::std::string* TaskCompleteRequestMessage::mutable_listen_port_() {
  
  // @@protoc_insertion_point(field_mutable:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
  return listen_port__.MutableNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline ::std::string* TaskCompleteRequestMessage::release_listen_port_() {
  // @@protoc_insertion_point(field_release:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
  
  return listen_port__.ReleaseNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited());
}
inline void TaskCompleteRequestMessage::set_allocated_listen_port_(::std::string* listen_port_) {
  if (listen_port_ != NULL) {
    
  } else {
    
  }
  listen_port__.SetAllocatedNoArena(&::google::protobuf::internal::GetEmptyStringAlreadyInited(), listen_port_);
  // @@protoc_insertion_point(field_set_allocated:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.listen_port_)
}

// repeated string file_list_ = 8;
inline int TaskCompleteRequestMessage::file_list__size() const {
  return file_list__.size();
}
inline void TaskCompleteRequestMessage::clear_file_list_() {
  file_list__.Clear();
}
inline const ::std::string& TaskCompleteRequestMessage::file_list_(int index) const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  return file_list__.Get(index);
}
inline ::std::string* TaskCompleteRequestMessage::mutable_file_list_(int index) {
  // @@protoc_insertion_point(field_mutable:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  return file_list__.Mutable(index);
}
inline void TaskCompleteRequestMessage::set_file_list_(int index, const ::std::string& value) {
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  file_list__.Mutable(index)->assign(value);
}
inline void TaskCompleteRequestMessage::set_file_list_(int index, const char* value) {
  file_list__.Mutable(index)->assign(value);
  // @@protoc_insertion_point(field_set_char:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
}
inline void TaskCompleteRequestMessage::set_file_list_(int index, const char* value, size_t size) {
  file_list__.Mutable(index)->assign(
    reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_set_pointer:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
}
inline ::std::string* TaskCompleteRequestMessage::add_file_list_() {
  // @@protoc_insertion_point(field_add_mutable:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  return file_list__.Add();
}
inline void TaskCompleteRequestMessage::add_file_list_(const ::std::string& value) {
  file_list__.Add()->assign(value);
  // @@protoc_insertion_point(field_add:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
}
inline void TaskCompleteRequestMessage::add_file_list_(const char* value) {
  file_list__.Add()->assign(value);
  // @@protoc_insertion_point(field_add_char:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
}
inline void TaskCompleteRequestMessage::add_file_list_(const char* value, size_t size) {
  file_list__.Add()->assign(reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_add_pointer:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
}
inline const ::google::protobuf::RepeatedPtrField< ::std::string>&
TaskCompleteRequestMessage::file_list_() const {
  // @@protoc_insertion_point(field_list:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  return file_list__;
}
inline ::google::protobuf::RepeatedPtrField< ::std::string>*
TaskCompleteRequestMessage::mutable_file_list_() {
  // @@protoc_insertion_point(field_mutable_list:Imagine_MapReduce.Internal.TaskCompleteRequestMessage.file_list_)
  return &file_list__;
}

// -------------------------------------------------------------------

// TaskCompleteResponseMessage

// optional .Imagine_MapReduce.Internal.Identity send_identity_ = 1;
inline void TaskCompleteResponseMessage::clear_send_identity_() {
  send_identity__ = 0;
}
inline ::Imagine_MapReduce::Internal::Identity TaskCompleteResponseMessage::send_identity_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.send_identity_)
  return static_cast< ::Imagine_MapReduce::Internal::Identity >(send_identity__);
}
inline void TaskCompleteResponseMessage::set_send_identity_(::Imagine_MapReduce::Internal::Identity value) {
  
  send_identity__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.send_identity_)
}

// optional .Imagine_MapReduce.Internal.Identity recv_identity_ = 2;
inline void TaskCompleteResponseMessage::clear_recv_identity_() {
  recv_identity__ = 0;
}
inline ::Imagine_MapReduce::Internal::Identity TaskCompleteResponseMessage::recv_identity_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.recv_identity_)
  return static_cast< ::Imagine_MapReduce::Internal::Identity >(recv_identity__);
}
inline void TaskCompleteResponseMessage::set_recv_identity_(::Imagine_MapReduce::Internal::Identity value) {
  
  recv_identity__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.recv_identity_)
}

// optional .Imagine_MapReduce.Internal.Status status_ = 3;
inline void TaskCompleteResponseMessage::clear_status_() {
  status__ = 0;
}
inline ::Imagine_MapReduce::Internal::Status TaskCompleteResponseMessage::status_() const {
  // @@protoc_insertion_point(field_get:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.status_)
  return static_cast< ::Imagine_MapReduce::Internal::Status >(status__);
}
inline void TaskCompleteResponseMessage::set_status_(::Imagine_MapReduce::Internal::Status value) {
  
  status__ = value;
  // @@protoc_insertion_point(field_set:Imagine_MapReduce.Internal.TaskCompleteResponseMessage.status_)
}

#endif  // !PROTOBUF_INLINE_NOT_IN_HEADERS
// -------------------------------------------------------------------


// @@protoc_insertion_point(namespace_scope)

}  // namespace Internal
}  // namespace Imagine_MapReduce

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_TaskCompleteMessage_2eproto__INCLUDED
