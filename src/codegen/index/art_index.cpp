//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index.cpp
//
// Identification: src/codegen/index/art_index.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/index/art_index.h"

#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/proxy/art_index_proxy.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/item_pointer_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/table.h"
#include "codegen/type/integer_type.h"

namespace peloton {
namespace codegen {

namespace {

//===----------------------------------------------------------------------===//
//
// A wrapper around art::Key instances
//
//===----------------------------------------------------------------------===//
class ArtKey {
 public:
  explicit ArtKey(llvm::Value *art_key) : art_key_(art_key) {}

  llvm::Value *GetRawPtr() const { return art_key_; }

  // Get the length of this art::Art key
  llvm::Value *GetKeyLen(CodeGen &codegen) const;

  // Explicitly set the buffer used by this art::Art key
  void SetBuffer(CodeGen &codegen, llvm::Value *new_buf, llvm::Value *buf_len);

  // Get the buffer used by this art::Art key
  llvm::Value *GetBuffer(CodeGen &codegen) const;

  // In general, what is the maximum size an art::Art key can take on?
  static llvm::Value *MaxKeyLen(CodeGen &codegen);

  static ArtKey Make(CodeGen &codegen);

 private:
  // The art key pointer
  llvm::Value *art_key_;
};

//===----------------------------------------------------------------------===//
//
// A helper class to create binary comparable keys.
//
//===----------------------------------------------------------------------===//
class BinaryComparableKey {
 public:
  // What is the actual (i.e., runtime) length of the given key
  llvm::Value *ComputeKeyLen(CodeGen &codegen,
                             const std::vector<Value> &vals) const;

  // What is the maximum possible length of a key with the given types
  uint64_t MaxKeyLength(std::vector<type::Type> &types) const;

  // Convert the given component key values into a binary comparable ART key
  void Construct(CodeGen &codegen, const std::vector<Value> &vals,
                 ArtKey &art_key) const;
};

////////////////////////////////////////////////////////////////////////////////
///
/// ArtKey
///
////////////////////////////////////////////////////////////////////////////////

llvm::Value *ArtKey::GetKeyLen(CodeGen &codegen) const {
  auto *len_ptr = codegen->CreateConstInBoundsGEP2_32(
      KeyProxy::GetType(codegen), art_key_, 0, 1);
  return codegen->CreateLoad(len_ptr, "keyLen");
}

void ArtKey::SetBuffer(CodeGen &codegen, llvm::Value *new_buf,
                       llvm::Value *buf_len) {
  codegen.Call(KeyProxy::set, {art_key_, new_buf, buf_len});
}

llvm::Value *ArtKey::GetBuffer(CodeGen &codegen) const {
  auto *buf_ptr = codegen->CreateConstInBoundsGEP2_32(
      KeyProxy::GetType(codegen), art_key_, 0, 0);
  return codegen->CreateLoad(buf_ptr, "keyBuf");
}

llvm::Value *ArtKey::MaxKeyLen(CodeGen &codegen) {
  return codegen.Const64(art::Key::maxKeyLen);
}

ArtKey ArtKey::Make(CodeGen &codegen) {
  auto *key_type = KeyProxy::GetType(codegen);
  auto *raw_art_key = codegen.AllocateVariable(key_type, "artKey");
  auto *buf_ptr =
      codegen->CreateConstInBoundsGEP2_32(key_type, raw_art_key, 0, 0);
  auto *stack_buf_ptr =
      codegen->CreateConstInBoundsGEP2_32(key_type, raw_art_key, 0, 2);
  stack_buf_ptr =
      codegen->CreatePointerCast(stack_buf_ptr, codegen.CharPtrType());
  codegen->CreateStore(stack_buf_ptr, buf_ptr);
  return ArtKey{raw_art_key};
}

////////////////////////////////////////////////////////////////////////////////
///
/// BinaryComparableKey
///
////////////////////////////////////////////////////////////////////////////////

llvm::Value *BinaryComparableKey::ComputeKeyLen(
    CodeGen &codegen, const std::vector<Value> &vals) const {
  (void)codegen;
  (void)vals;
  // TODO(pmenon): Implement me
  throw Exception{"Implement me"};
}

uint64_t BinaryComparableKey::MaxKeyLength(
    std::vector<type::Type> &types) const {
  uint64_t sz = 0;
  for (const auto &type : types) {
    sz += type.GetSqlType().GetMaxSize(type);
  }
  return sz;
}

void BinaryComparableKey::Construct(CodeGen &codegen,
                                    const std::vector<Value> &vals,
                                    ArtKey &art_key) const {
  // The buffer we'll write into
  llvm::Value *buffer = art_key.GetBuffer(codegen);

  std::vector<type::Type> val_types;
  for (const auto &v : vals) {
    val_types.push_back(v.GetType());
  }

  if (MaxKeyLength(val_types) > art::Key::defaultLen) {
    //
    // It's possible that the input key we're about to convert is larger than
    // the buffer we're provided (through art::Art key). If so, we need to first
    // check if the input key is too large (i.e., the size can fit in 4 bytes).
    // In the extreme, if the input key size is too large, we throw an
    // exception. If the actual input key length is "safe", but is indeed larger
    // than the default size, we need to malloc a new buffer, assign the buffer
    // to the art::Art key and use that to write out the binary comparable
    // version.
    //

    auto *key_len = ComputeKeyLen(codegen, vals);
    auto *cond = codegen->CreateICmpUGT(key_len, ArtKey::MaxKeyLen(codegen));
    lang::If key_too_large{codegen, cond};
    {
      // Key is too large, throw exception
      codegen.Call(RuntimeFunctionsProxy::ThrowIndexKeyTooLargeException, {});
      codegen->CreateUnreachable();
    }
    key_too_large.EndIf();

    // Check if the length of the key is larger than that allocated in the key
    llvm::Value *new_buffer = nullptr;

    cond = codegen->CreateICmpUGT(key_len, art_key.GetKeyLen(codegen));
    lang::If need_to_alloc{codegen, cond};
    {
      new_buffer = codegen.CallMalloc(key_len);
      art_key.SetBuffer(codegen, buffer, key_len);
    }
    need_to_alloc.EndIf();
    buffer = need_to_alloc.BuildPHI(buffer, new_buffer);
  }

  // All is well, write the column values out in binary comparable form
  for (const auto &val : vals) {
    buffer = val.WriteBinaryComparable(codegen, buffer);
  }
}

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////
///
/// ArtIndex
///
////////////////////////////////////////////////////////////////////////////////

ArtIndex::ArtIndex(const index::IndexMetadata &index_meta, Table &table)
    : Index(index_meta, table) {}

llvm::Type *ArtIndex::GetType(CodeGen &codegen) const {
  return ArtIndexProxy::GetType(codegen);
}

llvm::Type *ArtIndex::GetIteratorType(CodeGen &codegen) const {
  return ArtIndexIteratorProxy::GetType(codegen);
}

void ArtIndex::GenerateCode(CodeContext &cc) { GenerateLoadKeyFunction(cc); }

void ArtIndex::Init(CodeGen &codegen, llvm::Value *table_ptr) const {
  // Load this index
  auto index_oid = GetMetadata().GetOid();
  auto *index = GetTable().GetIndexWithOid(codegen, table_ptr, index_oid);
  index = codegen->CreateBitCast(index, GetType(codegen)->getPointerTo());

  // Cast the table to a void pointer
  auto *void_ptr_type = codegen.VoidType()->getPointerTo();
  table_ptr = codegen->CreateBitCast(table_ptr, void_ptr_type);

  // Call ArtIndex::SetLoadKey(...) with the function we generated
  auto &cc = codegen.GetCodeContext();
  codegen.Call(ArtIndexProxy::SetLoadKeyFunc,
               {index, load_key_func_->Resolve(cc), table_ptr});
}

void ArtIndex::GenerateLoadKeyFunction(CodeContext &cc) {
  const auto &table = GetTable();
  const auto &index_meta = GetMetadata();

  const std::string func_name =
      StringUtil::Format("_%lu_%s_%s_loadKey", cc.GetID(),
                         table.GetName().c_str(), index_meta.GetName().c_str());
  const auto load_key_declaration_fn = [&func_name](CodeContext &code_context) {
    CodeGen codegen{code_context};
    const std::vector<FunctionDeclaration::ArgumentInfo> load_key_args = {
        {"table", codegen.VoidType()->getPointerTo()},
        {"itemPointer", codegen.Int64Type()},
        {"artKey", KeyProxy::GetType(codegen)->getPointerTo()}};
    return FunctionDeclaration::MakeDeclaration(
        code_context, func_name, FunctionDeclaration::Visibility::External,
        codegen.VoidType(), load_key_args);
  };

  FunctionBuilder load_key_builder{cc, load_key_declaration_fn(cc)};
  {
    CodeGen codegen{cc};

    auto *table_type = table.GetTableType(codegen);
    auto *table_ptr = codegen->CreateBitCast(
        load_key_builder.GetArgumentByPosition(0), table_type->getPointerTo());

    auto *item_pointer_type = ItemPointerProxy::GetType(codegen);
    auto *item_ptr =
        codegen->CreateIntToPtr(load_key_builder.GetArgumentByPosition(1),
                                item_pointer_type->getPointerTo());

    // Load the tuple, pulling out only the required index columns
    ItemPointerAccess item_pointer{item_ptr};
    std::vector<Value> col_vals;
    table.LoadRow(codegen, table_ptr, item_pointer, index_meta.GetKeyAttrs(),
                  col_vals);

    // Generate key construction
    ArtKey art_key{load_key_builder.GetArgumentByPosition(2)};
    BinaryComparableKey key_constructor;
    key_constructor.Construct(codegen, col_vals, art_key);
  }
  load_key_builder.ReturnAndFinish();

  load_key_func_.reset(new GeneratedFunction(cc, load_key_builder.GetFunction(),
                                             load_key_declaration_fn));
}

}  // namespace codegen
}  // namespace peloton