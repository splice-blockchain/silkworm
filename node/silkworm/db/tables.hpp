/*
   Copyright 2022 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#pragma once

#include <optional>

#include <silkworm/db/mdbx.hpp>

/*
Part of the compatibility layer with the Turbo-Geth DB format;
see its common/dbutils/bucket.go.
*/
namespace silkworm::db::table {

inline constexpr VersionBase kRequiredSchemaVersion{3, 0, 0};  // We're compatible with this

inline constexpr const char* kLastHeaderKey{"LastHeader"};

/* Canonical tables */

//! \brief At block N stores value of state of account for block N-1.
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE)
//!   value : address + previous_account (encoded)
//! \endverbatim
//! \example If block N changed account A from value X to Y. Then:
//! \verbatim
//!   key   : block_num_u64 (BE)
//!   value : address + X
//! \endverbatim
inline constexpr db::MapConfig kAccountChangeSet{"AccountChangeSet", mdbx::key_mode::usual, mdbx::value_mode::multi};

//! \brief Holds the list of blocks in which a specific account has been changed
//! \struct
//! \verbatim
//!   key   : plain account address (20 bytes) + suffix (BE 64bit unsigned integer)
//!   value : binary bitmap holding list of blocks including a state change for the account
//! \endverbatim
//! \details Each record's key holds a suffix which is a 64bit unsigned integer specifying the "upper bound" limit
//! of the list of blocks contained in value part. When this integer is equal to UINT64_MAX it means this
//! record holds the last known chunk of blocks which have changed the account. This is due to
//! how RoaringBitmap64 work.
//! \remark This table/bucket indexes the contents of PlainState (Account record type) therefore honoring the
//! same content limits wrt pruning
inline constexpr db::MapConfig kAccountHistory{"AccountHistory"};

//! \brief Holds blockbody data
//! \struct
//! \verbatim
//!   key   : block number (BE 8 bytes) + block header hash (32 bytes)
//!   value : block body data RLP encoded
//! \endverbatim
inline constexpr db::MapConfig kBlockBodies{"BlockBody"};

//! \brief Stores the binding of *canonical* block number with header hash
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE)
//!   value : header_hash
//! \endverbatim
inline constexpr db::MapConfig kCanonicalHashes{"CanonicalHeader"};

//! \brief Stores the headers downloaded from peers
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE) + header hash
//!   value : header RLP encoded
//! \endverbatim
inline constexpr db::MapConfig kHeaders{"Header"};

//! \brief Stores the total difficulty accrued at each block height
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE) + header hash
//!   value : total difficulty (RLP encoded
//! \endverbatim
inline constexpr db::MapConfig kDifficulty{"HeadersTotalDifficulty"};

//! \brief Stores the receipts for every canonical block
//! \remarks Non canonical blocks' receipts are not stored
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE)
//!   value : receipts (CBOR Encoded)
//! \endverbatim
inline constexpr db::MapConfig kBlockReceipts{"Receipt"};
inline constexpr db::MapConfig kBloomBitsIndex{"BloomBitsIndex"};
inline constexpr db::MapConfig kBloomBits{"BloomBits"};
inline constexpr db::MapConfig kBodiesSnapshotInfo{"BodiesSnapshotInfo"};

//! \brief Stores the mapping of block number to the set (sorted) of all accounts touched by call traces.
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE)
//!   value : account address + two bits (one for "from" + another for "to")
//! \endverbatim
inline constexpr db::MapConfig kCallTraceSet{"CallTraceSet", mdbx::key_mode::usual, mdbx::value_mode::multi};
inline constexpr db::MapConfig kCallFromIndex{"CallFromIndex"};
inline constexpr db::MapConfig kCallToIndex{"CallToIndex"};

//! \brief Stores contract's code
//! \struct
//! \verbatim
//!   key   : contract code hash
//!   value : contract code
//! \endverbatim
inline constexpr db::MapConfig kCode{"Code"};

inline constexpr db::MapConfig kConfig{"Config"};
inline constexpr db::MapConfig kDatabaseInfo{"DbInfo"};
inline constexpr db::MapConfig kBlockTransactions{"BlockTransaction"};

//! \brief Store "current" state for accounts with hashed address key
//! \struct
//! \verbatim
//!   key   : account address hash (32 bytes)
//!   value : account encoded for storage
//! \endverbatim
//! \remarks This table stores the same values for PlainState (Account record type) but with hashed key
inline constexpr db::MapConfig kHashedAccounts{"HashedAccount"};

//! \brief Store contract code hash for given contract by key hashed address + incarnation
//! \def "Incarnation" how many times given account was SelfDestruct'ed.
//! \struct
//! \verbatim
//!   key   : contract address hash (32 bytes) + incarnation (u64 BE)
//!   value : code hash (32 bytes)
//! \endverbatim
//! \remarks This table stores the same values for PlainCodeHash but with hashed key address
inline constexpr db::MapConfig kHashedCodeHash{"HashedCodeHash"};

//! \brief Store "current" state for contract storage with hashed address
//! \struct
//! \verbatim
//!   key   : contract address hash (32 bytes) + incarnation (u64 BE)
//!   value : storage key hash (32 bytes) + storage value (hash 32 bytes)
//! \endverbatim
//! \remarks This table stores the same values for PlainState (storage record type) but with hashed key
inline constexpr db::MapConfig kHashedStorage{"HashedStorage", mdbx::key_mode::usual, mdbx::value_mode::multi};
inline constexpr db::MapConfig kHeadBlock{"LastBlock"};
inline constexpr db::MapConfig kHeadHeader{"LastHeader"};

//! \brief Map of header hash -> block number
//! \struct
//! \verbatim
//!   key   : header hash (32 bytes)
//!   value : block number (BE 8 bytes)
//! \endverbatim
inline constexpr db::MapConfig kHeaderNumbers{"HeaderNumber"};
inline constexpr db::MapConfig kHeadersSnapshotInfo{"HeadersSnapshotInfo"};

//! \brief Stores the last incarnation of last contract SelfDestruct
//! \struct
//! \verbatim
//!   key   : contract address (unhashed 20 bytes)
//!   value : incarnation (u64 BE)
//! \endverbatim
inline constexpr db::MapConfig kIncarnationMap{"IncarnationMap"};

//! \brief Holds the list of blocks in which a specific log address has been touched
//! \struct
//! \verbatim
//!   key   : address (20 bytes) + suffix (BE 64bit unsigned integer)
//!   value : binary bitmap holding list of blocks
//! \endverbatim
//! \details Each record's key holds a suffix which is a 64bit unsigned integer specifying the "upper bound" limit
//! of the list of blocks contained in value part. When this integer is equal to UINT64_MAX it means this
//! record holds the last known chunk of blocks which have changed the account. This is due to
//! how RoaringBitmap64 work.
inline constexpr db::MapConfig kLogAddressIndex{"LogAddressIndex"};

//! \brief Holds the list of blocks in which a specific log topic has been touched
//! \struct
//! \verbatim
//!   key   : hash (32 bytes) + suffix (BE 64bit unsigned integer)
//!   value : binary bitmap holding list of blocks
//! \endverbatim
//! \details Each record's key holds a suffix which is a 64bit unsigned integer specifying the "upper bound" limit
//! of the list of blocks contained in value part. When this integer is equal to UINT64_MAX it means this
//! record holds the last known chunk of blocks which have changed the account. This is due to
//! how RoaringBitmap64 work.
inline constexpr db::MapConfig kLogTopicIndex{"LogTopicIndex"};

//! \brief Stores the logs for every transaction in canonical blocks
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE) + transaction_index_u32 (BE)
//!   value : logs of transaction (CBOR Encoded)
//! \endverbatim
//! \remarks Non canonical blocks' transactions logs aren't here
inline constexpr db::MapConfig kLogs{"TransactionLog"};

inline constexpr db::MapConfig kMigrations{"Migration"};

//! \brief Store contract code hash for given contract address + incarnation
//! \def "Incarnation" how many times given account was SelfDestruct'ed.
//! \struct
//! \verbatim
//!   key   : contract address (20 bytes) + incarnation (u64 BE)
//!   value : code hash (32 bytes)
//! \endverbatim
inline constexpr db::MapConfig kPlainCodeHash{"PlainCodeHash"};

//! \brief Store "current" state for accounts and storage and is used for block execution
//! \def "Incarnation" how many times given account was SelfDestruct'ed.
//! \struct
//! \verbatim
//! Accounts :
//!   key   : address (20 bytes)
//!   value : account encoded for storage
//! Storage :
//!   key   : address (20 bytes) + incarnation (u64 BE)
//!   value : storage key (32 bytes) + storage value (hash 32 bytes)
//! \endverbatim
inline constexpr db::MapConfig kPlainState{"PlainState", mdbx::key_mode::usual, mdbx::value_mode::multi};

//! \brief Store recovered senders' addresses for each transaction in a block
//! \remarks Senders' addresses are not stored in transactions so they must be recovered from the signature
//! of the transaction itself
//! \struct
//! \verbatim
//!   key   : block_num (u64 BE)
//!   value : array of addresses (each 20 bytes)
//!   The addresses in array are listed in the same order of the transactions of the block
//! \endverbatim
inline constexpr db::MapConfig kSenders{"TxSender"};

//! \brief Stores sequence values for different keys
//! \remarks Usually keys are table names
//! \struct
//! \verbatim
//!   key   : a string
//!   value : last increment generated (u64 BE)
//! \endverbatim
inline constexpr db::MapConfig kSequence{"Sequence"};

inline constexpr db::MapConfig kSnapshotInfo{"SnapshotInfo"};
inline constexpr db::MapConfig kStateSnapshotInfo{"StateSnapshotInfo"};

//! \brief At block N stores value of state of storage for block N-1.
//! \struct
//! \verbatim
//!   key   : block_num_u64 (BE) + address + incarnation_u64 (BE)
//!   value : plain_storage_location (32 bytes) + previous_value (no leading zeros)
//! \endverbatim
//! \example If block N changed storage from value X to Y. Then:
//! \verbatim
//!   key   : block_num_u64 (BE) + address + incarnation_u64 (BE)
//!   value : plain_storage_location (32 bytes) + X
//! \endverbatim
inline constexpr db::MapConfig kStorageChangeSet{"StorageChangeSet", mdbx::key_mode::usual, mdbx::value_mode::multi};

//! \brief Holds the list of blocks in which a specific storage location has been changed
//! \struct
//! \verbatim
//!   key   : plain contract account address (20 bytes) + location (32 bytes hash) + suffix (BE 64bit unsigned integer)
//!   value : binary bitmap holding list of blocks including a state change for the account
//! \endverbatim
//! \remark Each record's key holds a suffix which is a 64bit unsigned integer specifying the "upper bound" limit
//! of the list of blocks contained in value part. When this integer is equal to UINT64_MAX it means this
//! record holds the last known chunk of blocks which have changed the account. This is due to
//! how RoaringBitmap64 work.
//! \remark This table/bucket indexes the contents of PlainState (Account record type) therefore honoring the
//! same content limits wrt pruning
inline constexpr db::MapConfig kStorageHistory{"StorageHistory"};

//! \brief Stores reached progress for each stage
//! \struct
//! \verbatim
//!   key   : stage name
//!   value : block_num_u64 (BE)
//! \endverbatim
inline constexpr db::MapConfig kSyncStageProgress{"SyncStage"};

//! \brief Unwind point for stages
//! \struct
//! \verbatim
//!   key   : stage name
//!   value : block number (BE u64)
//! \endverbatim
inline constexpr db::MapConfig kSyncStageUnwind{"SyncStageUnwind"};

//! \brief Hold the nodes composing the StateRoot
//! \struct
//! \verbatim
//!   key   : node key
//!   value : serialized node value (see core::trie::Node)
//! \endverbatim
//! \remark The only record with empty key is the root node
inline constexpr db::MapConfig kTrieOfAccounts{"TrieAccount"};

//! \brief Hold the nodes composing the StorageRoot for each contract
//! \struct
//! \verbatim
//!   key   : db::kHashedStoragePrefix(40 bytes == hashed address + incarnation) + node key
//!   value : serialized node value (see core::trie::Node)
//! \endverbatim
//! \details Each trie has its own invariant db::kHashedStoragePrefix\n
//! Records with key len == 40 (ie node key == 0) are root nodes
inline constexpr db::MapConfig kTrieOfStorage{"TrieStorage"};

//! \brief Hold the map of transaction hashes -> block number
//! \struct
//! \verbatim
//!   key   : transaction hash (32 bytes)
//!   value : block number (BE u64)
//! \endverbatim
inline constexpr db::MapConfig kTxLookup{"BlockTransactionLookup"};

inline constexpr db::MapConfig kChainDataTables[]{
    kAccountChangeSet,
    kAccountHistory,
    kBlockBodies,
    kBlockReceipts,
    kBloomBits,
    kBloomBitsIndex,
    kBodiesSnapshotInfo,
    kCallFromIndex,
    kCallToIndex,
    kCallTraceSet,
    kCanonicalHashes,
    kHeaders,
    kDifficulty,
    kCode,
    kConfig,
    kHashedCodeHash,
    kDatabaseInfo,
    kBlockTransactions,
    kHashedAccounts,
    kHashedStorage,
    kHeadBlock,
    kHeadHeader,
    kHeaderNumbers,
    kHeadersSnapshotInfo,
    kIncarnationMap,
    kLogAddressIndex,
    kLogTopicIndex,
    kLogs,
    kMigrations,
    kPlainCodeHash,
    kPlainState,
    kSenders,
    kSequence,
    kSnapshotInfo,
    kStateSnapshotInfo,
    kStorageChangeSet,
    kStorageHistory,
    kSyncStageProgress,
    kSyncStageUnwind,
    kTrieOfAccounts,
    kTrieOfStorage,
    kTxLookup,
};

//! \brief Ensures all defined tables are present in db with consistent flags.
//! \remark Should a table not exist it gets created
void check_or_create_chaindata_tables(mdbx::txn& txn);

}  // namespace silkworm::db::table
