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

#include <silkworm/db/access_layer.hpp>

#include "types.hpp"

namespace silkworm {

/*
 * HeaderRetrieval has the responsibility to retrieve BlockHeader from the db using the hash or the block number.
 */
class HeaderRetrieval {
  public:
    static constexpr size_t kSoftResponseLimit = 2 * 1024 * 1024;  // Target maximum size of returned blocks
    static constexpr size_t kEstimateHeaderRlpSize = 500;          // Approximate size of an RLP encoded block header
    static constexpr size_t kHardResponseLimit = 1024;             // Max number of block headers to be fetched per retrieve request

    explicit HeaderRetrieval(db::ROAccess db_ro_access)
        : db_tx_{db_ro_access.start_ro_tx()} {};

    // Headers
    std::vector<BlockHeader> recover_by_hash(Hash origin, uint64_t amount, uint64_t skip, bool reverse);
    std::vector<BlockHeader> recover_by_number(BlockNum origin, uint64_t amount, uint64_t skip, bool reverse);

    // Node current status
    BlockNum head_height();
    std::tuple<Hash, BigInt> head_hash_and_total_difficulty();

    // Ancestor
    std::tuple<Hash, BlockNum> get_ancestor(Hash hash, BlockNum block_num, BlockNum ancestor_delta,
                                            uint64_t& max_non_canonical);

  protected:
    db::ROTxn db_tx_;
};

}  // namespace silkworm
