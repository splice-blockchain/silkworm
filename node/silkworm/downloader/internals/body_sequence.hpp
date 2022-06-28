/*
Copyright 2021-2022 The Silkworm Authors

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

#ifndef SILKWORM_BODY_SEQUENCE_HPP
#define SILKWORM_BODY_SEQUENCE_HPP

#include <list>

#include <silkworm/chain/identity.hpp>

#include <silkworm/downloader/packets/new_block_packet.hpp>
#include <silkworm/downloader/packets/block_bodies_packet.hpp>
#include <silkworm/downloader/packets/get_block_bodies_packet.hpp>

#include "db_tx.hpp"
#include "id_sequence.hpp"
#include "types.hpp"
#include "statistics.hpp"

namespace silkworm {

/** BodySequence represents the sequence of body that we are downloading.
 *  It has these responsibilities:
 *    - decide what bodies request (to peers)
 *    - collect bodies,
 *    - decide what bodies can be persisted on the db
 */
class BodySequence {
  public:
    BodySequence(const Db::ReadOnlyAccess&, const ChainIdentity&);
    ~BodySequence();

    // sync current state - this must be done at body forward
    void start_bodies_downloading(BlockNum highest_body_in_db, BlockNum highest_header_in_db);
    void stop_bodies_downloading();

    //! core functionalities: trigger the internal algorithms to decide what bodies we miss
    using MinBlock = BlockNum;
    auto request_more_bodies(time_point_t tp, uint64_t active_peers)
        -> std::tuple<GetBlockBodiesPacket66, std::vector<PeerPenalization>, MinBlock>;

    //! it needs to know if the request issued was not delivered
    void request_nack(time_point_t tp, const GetBlockBodiesPacket66&);

    //! core functionalities: process received bodies
    Penalty accept_requested_bodies(BlockBodiesPacket66&, const PeerId&);

    //! core functionalities: process received block announcement
    Penalty accept_new_block(const Block&, const PeerId&);

    //! core functionalities: returns bodies that are ready to be persisted
    auto withdraw_ready_bodies() -> std::list<Block>;

    //! minor functionalities
    std::list<NewBlockPacket>& announces_to_do();

    [[nodiscard]] BlockNum highest_block_in_db() const;
    [[nodiscard]] BlockNum highest_block_in_memory() const;
    [[nodiscard]] BlockNum lowest_block_in_memory() const;
    [[nodiscard]] BlockNum target_height() const;
    [[nodiscard]] long outstanding_bodies(time_point_t tp) const;
    [[nodiscard]] bool has_bodies_to_request(time_point_t tp, uint64_t active_peers) const;
    [[nodiscard]] size_t ready_bodies() const;

    [[nodiscard]] const Download_Statistics& statistics() const;

    // downloading process tuning parameters
    static /*constexpr*/ seconds_t kRequestDeadline; // = std::chrono::seconds(30);
                                    // after this a response is considered lost it is related to Sentry's peerDeadline
    static /*constexpr*/ seconds_t kNoPeerDelay; // = std::chrono::seconds(1);
                                                                       // delay when no peer accepted the last request
    static /*constexpr*/ size_t kPerPeerMaxOutstandingRequests; // = 4;
    static /*constexpr*/ BlockNum kMaxBlocksPerMessage; // = 128;               // go-ethereum client acceptance limit
    static constexpr BlockNum kMaxAnnouncedBlocks = 10000;
    static constexpr BlockNum kMaxInMemoryBodies = 30000;

  protected:
    using RequestId = uint64_t;

    struct BodyRequest {
        RequestId request_id{0};
        Hash block_hash;
        BlockNum block_height{0};
        BlockHeader header;
        BlockBody body;
        time_point_t request_time;
        bool ready{false};
    };

    struct AnnouncedBlocks {
        void add(Block block);
        std::optional<BlockBody> remove(BlockNum bn);
        size_t size();
      private:
        std::map<BlockNum, Block> blocks_;
    };

    struct TimeOrderedRequestContainer: public std::map<RequestId, std::list<BodyRequest>> { // ordering:less<RequestId>
        using Impl = std::map<RequestId, std::list<BodyRequest>>;
        using Iter = Impl::iterator;

        [[nodiscard]] Iter find_by_request_id(RequestId);
        [[nodiscard]] size_t compute_expired(time_point_t tp) const;

        [[nodiscard]] BodyRequest* find_by_block_num(BlockNum);
        [[nodiscard]] size_t count() const;
        [[nodiscard]] bool contains(BlockNum) const;
    };

    void recover_initial_state();
    void make_new_requests(GetBlockBodiesPacket66&, std::list<BodyRequest>&, MinBlock&, time_point_t, seconds_t timeout);
    auto renew_stale_requests(GetBlockBodiesPacket66&, std::list<BodyRequest>&, MinBlock&, time_point_t, seconds_t timeout)
        -> std::vector<PeerPenalization>;
    void add_to_announcements(BlockHeader, BlockBody, Db::ReadOnlyAccess::Tx&);
    BlockHeader read_canonical_header(Db::ReadOnlyAccess::Tx& tx, BlockNum bn);
    static bool is_valid_body(const BlockHeader&, const BlockBody&);

    TimeOrderedRequestContainer pending_requests_;
    std::map<BlockNum, BodyRequest> ready_requests_;

    AnnouncedBlocks announced_blocks_;
    std::list<NewBlockPacket> announcements_to_do_;

    Db::ReadOnlyAccess db_access_;
    [[maybe_unused]] const ChainIdentity& chain_identity_;

    bool in_downloading_{false};
    BlockNum highest_body_in_db_{0};
    BlockNum headers_stage_height_{0};
    BlockNum highest_requested_body_{0};
    time_point_t last_nack_;
    MonotonicIdSequence id_sequence_;

    Download_Statistics statistics_;
};

}


#endif  // SILKWORM_BODY_SEQUENCE_HPP
