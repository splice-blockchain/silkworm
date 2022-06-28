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

#include <silkworm/chain/difficulty.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/consensus/base/engine.hpp>

#include "id_sequence.hpp"
#include "body_sequence.hpp"
#include "random_number.hpp"

namespace silkworm {

seconds_t BodySequence::kRequestDeadline;
BlockNum BodySequence::kMaxBlocksPerMessage;
size_t BodySequence::kPerPeerMaxOutstandingRequests;
seconds_t BodySequence::kNoPeerDelay;

BodySequence::BodySequence(const Db::ReadOnlyAccess& dba, const ChainIdentity& ci)
    : db_access_(dba), chain_identity_(ci) {
    recover_initial_state();
}

BodySequence::~BodySequence() {

}

void BodySequence::recover_initial_state() {
    // does nothing
}

BlockNum BodySequence::highest_block_in_db() const { return highest_body_in_db_; }

BlockNum BodySequence::target_height() const { return headers_stage_height_; }

BlockNum BodySequence::highest_block_in_memory() const {
    return highest_requested_body_ == highest_body_in_db_ ? 0 : highest_requested_body_;
}

BlockNum BodySequence::lowest_block_in_memory() const {
    return highest_block_in_memory() == 0 ? 0 : highest_body_in_db_ + 1;
}

size_t BodySequence::ready_bodies() const { return ready_requests_.size(); }

void BodySequence::start_bodies_downloading(BlockNum highest_body_in_db, BlockNum highest_header_in_db) {
    highest_body_in_db_ = highest_body_in_db;
    headers_stage_height_ = highest_header_in_db;
    highest_requested_body_ = highest_body_in_db_;
    in_downloading_ = true;
    pending_requests_.clear();
    ready_requests_.clear();
    statistics_ = {}; // reset statistics
}

void BodySequence::stop_bodies_downloading() {
    in_downloading_ = false;
}

long BodySequence::outstanding_bodies(time_point_t tp) const {
    size_t pending = pending_requests_.size();
    size_t stale = pending_requests_.compute_expired(tp);
    return static_cast<long>(pending - stale);
}

std::list<NewBlockPacket>& BodySequence::announces_to_do() {
    return announcements_to_do_;
}

Penalty BodySequence::accept_requested_bodies(BlockBodiesPacket66& packet, const PeerId&) {
    Penalty penalty = NoPenalty;

    statistics_.received_items += packet.request.size();

    // Check request id
    if (!id_sequence_.contains(packet.requestId)) {
        statistics_.reject_causes.not_requested += packet.request.size();
        return Penalty::BadBlockPenalty;
    }

    // Find matching requests
    auto mr = pending_requests_.find_by_request_id(packet.requestId);
    if (mr == pending_requests_.end()) {
        statistics_.reject_causes.duplicated += packet.request.size(); // we assume it is a slow response to a past
        return NoPenalty;                // request that has been just renewed so it is not present with the old id
    }

    std::list<BodyRequest>& matching_requests = mr->second;

    for (auto& body: packet.request) {
        Hash oh = consensus::EngineBase::compute_ommers_hash(body);
        Hash tr = consensus::EngineBase::compute_transaction_root(body);

        auto r = std::find_if(matching_requests.begin(), matching_requests.end(), [&oh, &tr](const auto& request) {
            return (request.header.ommers_hash == oh && request.header.transactions_root == tr);
        });

        if (r == matching_requests.end()) {
            penalty = BadBlockPenalty;
            SILK_TRACE << "BodySequence: body rejected, no matching requests";
            statistics_.reject_causes.not_requested += 1;
            continue;
        }

        BodyRequest& request = *r;

        // insert into ready requests
        request.body = std::move(body);
        request.ready = true;
        statistics_.accepted_items += 1;
        SILK_TRACE << "BodySequence: body accepted, block_num=" << request.block_height;
        ready_requests_.insert({request.block_height, std::move(request)}); // todo: avoid reallocation

        // remove from pending requests
        matching_requests.erase(r);
    }

    // Process remaining elements in matching_requests invalidating corresponding BodyRequest
    for(auto& request: matching_requests) {
        request.request_id = 0;
        request.request_time = time_point_t();
    }

    // Remove entry if empty
    if (matching_requests.empty())
        pending_requests_.erase(mr);

    return penalty;
}

Penalty BodySequence::accept_new_block(const Block& block, const PeerId&) {
    // save for later usage
    announced_blocks_.add(block);

    return Penalty::NoPenalty;
}

bool BodySequence::has_bodies_to_request(time_point_t tp, [[maybe_unused]] uint64_t active_peers) const {
    return in_downloading_ &&
           (tp - last_nack_ >= kNoPeerDelay) &&
           outstanding_bodies(tp) < static_cast<long>(kPerPeerMaxOutstandingRequests * active_peers * kMaxBlocksPerMessage);
}

auto BodySequence::request_more_bodies(time_point_t tp, uint64_t active_peers)
    -> std::tuple<GetBlockBodiesPacket66, std::vector<PeerPenalization>, MinBlock> {

    if (!in_downloading_ || (tp - last_nack_ < kNoPeerDelay))
        return {};

    GetBlockBodiesPacket66 packet;
    packet.requestId = id_sequence_.generate_one();

    std::list<BodyRequest> new_bundle;

    seconds_t timeout = BodySequence::kRequestDeadline;

    BlockNum min_block{0};

    auto penalizations = renew_stale_requests(packet, new_bundle, min_block, tp, timeout);

    size_t stale_requests = 0; // if not the packet is full and we wil not use this information
    auto outstanding_bodies = pending_requests_.size() - stale_requests;

    if (packet.request.size() < kMaxBlocksPerMessage &&   // if this condition is true stale_requests == 0
        outstanding_bodies < kPerPeerMaxOutstandingRequests * active_peers * kMaxBlocksPerMessage &&
        ready_requests_.size() < kMaxInMemoryBodies) {

        make_new_requests(packet, new_bundle, min_block, tp, timeout);
    }

    if (!new_bundle.empty())
        pending_requests_[packet.requestId] = std::move(new_bundle); // insert

    statistics_.requested_items += packet.request.size();

    return {std::move(packet), std::move(penalizations), min_block};
}

//! Re-evaluate past (stale) requests
auto BodySequence::renew_stale_requests(GetBlockBodiesPacket66& packet, std::list<BodyRequest>& new_bundle,
                                        BlockNum& min_block,
                                        time_point_t tp, seconds_t timeout) -> std::vector<PeerPenalization> {
    std::vector<PeerPenalization> penalizations;

    while (!pending_requests_.empty() && packet.request.size() < kMaxBlocksPerMessage) { // consume pending_requests_
        auto pb = pending_requests_.begin();
        std::list<BodyRequest>& past_bundle = pb->second;

        if (past_bundle.empty()) {
            pending_requests_.erase(pb);
            continue;
        }

        if (tp < past_bundle.begin()->request_time + timeout) // assume past_bundle not empty
            break; // other requests are after this

        while (!past_bundle.empty() && packet.request.size() < kMaxBlocksPerMessage) {
            BodyRequest& past_request = *(past_bundle.begin());

            // retry body request
            packet.request.push_back(past_request.block_hash);
            past_request.request_time = tp;
            past_request.request_id = packet.requestId;

            min_block = std::max(min_block, past_request.block_height);

            // todo: Erigon increment a penalization counter for the peer but it doesn't use it
            // penalizations.emplace_back({Penalty::BadBlockPenalty, }); // todo: find/create a more precise penalization

            new_bundle.splice(new_bundle.end(), past_bundle, past_bundle.begin()); // transfer past_bundle node from
                                                                                   // past_bundle list to the new one

            SILK_TRACE << "BodySequence: renewed request block num= " << past_request.block_height
                       << ", hash= " << past_request.block_hash;
        }

        if (past_bundle.empty()) {
            pending_requests_.erase(pb);
        }
    }

    return penalizations;
}

//! Make requests of new bodies to get progress
void BodySequence::make_new_requests(GetBlockBodiesPacket66& packet, std::list<BodyRequest>& bundle, BlockNum& min_block,
                                     time_point_t tp, seconds_t) {
    auto tx = db_access_.start_ro_tx();

    while (packet.request.size() < kMaxBlocksPerMessage && highest_requested_body_ < headers_stage_height_) {
        BlockNum bn = highest_requested_body_ + 1;

        BodyRequest new_request;
        new_request.block_height = bn;
        new_request.request_id = packet.requestId;
        new_request.request_time = tp;

        new_request.header = read_canonical_header(tx, bn);
        new_request.block_hash = new_request.header.hash();

        std::optional<BlockBody> announced_body = announced_blocks_.remove(bn);
        if (announced_body && is_valid_body(new_request.header, *announced_body)) {
            add_to_announcements(new_request.header, *announced_body, tx);

            new_request.body = std::move(*announced_body);
            new_request.ready = true;

            ready_requests_.emplace(bn, std::move(new_request)); // todo: check efficiency
        }
        else {
            packet.request.push_back(new_request.block_hash);

            min_block = std::max(min_block, new_request.block_height);

            SILK_TRACE << "BodySequence: requested body block-num= " << new_request.block_height
                       << ", hash= " << new_request.block_hash;

            bundle.emplace_back(std::move(new_request));
        }

        highest_requested_body_++;
    }

}

void BodySequence::request_nack(time_point_t tp, const GetBlockBodiesPacket66& packet) {
    last_nack_ = tp;
    if (packet.request.empty()) return;

    auto b = pending_requests_.find_by_request_id(packet.requestId); // insert
    if (b == pending_requests_.end())
        return;   // should not happen

    std::list<BodyRequest>& bundle = b->second;

    for (BodyRequest& past_request: bundle) {
        past_request.request_time -= BodySequence::kRequestDeadline;
    }

    size_t cardinality = packet.request.size();
    statistics_.requested_items -= cardinality;
}

BlockHeader BodySequence::read_canonical_header(Db::ReadOnlyAccess::Tx& tx, BlockNum bn) {
    auto header = tx.read_canonical_header(bn);
    if (!header)
        throw std::logic_error("BodySequence exception, cause: header of block " + std::to_string(bn) + " expected in db");

    return *header;
}

bool BodySequence::is_valid_body(const BlockHeader& header, const BlockBody& body) {
    if (header.ommers_hash != consensus::EngineBase::compute_ommers_hash(body))
        return false;
    if (header.transactions_root != consensus::EngineBase::compute_transaction_root(body))
        return false;
    return true;
}

auto BodySequence::withdraw_ready_bodies() -> std::list<Block> {
    std::list<Block> ready_bodies;

    auto curr_req = ready_requests_.begin();
    while (curr_req != ready_requests_.end()) {
        BlockNum block_height = curr_req->first;
        BodyRequest& request = curr_req->second;
        if (block_height != request.block_height || !request.ready)
            throw new std::logic_error("ready-request queue has a wrong ordering or a wrong member");

        if (block_height > highest_body_in_db_ + 1)
            break; // we need to save body in order without holes, so we accept only bodies with
                   // height = highest_body_in_db_ + 1 or past bodies
        
        highest_body_in_db_ = std::max(highest_body_in_db_, request.block_height);
        ready_bodies.push_back({std::move(request.body), std::move(request.header)});

        curr_req = ready_requests_.erase(curr_req);  // erase curr_req and update curr_req to point to the next request
    }

    return ready_bodies;
}

void BodySequence::add_to_announcements(BlockHeader header, BlockBody body, Db::ReadOnlyAccess::Tx& tx) {

    // calculate total difficulty of the block
    auto parent_td = tx.read_total_difficulty(header.number -1, header.parent_hash);
    if (!parent_td) {
        log::Warning() << "BodySequence: dangling block " << std::to_string(header.number);
        return; // non inserted in announcement list
    }

    auto td = *parent_td + header.difficulty;

    //auto td = parent_td + canonical_difficulty(header.number, header.timestamp,
    //                                           parent_td, parent_ts, parent_has_uncle, chain_config_);

    NewBlockPacket packet{{std::move(body), std::move(header)}, td};

    // add to list
    announcements_to_do_.push_back(std::move(packet));
}

void BodySequence::AnnouncedBlocks::add(Block block) {
    if (blocks_.size() >= kMaxAnnouncedBlocks) {
        return;
    }

    blocks_.emplace(block.header.number, std::move(block));
}

std::optional<BlockBody> BodySequence::AnnouncedBlocks::remove(BlockNum bn) {
    auto b = blocks_.find(bn);
    if (b == blocks_.end())
        return std::nullopt;

    std::optional<BlockBody> body = std::move(b->second);
    blocks_.erase(b);
    return body;
}

size_t BodySequence::AnnouncedBlocks::size() {
    return blocks_.size();
}

auto BodySequence::TimeOrderedRequestContainer::find_by_request_id(RequestId request_id) -> Iter {
    return find(request_id);
}

size_t BodySequence::TimeOrderedRequestContainer::compute_expired(time_point_t tp) const {
    size_t expired{0};
    auto br = begin();
    while (br != end() && tp > br->second.begin()->request_time + kRequestDeadline) {
        expired += br->second.size();
        br++;
    }
    return expired;
}

size_t BodySequence::TimeOrderedRequestContainer::count() const {
    size_t count{0};
    for(auto& item: *this) {
        count += item.second.size();
    }
    return count;
}

bool BodySequence::TimeOrderedRequestContainer::contains(BlockNum bn) const {
    for(auto& item: *this) {
        for(auto& req: item.second) {
            if (req.block_height == bn) {
                return true;
            }
        }
    }
    return false;
}

auto BodySequence::TimeOrderedRequestContainer::find_by_block_num(BlockNum bn) -> BodyRequest* {
    for(auto& item: *this) {
        for(auto& req: item.second) {
            if (req.block_height == bn) {
                return &req;
            }
        }
    }
    return nullptr;
}

const Download_Statistics& BodySequence::statistics() const {
    return statistics_;
}

}