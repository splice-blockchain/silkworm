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

#include "block_exchange.hpp"

#include <chrono>
#include <thread>

#include <silkworm/common/log.hpp>
#include <silkworm/downloader/internals/preverified_hashes.hpp>
#include <silkworm/downloader/messages/inbound_message.hpp>
#include <silkworm/downloader/messages/outbound_get_block_bodies.hpp>
#include <silkworm/downloader/rpc/penalize_peer.hpp>

namespace silkworm {

BlockExchange::BlockExchange(SentryClient& sentry, const Db::ReadOnlyAccess& dba, const ChainIdentity& ci)
    : db_access_{dba},
      sentry_{sentry},
      chain_identity_{ci},
      preverified_hashes_{PreverifiedHashes::load(ci.chain.chain_id)},
      header_chain_{ci},
      body_sequence_{dba, ci} {
    auto tx = db_access_.start_ro_tx();
    header_chain_.recover_initial_state(tx);
    header_chain_.set_preverified_hashes(&preverified_hashes_);
}

BlockExchange::~BlockExchange() {
    stop();
    log::Info() << "BlockExchange destroyed";
}

const ChainIdentity& BlockExchange::chain_identity() const { return chain_identity_; }
const PreverifiedHashes& BlockExchange::preverified_hashes() const { return preverified_hashes_; }

void BlockExchange::accept(std::shared_ptr<Message> message) { messages_.push(message); }

void BlockExchange::receive_message(const sentry::InboundMessage& raw_message) {
    try {
        auto message = InboundMessage::make(raw_message);

        SILK_TRACE << "BlockExchange received message " << *message;

        messages_.push(message);
    }
    catch(rlp::DecodingError& error) {
        PeerId peer_id = string_from_H512(raw_message.peer_id());
        log::Warning() << "BlockExchange received and ignored a malformed message, peer= " << peer_id <<
                          ", msg-id= " << raw_message.id() << "/" << sentry::MessageId_Name(raw_message.id()) <<
                          " - " << error.what();
        send_penalization(peer_id, BadBlockPenalty);
        statistics_.malformed_msgs++;
    }
}

void BlockExchange::execution_loop() {
    using namespace std::chrono;
    using namespace std::chrono_literals;
    log::set_thread_name("block-exchange");

    sentry_.subscribe(SentryClient::Scope::BlockAnnouncements, [this](const sentry::InboundMessage& msg) {
        statistics_.received_msgs++;
        statistics_.received_bytes += msg.ByteSizeLong();
        receive_message(msg);
    });

    sentry_.subscribe(SentryClient::Scope::BlockRequests, [this](const sentry::InboundMessage& msg) {
        statistics_.nonsolic_msgs++;
        receive_message(msg);
    });

    auto constexpr kShortInterval = 100ms;
    time_point_t last_update = system_clock::now();

    while (!is_stopping() && !sentry_.is_stopping()) {
        // pop a message from the queue
        std::shared_ptr<Message> message;
        bool present = messages_.timed_wait_and_pop(message, kShortInterval);

        // process the message (command pattern)
        if (present) {
            message->execute(db_access_, header_chain_, body_sequence_, sentry_);
            statistics_.processed_msgs++;
        }

        auto now = system_clock::now();

        // request headers & bodies
        constexpr auto only_one_request = 1;
        auto outstanding_requests = BodySequence::kPerPeerMaxOutstandingRequests * sentry_.active_peers();

        if (messages_.size() < outstanding_requests && body_sequence_.has_bodies_to_request(now, sentry_.active_peers())) {

            auto request_message = std::make_shared<OutboundGetBlockBodies>(only_one_request);
            request_message->execute(db_access_, header_chain_, body_sequence_, sentry_);

            statistics_.tried_msgs += only_one_request;
            statistics_.sent_msgs += request_message->sent_requests();
            statistics_.nack_msgs += request_message->nack_requests();
        }
        // todo: request headers

        // log status
        if (silkworm::log::test_verbosity(silkworm::log::Level::kDebug) && now - last_update > interval_for_stats_) {
            log_status();
            last_update = now;
        }

    }

    stop();
    log::Warning() << "BlockExchange execution_loop is stopping...";
}

void BlockExchange::log_status() {
    static Network_Statistics prev_statistic{};

    log::Debug() << "BlockExchange stats:" << std::setfill('_') << std::right
                 << " in-queue:"   << std::setw(5) << messages_.size()
                 << ", peers:"     << std::setw(2) << sentry_.active_peers()
                 << ", " << Interval_Network_Statistics{prev_statistic, statistics_, interval_for_stats_};

    auto [min_anchor_height, max_anchor_height] = header_chain_.anchor_height_range();
    log::Debug() << "BlockExchange headers: " << std::setfill('_') << std::right
                 << "links= " << std::setw(7) << header_chain_.pending_links()
                 << ", anchors= " << std::setw(4) << header_chain_.anchors()
                 << ", db-height= " << std::setw(10) << header_chain_.highest_block_in_db()
                 << ", mem-height= " << std::setw(10) << min_anchor_height
                 << "~" << std::setw(10) << max_anchor_height
                 << " (#" << std::setw(7) << std::showpos
                 << max_anchor_height - min_anchor_height << ")"
                 << ", net-height= " << std::setw(10) << header_chain_.top_seen_block_height();

    log::Debug() << "BlockExchange bodies:  " << std::setfill('_') << std::right
                 << "outst= " << std::setw(7)
                 << body_sequence_.outstanding_bodies(std::chrono::system_clock::now())
                 << ", ready= " << std::setw(6) << body_sequence_.ready_bodies()
                 << ", db-height= " << std::setw(10) << body_sequence_.highest_block_in_db()
                 << ", mem-height= " << std::setw(10) << body_sequence_.lowest_block_in_memory()
                 << "~" << std::setw(10) << body_sequence_.highest_block_in_memory()
                 << " (#" << std::setw(7) << std::showpos
                 << body_sequence_.highest_block_in_memory() - body_sequence_.lowest_block_in_memory() << ")"
                 << ", net-height= " << std::setw(10) << body_sequence_.target_height();

    log::Debug() << "BlockExchange header stats: " << header_chain_.statistics();

    log::Debug() << "BlockExchange body   stats: " << body_sequence_.statistics();

    prev_statistic.inaccurate_copy(statistics_); // save values
}

void BlockExchange::send_penalization(PeerId id, Penalty p) noexcept {
    rpc::PenalizePeer penalize_peer(id, p);
    penalize_peer.do_not_throw_on_failure();
    penalize_peer.timeout(kRpcTimeout);

    sentry_.exec_remotely(penalize_peer);
}

}  // namespace silkworm
