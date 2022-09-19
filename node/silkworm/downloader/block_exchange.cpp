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

#include "block_exchange.hpp"

#include <chrono>
#include <functional>

#include <silkworm/common/log.hpp>
#include <silkworm/downloader/internals/preverified_hashes.hpp>
#include <silkworm/downloader/messages/inbound_message.hpp>
#include <silkworm/downloader/rpc/penalize_peer.hpp>

namespace silkworm {

BlockExchange::BlockExchange(SentryClient& sentry, const db::ROAccess& dba, const ChainIdentity& ci)
    : Worker("BlockExchange"),
      db_access_{dba},
      sentry_{sentry},
      chain_identity_{ci},
      preverified_hashes_{PreverifiedHashes::load(ci.config.chain_id)},
      header_chain_{ci},
      body_sequence_{dba, ci} {
    auto tx = db_access_.start_ro_tx();
    header_chain_.recover_initial_state(tx);
    header_chain_.set_preverified_hashes(&preverified_hashes_);
}

const ChainIdentity& BlockExchange::chain_identity() const { return chain_identity_; }

const PreverifiedHashes& BlockExchange::preverified_hashes() const { return preverified_hashes_; }

SentryClient& BlockExchange::sentry() const { return sentry_; }

void BlockExchange::enqueue_outgoing_message(std::shared_ptr<Message> message) {
    if (!is_running()) return;
    messages_.push(message);
    kick();  // Signals Worker there's work to do
}

void BlockExchange::enqueue_incoming_message(const sentry::InboundMessage& raw_message) {
    if (!is_running()) return;
    try {
        auto message = InboundMessage::make(raw_message);
        messages_.push(message);
        kick();  // Signals Worker there's work to do
    } catch (rlp::DecodingError& error) {
        PeerId peer_id = bytes_from_H512(raw_message.peer_id());
        log::Trace("Ignored malformed message",
                   {"peer", to_hex(human_readable_id(peer_id)),
                    "msg-id", sentry::MessageId_Name(raw_message.id()),
                    "what", error.what()});
        send_penalization(peer_id, BadBlockPenalty);
    }
}

void BlockExchange::work() {
    using namespace std::chrono;
    using namespace std::chrono_literals;
    using namespace std::placeholders;

    static const bool should_trace{log::test_verbosity(log::Level::kTrace)};

    incoming_message_subscription_ = sentry_.signal_message_received.connect(std::bind(&BlockExchange::enqueue_incoming_message, this, _1));

    auto constexpr kShortInterval{1s};
    auto next_status_update{std::chrono::steady_clock::now()};

    while (wait_for_kick(/*timeout_milliseconds=*/5)) {
        std::shared_ptr<Message> message;
        // Consume the whole queue
        while (messages_.try_pop(message)) {
            // log status
            if (should_trace) {
                if (const auto time_now{std::chrono::steady_clock::now()}; time_now >= next_status_update) {
                    log_status();
                    next_status_update = time_now + 30s;
                }
            }
            message->execute(db_access_, header_chain_, body_sequence_, sentry_);
        }
    }

    incoming_message_subscription_.disconnect();
    messages_.clear();
}

void BlockExchange::log_status() {
    log::Trace() << "BlockExchange messages: " << std::setfill('_') << std::setw(5) << std::right << messages_.size()
                 << " in queue";

    auto [min_anchor_height, max_anchor_height] = header_chain_.anchor_height_range();
    log::Trace() << "BlockExchange headers: " << std::setfill('_') << "links= " << std::setw(7) << std::right
                 << header_chain_.pending_links() << ", anchors= " << std::setw(3) << std::right
                 << header_chain_.anchors() << ", db-height= " << std::setw(10) << std::right
                 << header_chain_.highest_block_in_db() << ", mem-height= " << std::setw(10) << std::right
                 << min_anchor_height << "~" << std::setw(10) << std::right << max_anchor_height
                 << ", net-height= " << std::setw(10) << std::right << header_chain_.top_seen_block_height()
                 << "; stats: " << header_chain_.statistics();

    log::Trace() << "BlockExchange bodies:  " << std::setfill('_') << "outstanding bodies= " << std::setw(6)
                 << std::right << body_sequence_.outstanding_bodies(std::chrono::system_clock::now()) << "  "
                 << ", db-height= " << std::setw(10) << std::right << body_sequence_.highest_block_in_db()
                 << ", mem-height= " << std::setw(10) << std::right << body_sequence_.lowest_block_in_memory()
                 << "~" << std::setw(10) << std::right << body_sequence_.highest_block_in_memory()
                 << ", net-height= " << std::setw(10) << std::right << body_sequence_.target_height()
                 << "; stats: " << body_sequence_.statistics();
}

void BlockExchange::send_penalization(const PeerId& id, Penalty p) noexcept {
    rpc::PenalizePeer penalize_peer(id, p);
    penalize_peer.do_not_throw_on_failure();
    penalize_peer.timeout(kRpcTimeout);
    sentry_.exec_remotely(penalize_peer);
}

}  // namespace silkworm
