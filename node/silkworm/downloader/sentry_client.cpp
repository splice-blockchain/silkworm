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

#include "sentry_client.hpp"

#include <silkworm/common/log.hpp>
#include <silkworm/downloader/rpc/hand_shake.hpp>
#include <silkworm/downloader/rpc/peer_count.hpp>
#include <silkworm/downloader/rpc/receive_messages.hpp>
#include <silkworm/downloader/rpc/set_status.hpp>

namespace silkworm {

constexpr int kMaxReceiveMessageSize = 10_Mebi;  // reference: eth/66 protocol

static std::shared_ptr<grpc::Channel> create_custom_channel(const std::string& sentry_addr) {
    grpc::ChannelArguments custom_args{};
    custom_args.SetMaxReceiveMessageSize(kMaxReceiveMessageSize);
    return grpc::CreateCustomChannel(sentry_addr, grpc::InsecureChannelCredentials(), custom_args);
}

SentryClient::SentryClient(const std::string& sentry_addr)
    : Worker("SentryClient"),
      base_t(create_custom_channel(sentry_addr)),
      message_subscription_(rpc::ReceiveMessages::Scope::BlockAnnouncements |
                            rpc::ReceiveMessages::Scope::BlockRequests) {
    log::Info("SentryClient", {"remote", sentry_addr}) << " connecting ...";
}

rpc::ReceiveMessages::Scope SentryClient::scope(const sentry::InboundMessage& message) {
    switch (message.id()) {
        case sentry::MessageId::BLOCK_HEADERS_66:
        case sentry::MessageId::BLOCK_BODIES_66:
        case sentry::MessageId::NEW_BLOCK_HASHES_66:
        case sentry::MessageId::NEW_BLOCK_66:
            return rpc::ReceiveMessages::Scope::BlockAnnouncements;
        case sentry::MessageId::GET_BLOCK_HEADERS_66:
        case sentry::MessageId::GET_BLOCK_BODIES_66:
            return rpc::ReceiveMessages::Scope::BlockRequests;
        default:
            return rpc::ReceiveMessages::Scope::Other;
    }
}

void SentryClient::set_status(Hash head_hash, BigInt head_td, const ChainIdentity& chain_identity) {
    rpc::SetStatus set_status{chain_identity, head_hash, head_td};
    exec_remotely(set_status);
    log::Trace("SentryClient") << "set_status sent";
}

void SentryClient::stop(bool wait) {
    Worker::stop(wait);
    // Regardless "wait" we do need to join stats thread
    if (thread_stats_) {
        thread_stats_->join();
        thread_stats_.reset();
    }
}

void SentryClient::work() {
    hand_shake();

    // send a message subscription
    // rpc::ReceiveMessages message_subscription(Scope::BlockAnnouncements | Scope::BlockRequests);
    exec_remotely(message_subscription_);

    // Spawn receiving stats thread
    thread_stats_ = std::make_unique<std::thread>([&]() {
        log::set_thread_name("SentryClient-Stats");
        thread_stats_state_.store(Worker::State::kStarted);
        thread_stats_cv_.notify_all();
        log::Trace("Thread", {"name", log::get_thread_name()}) << "started";
        try {
            work_stats();
        } catch (const std::exception& ex) {
            log::Error(log::get_thread_name(), {"exception", typeid(ex).name(), "what", std::string(ex.what())});
        } catch (...) {
            log::Error(log::get_thread_name(), {"exception", "undefined"});
        }
        log::Trace("Thread", {"name", log::get_thread_name()}) << "stopped";
        thread_stats_state_.store(Worker::State::kStopped);
    });

    {
        // Properly wait for start
        std::unique_lock l(thread_stats_mtx_);
        thread_stats_cv_.wait(l);
    }

    // Begin receive messages
    while (is_running() &&
           thread_stats_state_.load() == Worker::State::kStarted &&
           message_subscription_.receive_one_reply()) {
        const auto& message = message_subscription_.reply();
        signal_message_received(message);
    }
    signal_message_received.disconnect_all_slots();
    message_subscription_.try_cancel();
}

void SentryClient::work_stats() {
    // send a stats subscription
    // rpc::ReceivePeerStats receive_peer_stats;
    exec_remotely(stats_subscription_);

    // ask the remote sentry about the current active peers
    update_active_peers_count();

    // receive stats
    while (is_running() && stats_subscription_.receive_one_reply() /* is this blocking ????? */) {
        const sentry::PeerEvent& stat = stats_subscription_.reply();
        const auto peerId = bytes_from_H512(stat.peer_id());
        const char* event = "";
        if (stat.event_id() == sentry::PeerEvent::Connect) {
            event = "connected";
            ++active_peers_;
        } else {
            event = "disconnected";
            if (active_peers_ > 0) --active_peers_;  // workaround, to fix this we need to improve the interface
        }                                            // or issue a count_active_peers()

        log::Trace("SentryClient",
                   {"peer", event,
                    "id", to_hex(human_readable_id(peerId)),
                    "active", std::to_string(active_peers_)});
    }
    stats_subscription_.try_cancel();
}

void SentryClient::hand_shake() {
    log::Trace("SentryClient") << "hand-shaking...";
    rpc::HandShake hand_shake;
    exec_remotely(hand_shake);
    sentry::HandShakeReply reply = hand_shake.reply();

    sentry::Protocol server_protocol{reply.protocol()};
    if (server_protocol != sentry::Protocol::ETH66) {
        throw SentryClientException("protocol " +
                                    std::string(magic_enum::enum_name<sentry::Protocol>(server_protocol)) +
                                    " not supported");
    }
}

void SentryClient::update_active_peers_count() {
    using namespace std::chrono_literals;

    rpc::PeerCount rpc;
    rpc.timeout(1s);
    rpc.do_not_throw_on_failure();
    exec_remotely(rpc);

    if (!rpc.status().ok()) {
        throw SentryClientException(rpc.status_.error_message());
    }

    sentry::PeerCountReply peers = rpc.reply();
    active_peers_.store(peers.count());
}

uint64_t SentryClient::active_peers() {
    return active_peers_.load();
}
}  // namespace silkworm
