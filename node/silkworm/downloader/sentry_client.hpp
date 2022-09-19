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

#include <boost/signals2.hpp>
#include <p2psentry/sentry.grpc.pb.h>

#include <silkworm/chain/identity.hpp>
#include <silkworm/concurrency/worker.hpp>
#include <silkworm/downloader/internals/grpc_sync_client.hpp>
#include <silkworm/downloader/internals/sentry_type_casts.hpp>
#include <silkworm/downloader/internals/types.hpp>
#include <silkworm/downloader/rpc/receive_messages.hpp>
#include <silkworm/downloader/rpc/receive_peer_stats.hpp>

namespace silkworm {

/*
 * SentryClient is a client to connect to a remote sentry, send rpc and receive reply.
 * The remote sentry must implement the ethereum p2p protocol and must have an interface specified by sentry.proto
 * SentryClient uses gRPC/protobuf to communicate with the remote sentry.
 */
class SentryClient final : public rpc::Client<sentry::Sentry>, public Worker {
  public:
    using base_t = rpc::Client<sentry::Sentry>;

    explicit SentryClient(const std::string& sentry_addr);  // connect to the remote sentry

    // Not copy-able nor move-able
    SentryClient(const SentryClient&) = delete;
    SentryClient(SentryClient&&) = delete;

    void stop(bool wait) final;

    void set_status(Hash head_hash, BigInt head_td, const ChainIdentity&);  // init the remote sentry

    uint64_t active_peers();  // return cached peers count

    using base_t::exec_remotely;  // exec_remotely(SentryRpc& rpc) sends a rpc request to the remote sentry

    static rpc::ReceiveMessages::Scope scope(const sentry::InboundMessage& message);  // find the scope of the message

    //! \brief Notifies connected handlers a message a message has been received
    boost::signals2::signal<void(const sentry::InboundMessage& message)> signal_message_received;

  protected:
    rpc::ReceiveMessages message_subscription_;
    rpc::ReceivePeerStats stats_subscription_;
    std::atomic<uint64_t> active_peers_{0};

  private:
    std::unique_ptr<std::thread> thread_stats_{nullptr};  // Ancillary thread work for stats receiving
    std::atomic<Worker::State> thread_stats_state_{Worker::State::kStopped};
    std::condition_variable thread_stats_cv_;  // To track its start
    std::mutex thread_stats_mtx_;

    void work() final;                 // Messages receiving loop
    void work_stats();                 // Stats receiving loop
    void hand_shake();                 // needed by the remote sentry, also check the protocol version
    void update_active_peers_count();  // ask the remote sentry for active peers and update local cache
};

// custom exception
class SentryClientException : public std::runtime_error {
  public:
    explicit SentryClientException(const std::string& message) : std::runtime_error(message) {}
};

}  // namespace silkworm
