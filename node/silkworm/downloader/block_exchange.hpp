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

#include <silkworm/chain/identity.hpp>
#include <silkworm/concurrency/containers.hpp>
#include <silkworm/concurrency/worker.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/downloader/internals/body_sequence.hpp>
#include <silkworm/downloader/internals/header_chain.hpp>
#include <silkworm/downloader/messages/message.hpp>
#include <silkworm/downloader/sentry_client.hpp>

namespace silkworm {

//! \brief Implement the logic needed to download headers and bodies
class BlockExchange final : public Worker {
  public:
    BlockExchange(SentryClient&, const db::ROAccess&, const ChainIdentity&);

    // Not copy-able nor move-able
    BlockExchange(const BlockExchange&) = delete;
    BlockExchange(BlockExchange&&) = delete;

    void accept(std::shared_ptr<Message>); /*[[thread_safe]]*/

    const ChainIdentity& chain_identity() const;
    const PreverifiedHashes& preverified_hashes() const;
    SentryClient& sentry() const;

  private:
    using MessageQueue = ConcurrentQueue<std::shared_ptr<Message>>;  // used internally to store new messages

    void work() final;
    void process_incoming_message(const sentry::InboundMessage& raw_message);
    void send_penalization(const PeerId& id, Penalty p) noexcept;
    void log_status();

    static constexpr seconds_t kRpcTimeout = std::chrono::seconds(1);

    db::ROAccess db_access_;
    SentryClient& sentry_;
    const ChainIdentity& chain_identity_;
    PreverifiedHashes preverified_hashes_;
    HeaderChain header_chain_;
    BodySequence body_sequence_;
    MessageQueue messages_{};  // thread safe queue where to receive messages from sentry
};

}  // namespace silkworm
