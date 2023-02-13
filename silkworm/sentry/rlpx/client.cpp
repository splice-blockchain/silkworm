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

#include "client.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include <silkworm/node/common/log.hpp>
#include <silkworm/sentry/common/socket_stream.hpp>
#include <silkworm/sentry/common/timeout.hpp>

namespace silkworm::sentry::rlpx {

using namespace std::chrono_literals;
using namespace boost::asio;

awaitable<void> Client::start(
    silkworm::rpc::ServerContextPool& context_pool,
    common::EccKeyPair node_key,
    std::string client_id,
    uint16_t node_listen_port,
    std::function<std::unique_ptr<Protocol>()> protocol_factory) {
    if (peer_urls_.empty()) {
        co_return;
    }
    auto& peer_url = peer_urls_.front();
    auto& client_context = context_pool.next_io_context();

    ip::tcp::resolver resolver{client_context};
    auto endpoints = co_await resolver.async_resolve(
        peer_url.ip().to_string(),
        std::to_string(peer_url.port()),
        use_awaitable);
    const ip::tcp::endpoint& endpoint = *endpoints.cbegin();

    common::SocketStream stream{client_context};

    bool is_connected = false;
    while (!is_connected) {
        try {
            co_await stream.socket().async_connect(endpoint, use_awaitable);
            is_connected = true;
        } catch (const boost::system::system_error& ex) {
            if (ex.code() == boost::system::errc::operation_canceled)
                throw;
            log::Debug() << "RLPx client connect exception: " << ex.what();
        }
        if (!is_connected) {
            stream = common::SocketStream{client_context};
            log::Warning() << "Failed to connect to " << peer_url.to_string() << ", reconnecting...";
            common::Timeout timeout(10s);
            try {
                co_await timeout();
            } catch (const common::Timeout::ExpiredError&) {
            }
        }
    }

    auto remote_endpoint = stream.socket().remote_endpoint();
    log::Debug() << "RLPx client connected to "
                 << remote_endpoint.address().to_string() << ":" << remote_endpoint.port();

    auto peer = std::make_unique<Peer>(
        client_context,
        std::move(stream),
        node_key,
        client_id,
        node_listen_port,
        protocol_factory(),
        std::optional{peer_url.public_key()});

    co_await peer_channel_.send(std::move(peer));
}

}  // namespace silkworm::sentry::rlpx