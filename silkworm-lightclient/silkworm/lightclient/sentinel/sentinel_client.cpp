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

#include "sentinel_client.hpp"

#include <silkworm/sentry/common/timeout.hpp>
#include <silkworm/lightclient/sentinel/topic.hpp>

namespace silkworm::cl::sentinel {

using namespace std::chrono;
using namespace boost::asio;

using LightClientBootstrapPtr = std::shared_ptr<eth::LightClientBootstrap>;

LocalClient::LocalClient(Server* local_server) : local_server_(local_server) {}

awaitable<void> LocalClient::start() {
    sentry::common::Timeout timeout{1'000'000s};
    co_await timeout();
}

awaitable<LightClientBootstrapPtr> LocalClient::bootstrap_request_v1(const eth::Root& root) {
    const auto serialized_root = root.serialize();
    RequestData request{
        {serialized_root.cbegin(), serialized_root.cend()},
        kLightClientBootstrapV1
    };
    const ResponseData response = co_await local_server_->send_request(request);

    auto bootstrap = std::make_shared<eth::LightClientBootstrap>();
    const bool ok = bootstrap->deserialize(response.data.cbegin(), response.data.cend());
    if (!ok) {
        co_return LightClientBootstrapPtr{};
    }
    co_return bootstrap;
}

awaitable<void> RemoteClient::start() {
    sentry::common::Timeout timeout{1'000'000s};
    co_await timeout();
}

awaitable<LightClientBootstrapPtr> RemoteClient::bootstrap_request_v1(const eth::Root& /*root*/) {
    // TODO(canepat) implement
    co_return LightClientBootstrapPtr{};
}

}  // namespace silkworm::cl::sentinel