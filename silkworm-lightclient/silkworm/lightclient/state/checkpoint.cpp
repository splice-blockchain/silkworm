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

#include "checkpoint.hpp"

#include <stdexcept>

#include <silkworm/lightclient/util/http_session.hpp>

namespace silkworm::cl {

boost::asio::awaitable<std::unique_ptr<eth::BeaconState>> retrieve_beacon_state(const std::string& checkpoint_uri) {
    const auto content = co_await do_http_session(checkpoint_uri);
    auto beacon_state{std::make_unique<eth::BeaconState>()};
    const bool ok = beacon_state->deserialize(content.cbegin(), content.cend());
    if (!ok) throw std::runtime_error{"Cannot deserialize BeaconState received from: " + checkpoint_uri};
    co_return beacon_state;
}

}  // namespace silkworm::cl