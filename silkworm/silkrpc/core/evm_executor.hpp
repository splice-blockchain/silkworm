/*
   Copyright 2023 The Silkworm Authors

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

#include <memory>
#include <string>
#include <vector>

#include <silkworm/infra/concurrency/coroutine.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/impl/execution_context.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#include <silkworm/core/execution/evm.hpp>
#pragma GCC diagnostic pop
#include <silkworm/core/chain/config.hpp>
#include <silkworm/core/common/assert.hpp>
#include <silkworm/core/common/util.hpp>
#include <silkworm/core/protocol/rule_set.hpp>
#include <silkworm/core/types/block.hpp>
#include <silkworm/core/types/transaction.hpp>
#include <silkworm/silkrpc/core/rawdb/accessors.hpp>
#include <silkworm/silkrpc/core/remote_state.hpp>

namespace silkworm::rpc {

struct ExecutionResult {
    int64_t error_code;
    uint64_t gas_left;
    Bytes data;
    std::optional<std::string> pre_check_error{std::nullopt};
};

constexpr int kCacheSize = 32000;

template <typename T>
using ServiceBase = boost::asio::detail::execution_context_service_base<T>;

class BaselineAnalysisCacheService : public ServiceBase<BaselineAnalysisCacheService> {
  public:
    explicit BaselineAnalysisCacheService(boost::asio::execution_context& owner)
        : ServiceBase<BaselineAnalysisCacheService>(owner) {}

    void shutdown() override {}
    ObjectPool<EvmoneExecutionState>* get_object_pool() { return &state_pool_; }
    BaselineAnalysisCache* get_baseline_analysis_cache() { return &analysis_cache_; }

  private:
    ObjectPool<EvmoneExecutionState> state_pool_{true};
    BaselineAnalysisCache analysis_cache_{kCacheSize, true};
};

using Tracers = std::vector<std::shared_ptr<EvmTracer>>;

class EVMExecutor {
  public:
    static std::string get_error_message(int64_t error_code, const Bytes& error_data, bool full_error = true);

    EVMExecutor(const silkworm::ChainConfig& config, boost::asio::thread_pool& workers, state::RemoteState& remote_state)
        : config_(config),
          workers_{workers},
          remote_state_{remote_state},
          state_{remote_state_},
          rule_set_(protocol::rule_set_factory(config)) {
        SILKWORM_ASSERT(rule_set_);
        if (!has_service<BaselineAnalysisCacheService>(workers_)) {
            make_service<BaselineAnalysisCacheService>(workers_);
        }
    }
    virtual ~EVMExecutor() = default;

    EVMExecutor(const EVMExecutor&) = delete;
    EVMExecutor& operator=(const EVMExecutor&) = delete;

    boost::asio::awaitable<ExecutionResult> call(const silkworm::Block& block, const silkworm::Transaction& txn, Tracers tracers = {},
                                                 bool refund = true, bool gas_bailout = false);
    void reset();

  private:
    static std::optional<std::string> pre_check(const EVM& evm, const silkworm::Transaction& txn,
                                                const intx::uint256& base_fee_per_gas, const intx::uint128& g0);
    uint64_t refund_gas(const EVM& evm, const silkworm::Transaction& txn, uint64_t gas_left, uint64_t gas_refund);

    const silkworm::ChainConfig& config_;
    boost::asio::thread_pool& workers_;
    state::RemoteState& remote_state_;
    IntraBlockState state_;
    protocol::RuleSetPtr rule_set_;
};

}  // namespace silkworm::rpc
