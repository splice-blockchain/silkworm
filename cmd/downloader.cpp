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

#include <iostream>
#include <string>
#include <thread>

#include <CLI/CLI.hpp>
#include <boost/format.hpp>

#include <silkworm/buildinfo.h>
#include <silkworm/common/asio_timer.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/settings.hpp>
#include <silkworm/common/stopwatch.hpp>
#include <silkworm/concurrency/signal_handler.hpp>
#include <silkworm/concurrency/worker.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/downloader/internals/body_sequence.hpp>
#include <silkworm/downloader/internals/header_retrieval.hpp>
#include <silkworm/downloader/stage_bodies.hpp>
#include <silkworm/downloader/stage_headers.hpp>

#include "common.hpp"

using namespace silkworm;

class DownloaderLoop final : public Worker {
  public:
    explicit DownloaderLoop(NodeSettings* node_settings, SentryClientSettings* sentry_client_settings, mdbx::env* chaindata_env)
        : Worker("Downloader"),
          node_settings_{node_settings},
          sentry_client_settings_{sentry_client_settings},
          chaindata_env_{chaindata_env} {};
    ~DownloaderLoop() override = default;

    void stop(bool wait = false) final {
        stop_stages();
        Worker::stop(wait);
    };

  private:
    NodeSettings* node_settings_;
    SentryClientSettings* sentry_client_settings_;
    mdbx::env* chaindata_env_;

    std::unique_ptr<SentryClient> sentry_client_{nullptr};
    std::unique_ptr<BlockExchange> block_exchange_{nullptr};

    std::unique_ptr<std::thread> sentry_messages_thread_{nullptr};
    std::unique_ptr<std::thread> sentry_stats_thread_{nullptr};
    std::unique_ptr<std::thread> blocks_download_thread_{nullptr};

    std::map<const char*, std::unique_ptr<Stage>> stages_;
    std::map<const char*, std::unique_ptr<Stage>>::iterator current_stage_;
    std::vector<const char*> stages_forward_order_;
    std::vector<const char*> stages_unwind_order_;
    std::atomic<size_t> current_stages_count_{0};
    std::atomic<size_t> current_stage_number_{0};

    // Main DownloaderLoop work
    void work() final {
        Timer log_timer(
            node_settings_->asio_context, node_settings_->sync_loop_log_interval_seconds * 1'000,
            [&]() -> bool {
                if (!is_running()) {
                    log::Info(get_log_prefix()) << "stopping ...";
                    return false;
                }
                log::Info(get_log_prefix(), current_stage_->second->get_log_progress());
                return true;
            },
            true);

        Stage::Status shared_status{/*first_sync=*/true};
        Hash head_hash;
        intx::uint256 head_td;
        BlockNum head_height;

        try {
            // Open a temporary transaction to see if we have an uncompleted Unwind from previous
            // runs.
            {
                auto txn{chaindata_env_->start_read()};
                db::Cursor source(txn, db::table::kSyncStageProgress);
                mdbx::slice key(db::stages::kUnwindKey);
                auto data{source.find(key, /*throw_notfound=*/false)};
                if (data && data.value.size() == sizeof(BlockNum))
                    shared_status.unwind_point.emplace(endian::load_big_u64(db::from_slice(data.value).data()));
            }

            {
                // Destroy class after usage
                HeaderRetrieval headers_retrieval(db::ROAccess{*chaindata_env_});
                std::tie(head_hash, head_td) = headers_retrieval.head_hash_and_total_difficulty();
                head_height = headers_retrieval.head_height();
            }

            log::Info("Downloader") << "Started";
            log::Message("Chain head", {"hash", head_hash.to_hex(),
                                        "td", intx::to_string(head_td),
                                        "height", std::to_string(head_height)});

            sentry_client_ = std::make_unique<SentryClient>(sentry_client_settings_->api_addr);
            sentry_client_->set_status(head_hash, head_td, sentry_client_settings_->chain_identity.value());
            sentry_client_->hand_shake();

            sentry_messages_thread_ = std::make_unique<std::thread>([&]() -> void {
                log::set_thread_name("sentry receive");
                try {
                    sentry_client_->execution_loop();
                } catch (std::exception& e) {
                    log::Error("sentry_messages_thread_", {"exception", typeid(e).name()}) << e.what();
                } catch (...) {
                    log::Error("sentry_messages_thread_", {"exception", "undefined"});
                }
            });

            sentry_stats_thread_ = std::make_unique<std::thread>([&]() -> void {
                log::set_thread_name("sentry stats");
                try {
                    sentry_client_->stats_receiving_loop();
                } catch (std::exception& e) {
                    log::Error("sentry_stats_thread_", {"exception", typeid(e).name()}) << e.what();
                } catch (...) {
                    log::Error("sentry_stats_thread_", {"exception", "undefined"});
                }
            });

            log::Trace("BlockExchange ctor");
            db::ROAccess bx_db_access(*chaindata_env_);
            block_exchange_ = std::make_unique<BlockExchange>(*sentry_client_,
                                                              bx_db_access,
                                                              sentry_client_settings_->chain_identity.value());

            log::Trace("BlockExchange thread");
            blocks_download_thread_ = std::make_unique<std::thread>([&]() -> void {
                log::set_thread_name("block xchange");
                try {
                    block_exchange_->execution_loop();
                } catch (std::exception& e) {
                    log::Error("blocks_download_thread_", {"exception", typeid(e).name()}) << e.what();
                } catch (...) {
                    log::Error("blocks_download_thread_", {"exception", "undefined"});
                }
            });

            log::Trace("Init stages");
            // Init stages plus forward and unwind order
            stages_.emplace(db::stages::kHeadersKey, std::make_unique<HeadersStage>(shared_status, *block_exchange_, node_settings_));
            stages_.emplace(db::stages::kBlockBodiesKey, std::make_unique<BodiesStage>(shared_status, *block_exchange_, node_settings_));
            stages_forward_order_.insert(stages_forward_order_.begin(),
                                         {
                                             db::stages::kHeadersKey,
                                             db::stages::kBlockBodiesKey,
                                         });
            stages_unwind_order_.insert(stages_unwind_order_.begin(),
                                        {
                                            db::stages::kBlockBodiesKey,
                                            db::stages::kHeadersKey,
                                        });

            // Begin
            while (is_running()) {
                db::RWTxn cycle_txn{*chaindata_env_};

                // Run forward
                if (shared_status.unwind_point.has_value() == false) {
                    bool should_end_loop{false};
                    const auto forward_result{run_cycle(cycle_txn, log_timer)};
                    switch (forward_result) {
                        case Stage::Result::UnwindNeeded:
                            SILKWORM_ASSERT(shared_status.unwind_point.has_value());
                            break;
                        case Stage::Result::Error:
                            should_end_loop = true;
                            break;
                        default:
                            break;
                    }
                    if (should_end_loop) break;
                }

                // Run unwind if required
                if (shared_status.unwind_point.has_value()) {
                    // Need to persist unwind point (in case of user stop)
                    db::stages::write_stage_progress(*cycle_txn, db::stages::kUnwindKey, shared_status.unwind_point.value());
                    cycle_txn.commit(/*renew=*/true);

                    log::Warning("Unwinding", {"to", std::to_string(shared_status.unwind_point.value())});
                    const auto unwind_result{run_cycle(cycle_txn, log_timer, /*forward**/ false)};
                    bool should_end_loop{false};
                    switch (unwind_result) {
                        case Stage::Result::Error:
                            should_end_loop = true;
                            break;
                        default:
                            break;
                    };
                    if (should_end_loop) break;

                    // Erase unwind key from progress table
                    db::Cursor progress_table(*cycle_txn, db::table::kSyncStageProgress);
                    mdbx::slice key(db::stages::kUnwindKey);
                    (void)progress_table.erase(key);
                    cycle_txn.commit();

                    // Clear context
                    shared_status.unwind_point.reset();
                    shared_status.bad_block_hash.reset();
                }

                shared_status.first_sync = false;
            }

        } catch (const mdbx::exception& ex) {
            log::Error(name_,
                       {"function", std::string(__FUNCTION__), "exception", std::string(ex.what())});
        } catch (const std::exception& ex) {
            log::Error(name_,
                       {"function", std::string(__FUNCTION__), "exception", std::string(ex.what())});
        } catch (...) {
            log::Error(name_,
                       {"function", std::string(__FUNCTION__), "exception", "undefined"});
        }

        stop_stages();

        if (block_exchange_) {
            block_exchange_->stop();
            if (blocks_download_thread_) blocks_download_thread_->join();
        }

        if (sentry_client_) {
            sentry_client_->stop();
            if (sentry_messages_thread_) sentry_messages_thread_->join();
            if (sentry_stats_thread_) sentry_stats_thread_->join();
        }

        sentry_messages_thread_.reset();
        sentry_stats_thread_.reset();
        blocks_download_thread_.reset();

        block_exchange_.reset();
        sentry_client_.reset();

        log_timer.stop();
        log::Info("Downloader") << "Ended";
    }

    std::string get_log_prefix() const {
        static const std::string log_prefix_fmt{"[%u/%u %s]"};
        return boost::str(boost::format(log_prefix_fmt) %
                          current_stage_number_ %
                          current_stages_count_ %
                          current_stage_->first);
    }

    void stop_stages() {
        for (const auto& [_, stage] : stages_) {
            if (!stage->is_stopping()) {
                stage->stop();
            }
        }
    }

    Stage::Result run_cycle(db::RWTxn& cycle_txn, Timer& log_timer, bool forward = true) {
        Stage::Result ret{Stage::Result::Done};
        StopWatch stages_stop_watch(true);

        try {
            auto& stages_order{forward ? stages_forward_order_ : stages_unwind_order_};
            current_stages_count_ = stages_order.size();
            current_stage_number_ = 0;
            for (auto& stage_id : stages_order) {
                if (!is_running()) return Stage::Result::Error;
                current_stage_ = stages_.find(stage_id);
                if (current_stage_ == stages_.end()) {
                    // Should not happen
                    throw std::runtime_error("Stage " + std::string(stage_id) + " requested but not implemented");
                }
                ++current_stage_number_;
                current_stage_->second->set_log_prefix(get_log_prefix());
                log_timer.reset();  // Resets the interval for next log line from now
                if (forward) {
                    ret = current_stage_->second->forward(cycle_txn);
                } else {
                    ret = current_stage_->second->unwind(cycle_txn);
                }

                if (static_cast<int>(ret) > 2) {
                    return ret;
                }

                auto [_, stage_duration] = stages_stop_watch.lap();
                if (stage_duration > std::chrono::milliseconds(10)) {
                    log::Info(get_log_prefix(),
                              {"op", (forward ? "Forward" : "Unwind"),
                               "done", StopWatch::format(stage_duration)});
                }
            }

            return is_running() ? ret : Stage::Result::Error;

        } catch (const std::exception& ex) {
            log::Error(get_log_prefix(), {"exception", std::string(ex.what())});
            return Stage::Result::Error;
        }
    }
};

// Main
int main(int argc, char* argv[]) {
    using std::string, std::cout, std::cerr, std::optional, std::to_string;
    using namespace std::chrono;

    // Default values
    CLI::App app{"Downloader. Connect to p2p sentry and start header/body downloading process (stages 1 and 2)"};
    int return_value = 0;

    try {
        SignalHandler::init();
        NodeSettings node_settings{};
        SentryClientSettings sentry_client_settings{};
        log::Settings log_settings;

        // Command line parsing
        cmd::parse_silkworm_command_line(app, argc, argv, log_settings, node_settings, sentry_client_settings);

        // Apply values after parsing
        BodySequence::kMaxBlocksPerMessage = sentry_client_settings.max_blocks_per_request;
        BodySequence::kPerPeerMaxOutstandingRequests = sentry_client_settings.max_peer_outstanding_requests;
        BodySequence::kRequestDeadline = std::chrono::seconds(sentry_client_settings.stale_request_timeout_seconds);
        BodySequence::kNoPeerDelay = std::chrono::seconds(sentry_client_settings.no_peer_timeout_seconds);

        log::init(log_settings);
        log::set_thread_name("main");

        // Output BuildInfo
        auto build_info{silkworm_get_buildinfo()};
        log::Message("SILKWORM DOWNLOADER", {"version", std::string(build_info->git_branch) + std::string(build_info->project_version),
                                             "build", std::string(build_info->system_name) + "-" + std::string(build_info->system_processor) + " " + std::string(build_info->build_type),
                                             "compiler", std::string(build_info->compiler_id) + " " + std::string(build_info->compiler_version)});

        // Prepare database
        cmd::run_preflight_checklist(node_settings);

        // EIP-2124 based chain identity scheme (networkId + genesis + forks)
        sentry_client_settings.chain_identity = lookup_known_chain_identity(node_settings.chain_config->chain_id);
        if (!sentry_client_settings.chain_identity.has_value()) {
            throw std::logic_error("Chain id=" + std::to_string(node_settings.chain_config->chain_id) +
                                   " not supported");
        }

        log::Message("Chain/db status", {"chain-id", to_string(sentry_client_settings.chain_identity->config.chain_id),
                                         "genesis", to_hex(sentry_client_settings.chain_identity->genesis_hash),
                                         "hard_forks", to_string(sentry_client_settings.chain_identity->distinct_fork_numbers().size())});

        // Start boost asio
        using asio_guard_type = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
        auto asio_guard = std::make_unique<asio_guard_type>(node_settings.asio_context.get_executor());
        std::thread asio_thread{[&node_settings]() -> void {
            log::set_thread_name("asio");
            log::Trace("Boost Asio", {"state", "started"});
            node_settings.asio_context.run();
            log::Trace("Boost Asio", {"state", "stopped"});
        }};

        mdbx::env_managed env{db::open_env(node_settings.chaindata_env_config)};
        DownloaderLoop downloader_loop(&node_settings, &sentry_client_settings, &env);
        downloader_loop.start(/*wait=*/false);
        while (downloader_loop.get_state() != Worker::State::kStopped) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            if (SignalHandler::signalled()) {
                downloader_loop.stop(true);
            }
        }

        asio_guard.reset();
        asio_thread.join();

        log::Message() << "Closing Database chaindata path " << node_settings.data_directory->chaindata().path();
        env.close();

    } catch (const CLI::ParseError& ex) {
        return_value = app.exit(ex);
    } catch (std::exception& e) {
        cerr << "Exception (type " << typeid(e).name() << "): " << e.what() << "\n";
        return_value = 1;
    }

    return return_value;
}
