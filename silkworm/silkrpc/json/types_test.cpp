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

#include "types.hpp"

#include <optional>
#include <string>
#include <vector>

#include <catch2/catch.hpp>
#include <evmc/evmc.hpp>
#include <intx/intx.hpp>
#include <nlohmann/json.hpp>

#include <silkworm/core/common/util.hpp>
#include <silkworm/node/db/access_layer.hpp>

namespace silkworm::rpc {

using Catch::Matchers::Message;
using evmc::literals::operator""_address, evmc::literals::operator""_bytes32;
using silkworm::kGiga;
using std::string_literals::operator""s;

TEST_CASE("convert zero uint256 to quantity", "[silkrpc][to_quantity]") {
    intx::uint256 zero_u256{0};
    const auto zero_quantity = to_quantity(zero_u256);
    CHECK(zero_quantity == "0x0");
}

TEST_CASE("convert zero uint256 to quantity(buff)", "[silkrpc][to_quantity]") {
    intx::uint256 zero_u256{0};
    char zero_quantity[64];
    to_quantity(zero_quantity, zero_u256);
    CHECK(strcmp(zero_quantity, "0x0") == 0);
}

TEST_CASE("convert positive uint256 to quantity", "[silkrpc][to_quantity]") {
    intx::uint256 positive_u256{100};
    const auto positive_quantity = to_quantity(positive_u256);
    CHECK(positive_quantity == "0x64");
}

TEST_CASE("convert positive uint256 to quantity(buff)", "[silkrpc][to_quantity]") {
    intx::uint256 positive_u256{100};
    char positive_quantity[64];
    to_quantity(positive_quantity, positive_u256);
    CHECK(strcmp(positive_quantity, "0x64") == 0);
}

TEST_CASE("serialize empty address using to_hex(char *)", "[silkrpc][to_json]") {
    evmc::address address{};
    char address_zero[64];
    to_hex(address_zero, address);
    CHECK(strcmp(address_zero, "0x0000000000000000000000000000000000000000") == 0);
}

TEST_CASE("serialize empty address using to_hex(char *) small buffer", "[silkrpc][to_json]") {
    evmc::address address{};
    char address_zero[10];
    CHECK_THROWS(to_hex(address_zero, address));
}

TEST_CASE("serialize empty address", "[silkrpc][to_json]") {
    evmc::address address{};
    nlohmann::json j = address;
    CHECK(j == R"("0x0000000000000000000000000000000000000000")"_json);
}

TEST_CASE("serialize address", "[silkrpc][to_json]") {
    evmc::address address{0x0715a7794a1dc8e42615f059dd6e406a6594651a_address};
    nlohmann::json j = address;
    CHECK(j == R"("0x0715a7794a1dc8e42615f059dd6e406a6594651a")"_json);
}

TEST_CASE("deserialize empty address", "[silkrpc][from_json]") {
    auto j1 = R"("0000000000000000000000000000000000000000")"_json;
    auto address = j1.get<evmc::address>();
    CHECK(address == evmc::address{});
}

TEST_CASE("deserialize address", "[silkrpc][from_json]") {
    auto j1 = R"("0x0715a7794a1dc8e42615f059dd6e406a6594651a")"_json;
    auto address = j1.get<evmc::address>();
    CHECK(address == evmc::address{0x0715a7794a1dc8e42615f059dd6e406a6594651a_address});
}

TEST_CASE("serialize empty bytes32", "[silkrpc][to_json]") {
    evmc::bytes32 b32{};
    nlohmann::json j = b32;
    CHECK(j == R"("0x0000000000000000000000000000000000000000000000000000000000000000")"_json);
}

TEST_CASE("serialize empty Rlp", "[silkrpc][to_json]") {
    Rlp rlp;
    nlohmann::json j = rlp;
    CHECK(j == R"("0x")"_json);
}

TEST_CASE("serialize not empty Rlp", "[silkrpc][to_json]") {
    Rlp rlp;
    rlp.buffer.push_back(0x78);
    rlp.buffer.push_back(0x24);
    nlohmann::json j = rlp;
    CHECK(j == R"("0x7824")"_json);
}

TEST_CASE("serialize AccessListResult with gas_used", "[silkrpc][to_json]") {
    AccessListResult accessListResult;
    accessListResult.gas_used = 0x1234;
    nlohmann::json j = accessListResult;
    CHECK(j == R"({
        "accessList":[],
        "gasUsed":"0x1234"
    })"_json);
}

TEST_CASE("serialize AccessListResult with error", "[silkrpc][to_json]") {
    AccessListResult accessListResult;
    accessListResult.gas_used = 0x1234;
    accessListResult.error = "operation reverted";
    nlohmann::json j = accessListResult;
    CHECK(j == R"({
        "accessList":[],
        "error":"operation reverted",
        "gasUsed":"0x1234"
    })"_json);
}

TEST_CASE("serialize TxPoolStatusInfo", "[silkrpc][to_json]") {
    TxPoolStatusInfo status_info{};
    status_info.pending = 0x7;
    status_info.queued = 0x8;
    status_info.base_fee = 0x9;
    nlohmann::json j = status_info;
    CHECK(j == R"({
        "baseFee":"0x9",
        "pending":"0x7",
        "queued":"0x8"
    })"_json);
}

TEST_CASE("serialize non-empty bytes32", "[silkrpc][to_json]") {
    evmc::bytes32 b32{0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32};
    nlohmann::json j = b32;
    CHECK(j == R"("0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c")"_json);
}

TEST_CASE("serialize empty block header", "[silkrpc][to_json]") {
    silkworm::BlockHeader header{};
    nlohmann::json j = header;
    CHECK(j == R"({
        "baseFeePerGas":null,
        "hash": "0xc3bd2d00745c03048a5616146a96f5ff78e54efb9e5b04af208cdaff6f3830ee",
        "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "sha3Uncles":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "miner":"0x0000000000000000000000000000000000000000",
        "stateRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "difficulty":"0x0",
        "nonce":"0x0000000000000000",
        "number":"0x0",
        "gasLimit":"0x0",
        "gasUsed":"0x0",
        "timestamp":"0x0",
        "extraData":"0x",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "withdrawalsRoot":null
    })"_json);
}

TEST_CASE("serialize block header", "[silkrpc][to_json]") {
    silkworm::BlockHeader header{
        0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32,
        0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d_bytes32,
        0x0715a7794a1dc8e42615f059dd6e406a6594651a_address,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d_bytes32,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e_bytes32,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f_bytes32,
        silkworm::Bloom{},
        intx::uint256{0},
        uint64_t(5),
        uint64_t(1000000),
        uint64_t(1000000),
        uint64_t(5405021),
        *silkworm::from_hex("0001FF0100"),
        0x0000000000000000000000000000000000000000000000000000000000000001_bytes32,
        {0, 0, 0, 0, 0, 0, 0, 255}};
    nlohmann::json j = header;
    CHECK(j == R"({
        "baseFeePerGas":null,
        "hash": "0x5e053b099d472a3fc02394243961937ffa008bad0daa81a984a0830ba0beee01",
        "parentHash":"0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c",
        "sha3Uncles":"0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d",
        "miner":"0x0715a7794a1dc8e42615f059dd6e406a6594651a",
        "stateRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d",
        "transactionsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e",
        "receiptsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "difficulty":"0x0",
        "number":"0x5",
        "gasLimit":"0xf4240",
        "gasUsed":"0xf4240",
        "timestamp":"0x52795d",
        "extraData":"0x0001ff0100",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000001",
        "nonce":"0x00000000000000ff",
        "withdrawalsRoot":null
    })"_json);
}

TEST_CASE("serialize block header with baseFeePerGas", "[silkrpc][to_json]") {
    silkworm::BlockHeader header{
        0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32,
        0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d_bytes32,
        0x0715a7794a1dc8e42615f059dd6e406a6594651a_address,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d_bytes32,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e_bytes32,
        0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f_bytes32,
        silkworm::Bloom{},
        intx::uint256{0},
        uint64_t(5),
        uint64_t(1000000),
        uint64_t(1000000),
        uint64_t(5405021),
        *silkworm::from_hex("0001FF0100"),                                           // extradata
        0x0000000000000000000000000000000000000000000000000000000000000001_bytes32,  // mixhash
        {1, 2, 3, 4, 5, 6, 7, 8},                                                    // nonce
        std::optional<intx::uint256>(1000),                                          // base_fee_per_gas
    };
    nlohmann::json j = header;
    CHECK(j == R"({
        "baseFeePerGas":"0x3e8",
        "hash": "0x5e3a9484b3ee70cc9ae7673051efd0369cfa4126430075921c70255cbdefbe6",
        "parentHash":"0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c",
        "sha3Uncles":"0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d",
        "miner":"0x0715a7794a1dc8e42615f059dd6e406a6594651a",
        "stateRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d",
        "transactionsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e",
        "receiptsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "difficulty":"0x0",
        "number":"0x5",
        "gasLimit":"0xf4240",
        "gasUsed":"0xf4240",
        "timestamp":"0x52795d",
        "extraData":"0x0001ff0100",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000001",
        "nonce":"0x0102030405060708",
        "baseFeePerGas":"0x3e8",
        "withdrawalsRoot":null
    })"_json);
}

TEST_CASE("serialize block with baseFeePerGas", "[silkrpc][to_json]") {
    silkworm::rpc::Block rpc_block{
        { /* BlockWithHash */
         {/* Block */
          {
              /* BlockBody */
              .transactions = std::vector<silkworm::Transaction>{},
              .ommers = std::vector<silkworm::BlockHeader>{},
              .withdrawals = std::nullopt,
          },
          {
              /* BlockHeader */
              .parent_hash = 0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32,
              .ommers_hash = 0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d_bytes32,
              .beneficiary = 0x0715a7794a1dc8e42615f059dd6e406a6594651a_address,
              .state_root = 0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d_bytes32,
              .transactions_root = 0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e_bytes32,
              .receipts_root = 0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f_bytes32,
              .logs_bloom = silkworm::Bloom{},
              .difficulty = intx::uint256{0},
              .number = uint64_t(5),
              .gas_limit = uint64_t(1000000),
              .gas_used = uint64_t(1000000),
              .timestamp = uint64_t(5405021),
              .extra_data = *silkworm::from_hex("0001FF0100"),
              .prev_randao = 0x0000000000000000000000000000000000000000000000000000000000000001_bytes32,
              .nonce = {0, 0, 0, 0, 0, 0, 0, 255},
              .base_fee_per_gas = std::optional<intx::uint256>(0x244428),
          }}}};
    auto body = rpc_block.block;
    body.transactions.resize(2);
    body.transactions[0].nonce = 172339;
    body.transactions[0].max_priority_fee_per_gas = 50 * kGiga;
    body.transactions[0].max_fee_per_gas = 50 * kGiga;
    body.transactions[0].gas_limit = 90'000;
    body.transactions[0].to = 0xe5ef458d37212a06e3f59d40c454e76150ae7c32_address;
    body.transactions[0].value = 1'027'501'080 * kGiga;
    body.transactions[0].data = {};
    REQUIRE(body.transactions[0].set_v(27));
    body.transactions[0].r =
        intx::from_string<intx::uint256>("0x48b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353");
    body.transactions[0].s =
        intx::from_string<intx::uint256>("0x1fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804");

    body.transactions[1].type = TransactionType::kEip1559;
    body.transactions[1].nonce = 1;
    body.transactions[1].max_priority_fee_per_gas = 5 * kGiga;
    body.transactions[1].max_fee_per_gas = 30 * kGiga;
    body.transactions[1].gas_limit = 1'000'000;
    body.transactions[1].to = {};
    body.transactions[1].value = 0;
    body.transactions[1].data = *silkworm::from_hex("602a6000556101c960015560068060166000396000f3600035600055");
    REQUIRE(body.transactions[1].set_v(37));
    body.transactions[1].r =
        intx::from_string<intx::uint256>("0x52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb");
    body.transactions[1].s =
        intx::from_string<intx::uint256>("0x52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb");

    body.ommers.resize(1);
    body.ommers[0].parent_hash = 0xb397a22bb95bf14753ec174f02f99df3f0bdf70d1851cdff813ebf745f5aeb55_bytes32;
    body.ommers[0].ommers_hash = silkworm::kEmptyListHash;
    body.ommers[0].beneficiary = 0x0c729be7c39543c3d549282a40395299d987cec2_address;
    body.ommers[0].state_root = 0xc2bcdfd012534fa0b19ffba5fae6fc81edd390e9b7d5007d1e92e8e835286e9d_bytes32;
    body.ommers[0].transactions_root = silkworm::kEmptyRoot;
    body.ommers[0].receipts_root = silkworm::kEmptyRoot;
    body.ommers[0].difficulty = 12'555'442'155'599;
    body.ommers[0].number = 13'000'013;
    body.ommers[0].gas_limit = 3'141'592;
    body.ommers[0].gas_used = 0;
    body.ommers[0].timestamp = 1455404305;
    body.ommers[0].prev_randao = 0xf0a53dfdd6c2f2a661e718ef29092de60d81d45f84044bec7bf4b36630b2bc08_bytes32;
    body.ommers[0].nonce[7] = 35;

    nlohmann::json j = rpc_block;
    CHECK(j == R"({
        "parentHash":"0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c",
        "sha3Uncles":"0x474f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126d",
        "miner":"0x0715a7794a1dc8e42615f059dd6e406a6594651a",
        "stateRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126d",
        "transactionsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126e",
        "receiptsRoot":"0xb02a3b0ee16c858afaa34bcd6770b3c20ee56aa2f75858733eb0e927b5b7126f",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "difficulty":"0x0",
        "number":"0x5",
        "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "gasLimit":"0xf4240",
        "gasUsed":"0xf4240",
        "timestamp":"0x52795d",
        "size":"0x207",
        "extraData":"0x0001ff0100",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000001",
        "nonce":"0x00000000000000ff",
        "baseFeePerGas":"0x244428",
        "totalDifficulty":"0x0",
        "transactions":[],
        "uncles":[]
    })"_json);
}

TEST_CASE("serialize empty block", "[silkrpc][to_json]") {
    silkworm::rpc::Block block{};
    nlohmann::json j = block;
    CHECK(j == R"({
        "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "sha3Uncles":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "miner":"0x0000000000000000000000000000000000000000",
        "stateRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
               R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "difficulty":"0x0",
        "nonce":"0x0000000000000000",
        "number":"0x0",
        "gasLimit":"0x0",
        "gasUsed":"0x0",
        "timestamp":"0x0",
        "extraData":"0x",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "size":"0x1f5",
        "totalDifficulty":"0x0",
        "transactions":[],
        "uncles":[]
    })"_json);
}

TEST_CASE("serialize EIP-2718 block", "[silkrpc][to_json]") {
    const char* rlp_hex{
        "f90319f90211a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd4"
        "1ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165"
        "b75356e0314bf0706f279c729f51e017a0e6e49996c7ec59f7a23d22b83239a60151512c65613bf84a0d7da336399ebc4aa0cafe75574d"
        "59780665a97fbfd11365c7545aa8f1abf4e5e12e8243334ef7286bb9010000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "000000000000000000000083020000820200832fefd882a410845506eb0796636f6f6c65737420626c6f636b206f6e20636861696ea0bd"
        "4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f90101f85f800a82c35094095e7bae"
        "a6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8f"
        "ae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1b89e01f89b01800a8301e24194095e7baea6a6c7c4c2dfeb97"
        "7efac326af552d878080f838f7940000000000000000000000000000000000000001e1a000000000000000000000000000000000000000"
        "0000000000000000000000000001a03dbacc8d0259f2508625e97fdfc57cd85fdd16e5821bc2c10bdd1a52649e8335a0476e10695b183a"
        "87b0aa292a7f4b78ef0c3fbe62aa2c42c84e1d9c3da159ef14c0"};
    silkworm::Bytes rlp_bytes{*silkworm::from_hex(rlp_hex)};
    silkworm::ByteView view{rlp_bytes};

    silkworm::rpc::Block rpc_block;
    REQUIRE(silkworm::rlp::decode(view, rpc_block.block));

    nlohmann::json rpc_block_json = rpc_block;
    CHECK(rpc_block_json == R"({
        "difficulty":"0x20000",
        "extraData":"0x636f6f6c65737420626c6f636b206f6e20636861696e",
        "gasLimit":"0x2fefd8",
        "gasUsed":"0xa410",
        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "miner":"0x8888f1f195afa192cfee860698584c030f4c9db1",
        "mixHash":"0xbd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff498",
        "nonce":"0xa13a5a8c8f2bb1c4",
        "number":"0x200",
        "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot":"0xcafe75574d59780665a97fbfd11365c7545aa8f1abf4e5e12e8243334ef7286b",
        "sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "size":"0x31c",
        "stateRoot":"0xef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017",
        "timestamp":"0x5506eb07",
        "totalDifficulty":"0x0",
        "transactions":[
            "0x77b19baa4de67e45a7b26e4a220bccdbb6731885aa9927064e239ca232023215",
            "0x554af720acf477830f996f1bc5d11e54c38aa40042aeac6f66cb66f9084a959d"
        ],
        "transactionsRoot":"0xe6e49996c7ec59f7a23d22b83239a60151512c65613bf84a0d7da336399ebc4a",
        "uncles":[]
    })"_json);
}

TEST_CASE("serialize block with hydrated transactions", "[silkrpc][to_json]") {
    // 1) build block https://goerli.etherscan.io/block/3529604
    // 1.1) value from table Header for key 000000000035db84
    const char* header_rlp_hex{
        "f9025ca08059c265f40cdb2d3b3245847c21ed154eebf299fd0ff01ee3afded43cdadc45a01dcc4de8dec75d7aab85b567b6ccd41ad312"
        "451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a08add6cb86a4b4a4e5758ce21c8d156e4355917"
        "d29eae7c19f56d4a38f384401da095e5f810e7a45d476d7416fbffbc931473cfdba2b90204e019067bcc6d136dc3a08c3d469c1fbce4e4"
        "144d5e5f91a81baca60b1fb6b5bdcf691b9dc40a5bf21b35b9010004000000000000000000000000040010001000402000000000000000"
        "00000008000020001000000001000000000080000000000010000000000800000000000000000000000000000000000000000000000000"
        "10100000000000000000000008000008000000000000000000000000002000000000000000000000000000040000000000000010000000"
        "00000000000000000000000000000000000000400000000000000000000000020180440020000000080000000000000000000000000000"
        "00000000000000000000000000000000020000000000000000000000000000000000000000000000180000002000004010000880800000"
        "0200400000000000018335db84837a12008308b89a845f7cd33db861476f65726c6920496e697469617469766520417574686f72697479"
        "00000000001f3070be3668d4e3bdd1d08969becd5b06ab0ae4224873453d827a67b3a089ee03c69941418ac300e2c3ca9b5597c7a37959"
        "32a7ff2f907db605a93a88c5b4a800a0000000000000000000000000000000000000000000000000000000000000000088000000000000"
        "0000"};
    silkworm::Bytes header_rlp_bytes{*silkworm::from_hex(header_rlp_hex)};
    silkworm::ByteView header_view{header_rlp_bytes};
    silkworm::BlockHeader header;
    REQUIRE(silkworm::rlp::decode(header_view, header));

    // 1.2) value from table BlockBody for key 000000000035db84c9e65d063911aa583e17bbb7070893482203217caf6d9fbb50265c72e7bf73e5
    const char* body_rlp_hex{"c68341b58302c0"};
    silkworm::Bytes body_rlp_bytes{*silkworm::from_hex(body_rlp_hex)};
    silkworm::ByteView body_view{body_rlp_bytes};
    const auto body_for_storage{silkworm::db::detail::decode_stored_block_body(body_view)};
    REQUIRE(body_for_storage.txn_count == 2);
    REQUIRE(body_for_storage.base_txn_id == 0x41b583);

    // 1.3) value from table BlockTransaction for key 000000000041b583 and 000000000041b584
    const char* tx1_rlp_hex{
        "f87080843b9aca00830c350094fa365f1384e4eaf6d59f353c782af3ea42feaab988015c2a7b13fd000084d0e30db02ea06b0df7c31119"
        "b257e7faeb391984f199c8da817b14279ac09262bdf3493599a6a00c729ce28ec0030002490d6217a8b50041495925142e70fa1b77e465"
        "eab97c4b"};
    silkworm::Bytes tx1_rlp_bytes{*silkworm::from_hex(tx1_rlp_hex)};
    silkworm::ByteView tx1_view{tx1_rlp_bytes};
    silkworm::Transaction tx1;
    REQUIRE(silkworm::rlp::decode(tx1_view, tx1));
    const char* tx2_rlp_hex{
        "f901aa02843b9aca008304fa4a9431af35bdfa897cd42b204c003560c385d444707580b901449b4e463400000000000000000000000000"
        "0000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000c081c2ac5b"
        "ba256c88daa744c9caa7d6c99c32c1bc0c07bdca87bd2a054118c47b000000000000000000000000000000000000000000000000000000"
        "0000000030a5a151a2320abaab98cfa8366fc326fb6f45cf1c93697191ec1370e1caca0fc6237e3bc5328755ae66bc5ddb141f0cb10000"
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000060a4dcd35675e049ea5b"
        "58d9567f8029669d4cdbe72511d330d96a578e2714f1c9db00f6a9babc217b250fc7f217b0261506727657b420d9e05adc73675390ce2e"
        "b1e1aef3bac7d1b4b424c9dc07cdcac2729eabdb81c857325e20202ea24761601ba01d8e665abc1278a9526aaf4c604f75b293e43ccf9d"
        "c72918a633af584b73425ba07f8913ecd5db0e98d48097abefd7b2fa954d7cf1514496b870b8a1335034df4d"};
    silkworm::Bytes tx2_rlp_bytes{*silkworm::from_hex(tx2_rlp_hex)};
    silkworm::ByteView tx2_view{tx2_rlp_bytes};
    silkworm::Transaction tx2;
    REQUIRE(silkworm::rlp::decode(tx2_view, tx2));

    // 1.4) build the full block
    silkworm::rpc::Block rpc_block{
        {
            // BlockWithHash
            /*.block =*/{
                // Block
                {
                    // BlockBody
                    .transactions = std::vector<silkworm::Transaction>{tx1, tx2},
                    .ommers = std::vector<silkworm::BlockHeader>{},
                    .withdrawals = std::nullopt,
                },
                /*.header =*/header,
            },
            /*.hash =*/0xc9e65d063911aa583e17bbb7070893482203217caf6d9fbb50265c72e7bf73e5_bytes32,
        },
        /*.total_difficulty =*/intx::uint256{0x4e33ae},
        /*.full_tx =*/true,
    };

    nlohmann::json rpc_block_json = rpc_block;
    CHECK(rpc_block_json == R"({
        "difficulty":"0x1",
        "extraData":"0x476f65726c6920496e697469617469766520417574686f7269747900000000001f3070be3)"
                            R"(668d4e3bdd1d08969becd5b06ab0ae4224873453d827a67b3a089ee03c69941418ac300e2c3c)"
                            R"(a9b5597c7a3795932a7ff2f907db605a93a88c5b4a800",
        "gasLimit":"0x7a1200",
        "gasUsed":"0x8b89a",
        "hash":"0xc9e65d063911aa583e17bbb7070893482203217caf6d9fbb50265c72e7bf73e5",
        "logsBloom":"0x040000000000000000000000000400100010004020000000000000000000000800002000)"
                            R"(100000000100000000008000000000001000000000080000000000000000000000000000)"
                            R"(000000000000000000000010100000000000000000000008000008000000000000000000)"
                            R"(000000002000000000000000000000000000040000000000000010000000000000000000)"
                            R"(000000000000000000000000004000000000000000000000000201804400200000000800)"
                            R"(000000000000000000000000000000000000000000000000000000000002000000000000)"
                            R"(000000000000000000000000000000000018000000200000401000088080000002004000)"
                            R"(00000000",
        "miner":"0x0000000000000000000000000000000000000000",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "nonce":"0x0000000000000000",
        "number":"0x35db84",
        "parentHash":"0x8059c265f40cdb2d3b3245847c21ed154eebf299fd0ff01ee3afded43cdadc45",
        "receiptsRoot":"0x8c3d469c1fbce4e4144d5e5f91a81baca60b1fb6b5bdcf691b9dc40a5bf21b35",
        "sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "size":"0x485",
        "stateRoot":"0x8add6cb86a4b4a4e5758ce21c8d156e4355917d29eae7c19f56d4a38f384401d",
        "timestamp":"0x5f7cd33d",
        "totalDifficulty":"0x4e33ae",
        "transactions":[
            {
                "blockHash":"0xc9e65d063911aa583e17bbb7070893482203217caf6d9fbb50265c72e7bf73e5",
                "blockNumber":"0x35db84",
                "from":"0x4ed7fae4af36f11ac28275a98ca1d131e91bb6cd",
                "gas":"0xc3500",
                "gasPrice":"0x3b9aca00",
                "hash":"0xa52100232ad8abc15bdcd95b071194d2084781f88a71974eef7292c8513a03b4",
                "input":"0xd0e30db0",
                "nonce":"0x0",
                "r":"0x6b0df7c31119b257e7faeb391984f199c8da817b14279ac09262bdf3493599a6",
                "s":"0xc729ce28ec0030002490d6217a8b50041495925142e70fa1b77e465eab97c4b",
                "to":"0xfa365f1384e4eaf6d59f353c782af3ea42feaab9",
                "transactionIndex":"0x0",
                "type":"0x0",
                "v":"0x2e",
                "chainId":"0x5",
                "value":"0x15c2a7b13fd0000"
            },
            {
                "blockHash":"0xc9e65d063911aa583e17bbb7070893482203217caf6d9fbb50265c72e7bf73e5",
                "blockNumber":"0x35db84",
                "from":"0xab2e6a1020c511615f82155259086717802d1474",
                "gas":"0x4fa4a",
                "gasPrice":"0x3b9aca00",
                "hash":"0x81d69137fe27a549e957c2dd3d54f374a019bf12409ca44fb9e01dc82ac7e925",
                "input":"0x9b4e463400000000000000000000000000000000000000000000000000000000000000600)"
                            R"(0000000000000000000000000000000000000000000000000000000000000c081c2ac5bba256)"
                            R"(c88daa744c9caa7d6c99c32c1bc0c07bdca87bd2a054118c47b0000000000000000000000000)"
                            R"(000000000000000000000000000000000000030a5a151a2320abaab98cfa8366fc326fb6f45c)"
                            R"(f1c93697191ec1370e1caca0fc6237e3bc5328755ae66bc5ddb141f0cb100000000000000000)"
                            R"(0000000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(060a4dcd35675e049ea5b58d9567f8029669d4cdbe72511d330d96a578e2714f1c9db00f6a9b)"
                            R"(abc217b250fc7f217b0261506727657b420d9e05adc73675390ce2eb1e1aef3bac7d1b4b424c)"
                            R"(9dc07cdcac2729eabdb81c857325e20202ea2476160",
                "nonce":"0x2",
                "r":"0x1d8e665abc1278a9526aaf4c604f75b293e43ccf9dc72918a633af584b73425b",
                "s":"0x7f8913ecd5db0e98d48097abefd7b2fa954d7cf1514496b870b8a1335034df4d",
                "to":"0x31af35bdfa897cd42b204c003560c385d4447075",
                "transactionIndex":"0x1",
                "type":"0x0",
                "v":"0x1b",
                "value":"0x0"
            }
        ],
        "transactionsRoot":"0x95e5f810e7a45d476d7416fbffbc931473cfdba2b90204e019067bcc6d136dc3",
        "uncles":[]
    })"_json);
}

TEST_CASE("serialize block body with ommers", "[silkrpc][to_json]") {
    // https://etherscan.io/block/3
    const char* rlp_hex{
        "f90219c0f90215f90212a0d4e56740f876aef8c010b86a40d5f56745a118d090"
        "6a34e69aec8c0db1cb8fa3a01dcc4de8dec75d7aab85b567b6ccd41ad312451b"
        "948a7413f0a142fd40d4934794c8ebccc5f5689fa8659d83713341e5ad193494"
        "48a01e6e030581fd1873b4784280859cd3b3c04aa85520f08c304cf5ee63d393"
        "5adda056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e3"
        "63b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5"
        "e363b421b9010000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "000000000000008503ff80000001821388808455ba42429a5961746573205261"
        "6e64616c6c202d2045746865724e696e6a61a0f8c94dfe61cf26dcdf8cffeda3"
        "37cf6a903d65c449d7691a022837f6e2d994598868b769c5451a7aea"};
    silkworm::Bytes rlp_bytes{*silkworm::from_hex(rlp_hex)};
    silkworm::ByteView in{rlp_bytes};

    silkworm::rpc::Block rpc_block;
    silkworm::BlockBody block_body;
    REQUIRE(silkworm::rlp::decode(in, block_body));
    rpc_block.block.ommers = block_body.ommers;

    nlohmann::json rpc_block_json = rpc_block;
    CHECK(rpc_block_json == R"({
        "difficulty":"0x0",
        "extraData":"0x",
        "gasLimit":"0x0",
        "gasUsed":"0x0",
        "hash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "logsBloom":"0x000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(000000000000000000000000000000000000000000000000000000000000000000000000)"
                            R"(00000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "miner":"0x0000000000000000000000000000000000000000",
        "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "nonce":"0x0000000000000000",
        "number":"0x0",
        "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "sha3Uncles":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "size":"0x40c",
        "stateRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "timestamp":"0x0",
        "totalDifficulty":"0x0",
        "transactions":[],
        "transactionsRoot":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "uncles":["0x5cd50096dbb856a6d1befa6de8f9c20decb299f375154427d90761dc0b101109"]
    })"_json);
}

TEST_CASE("serialize empty transaction", "[silkrpc][to_json]") {
    silkworm::Transaction txn{};
    nlohmann::json j = txn;
    CHECK(j == R"({
        "nonce":"0x0",
        "gas":"0x0",
        "to":null,
        "type":"0x0",
        "value":"0x0",
        "input":"0x",
        "hash":"0x3763e4f6e4198413383534c763f3f5dac5c5e939f0a81724e3beb96d6e2ad0d5",
        "r":"0x0",
        "s":"0x0",
        "v":"0x1b"
    })"_json);
}

TEST_CASE("serialize empty call_bundle", "[silkrpc][to_json]") {
    struct CallBundleInfo bundle_info {};

    nlohmann::json j = bundle_info;
    CHECK(j == R"({
        "bundleHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "results":[]
    })"_json);
}

TEST_CASE("serialize call_bundle with error", "[silkrpc][to_json]") {
    struct CallBundleInfo bundle_info {};
    struct CallBundleTxInfo tx_info {};
    tx_info.gas_used = 0x234;
    tx_info.error_message = "operation reverted";
    bundle_info.txs_info.push_back(tx_info);

    nlohmann::json j = bundle_info;
    CHECK(j == R"({
        "bundleHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "results":[{"error": "operation reverted", "gasUsed": 564,
                    "txHash": "0x0000000000000000000000000000000000000000000000000000000000000000"}]
    })"_json);
}

TEST_CASE("serialize call_bundle with value", "[silkrpc][to_json]") {
    struct CallBundleInfo bundle_info {};
    struct CallBundleTxInfo tx_info {};
    tx_info.gas_used = 0x234;
    tx_info.value = 0x1230000000000000000000000000000000000000000000000000000000000321_bytes32;
    bundle_info.txs_info.push_back(tx_info);

    nlohmann::json j = bundle_info;
    CHECK(j == R"({
        "bundleHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "results":[{"value": "0x1230000000000000000000000000000000000000000000000000000000000321", "gasUsed": 564,
                    "txHash": "0x0000000000000000000000000000000000000000000000000000000000000000"}]
    })"_json);
}

TEST_CASE("serialize filled SyncingData", "[silkrpc][to_json]") {
    SyncingData syncing_data{};
    StageData stage_data;

    syncing_data.current_block = "0x1";
    syncing_data.highest_block = "0x2";
    stage_data.stage_name = "stage1";
    stage_data.block_number = "0x3";
    syncing_data.stages.push_back(stage_data);
    stage_data.stage_name = "stage2";
    stage_data.block_number = "0x4";
    syncing_data.stages.push_back(stage_data);

    nlohmann::json j = syncing_data;
    CHECK(j == R"({
      "currentBlock":"0x1","highestBlock":"0x2","stages":[{"block_number":"0x3","stage_name":"stage1"},{"block_number":"0x4","stage_name":"stage2"}]
    })"_json);
}

TEST_CASE("serialize legacy transaction (type=0)", "[silkrpc][to_json]") {
    // https://etherscan.io/tx/0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060
    // Block 46147
    silkworm::Transaction txn1{
        {.type = TransactionType::kLegacy,
         .nonce = 0,
         .max_priority_fee_per_gas = 50'000 * kGiga,
         .max_fee_per_gas = 50'000 * kGiga,
         .gas_limit = 21'000,
         .to = 0x5df9b87991262f6ba471f09758cde1c0fc1de734_address,
         .value = 31337},
        true,                                                                                                    // odd_y_parity
        intx::from_string<intx::uint256>("0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0"),  // r
        intx::from_string<intx::uint256>("0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"),  // s
    };
    nlohmann::json j1 = txn1;
    CHECK(j1 == R"({
        "from":"0xa1e4380a3b1f749673e270229993ee55f35663b4",
        "gas":"0x5208",
        "hash":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
        "input":"0x",
        "nonce":"0x0",
        "r":"0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
        "s":"0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
        "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
        "type":"0x0",
        "v":"0x1c",
        "value":"0x7a69"
    })"_json);

    silkworm::rpc::Transaction txn2{
        {
            {.type = TransactionType::kLegacy,
             .nonce = 0,
             .max_priority_fee_per_gas = 50'000 * kGiga,
             .max_fee_per_gas = 50'000 * kGiga,
             .gas_limit = 21'000,
             .to = 0x5df9b87991262f6ba471f09758cde1c0fc1de734_address,
             .value = 31337},
            true,                                                                                                    // odd_y_parity
            intx::from_string<intx::uint256>("0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0"),  // r
            intx::from_string<intx::uint256>("0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"),  // s
            0x007fb8417eb9ad4d958b050fc3720d5b46a2c053_address,                                                      // from
        },
        0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd_bytes32,  // block_hash
        46147,                                                                       // block_number
        intx::uint256{0},                                                            // block_base_fee_per_gas
        0                                                                            // transactionIndex
    };
    nlohmann::json j2 = txn2;
    CHECK(j2 == R"({
        "blockHash":"0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
        "blockNumber":"0xb443",
        "from":"0x007fb8417eb9ad4d958b050fc3720d5b46a2c053",
        "gas":"0x5208",
        "gasPrice":"0x2d79883d2000",
        "hash":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
        "input":"0x",
        "nonce":"0x0",
        "r":"0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
        "s":"0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
        "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
        "transactionIndex":"0x0",
        "type":"0x0",
        "v":"0x1c",
        "value":"0x7a69"
    })"_json);
    silkworm::rpc::Transaction txn3{
        {
            {.type = TransactionType::kLegacy,
             .nonce = 0,
             .max_priority_fee_per_gas = 50'000 * kGiga,
             .max_fee_per_gas = 50'000 * kGiga,
             .gas_limit = 21'000,
             .to = 0x5df9b87991262f6ba471f09758cde1c0fc1de734_address,
             .value = 31337},
            true,                                                                                                    // odd_y_parity
            intx::from_string<intx::uint256>("0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0"),  // r
            intx::from_string<intx::uint256>("0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"),  // s
            0x007fb8417eb9ad4d958b050fc3720d5b46a2c053_address,                                                      // from
        },
        0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd_bytes32,  // block_hash
        46147,                                                                       // block_number
        intx::uint256{0},                                                            // block_base_fee_per_gas
        0,                                                                           // transactionIndex
        true                                                                         // queued_in_pool
    };
    nlohmann::json j3 = txn3;
    CHECK(j3 == R"({
        "blockHash":null,
        "blockNumber":null,
        "from":"0x007fb8417eb9ad4d958b050fc3720d5b46a2c053",
        "gas":"0x5208",
        "gasPrice":"0x2d79883d2000",
        "hash":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
        "input":"0x",
        "nonce":"0x0",
        "r":"0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
        "s":"0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
        "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
        "transactionIndex":null,
        "type":"0x0",
        "v":"0x1c",
        "value":"0x7a69"
    })"_json);
}

TEST_CASE("serialize EIP-2930 transaction (type=1)", "[silkrpc][to_json]") {
    silkworm::Transaction txn1{
        {.type = TransactionType::kEip2930,
         .chain_id = 1,
         .nonce = 0,
         .max_priority_fee_per_gas = 20000000000,
         .max_fee_per_gas = 20000000000,
         .gas_limit = 0,
         .to = 0x0715a7794a1dc8e42615f059dd6e406a6594651a_address,
         .value = 0,
         .data = *from_hex("001122aabbcc")},
        false,                                               // odd_y_parity
        18,                                                  // r
        36,                                                  // s
        0x007fb8417eb9ad4d958b050fc3720d5b46a2c053_address,  // from
    };
    nlohmann::json j1 = txn1;
    CHECK(j1 == R"({
        "nonce":"0x0",
        "chainId":"0x1",
        "gas":"0x0",
        "to":"0x0715a7794a1dc8e42615f059dd6e406a6594651a",
        "from":"0x007fb8417eb9ad4d958b050fc3720d5b46a2c053",
        "type":"0x1",
        "value":"0x0",
        "input":"0x001122aabbcc",
        "hash":"0xe976a1c7600ed37c7aeea9b34de01b2424a68a4c9dfb0a0315a3db3cd9975512",
        "accessList":[],
        "r":"0x12",
        "s":"0x24",
        "v":"0x0"
    })"_json);

    std::vector<silkworm::AccessListEntry> access_list{
        {0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae_address,
         {
             0x0000000000000000000000000000000000000000000000000000000000000003_bytes32,
             0x0000000000000000000000000000000000000000000000000000000000000007_bytes32,
         }},
        {0xbb9bc244d798123fde783fcc1c72d3bb8c189413_address, {}},
    };

    silkworm::rpc::Transaction txn2{
        {
            {.type = TransactionType::kEip2930,
             .chain_id = 1,
             .nonce = 0,
             .max_priority_fee_per_gas = 20000000000,
             .max_fee_per_gas = 30000000000,
             .gas_limit = 0,
             .to = 0x0715a7794a1dc8e42615f059dd6e406a6594651a_address,
             .value = 0,
             .data = *from_hex("001122aabbcc"),
             .access_list = access_list},
            false,                                               // odd_y_parity
            18,                                                  // r
            36,                                                  // s
            0x007fb8417eb9ad4d958b050fc3720d5b46a2c053_address,  // from
        },
        0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32,
        123123,
        intx::uint256{12},
        3};
    nlohmann::json j2 = txn2;
    CHECK(j2 == R"({
        "nonce":"0x0",
        "gasPrice":"0x4a817c80c",
        "chainId":"0x1",
        "gas":"0x0",
        "to":"0x0715a7794a1dc8e42615f059dd6e406a6594651a",
        "from":"0x007fb8417eb9ad4d958b050fc3720d5b46a2c053",
        "type":"0x1",
        "value":"0x0",
        "input":"0x001122aabbcc",
        "hash":"0xae1aea7493cc9a029710b601f62538993ebc6281ac63a241b83a218bd060b291",
        "r":"0x12",
        "s":"0x24",
        "v":"0x0",
        "blockHash":"0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c",
        "blockNumber":"0x1e0f3",
        "transactionIndex":"0x3",
        "accessList":[
            {
                "address":"0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
                "storageKeys":[
                    "0x0000000000000000000000000000000000000000000000000000000000000003",
                    "0x0000000000000000000000000000000000000000000000000000000000000007"
                ]
            },
            {
                "address":"0xbb9bc244d798123fde783fcc1c72d3bb8c189413",
                "storageKeys":[]
            }
        ]
    })"_json);
}

TEST_CASE("serialize EIP-1559 transaction (type=2)", "[silkrpc][to_json]") {
    silkworm::Transaction txn1{
        {.type = TransactionType::kEip1559,
         .chain_id = 1,
         .nonce = 0,
         .max_priority_fee_per_gas = 50'000 * kGiga,
         .max_fee_per_gas = 50'000 * kGiga,
         .gas_limit = 21'000,
         .to = 0x5df9b87991262f6ba471f09758cde1c0fc1de734_address,
         .value = 31337,
         .data = *from_hex("001122aabbcc")},
        true,                                                                                                    // odd_y_parity
        intx::from_string<intx::uint256>("0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0"),  // r
        intx::from_string<intx::uint256>("0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"),  // s
        0x007fb8417eb9ad4d958b050fc3720d5b46a2c053_address,                                                      // from
    };
    nlohmann::json j1 = txn1;
    CHECK(j1 == R"({
        "nonce":"0x0",
        "chainId":"0x1",
        "gas":"0x5208",
        "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
        "from":"0x007fb8417eb9ad4d958b050fc3720d5b46a2c053",
        "type":"0x2",
        "value":"0x7a69",
        "input":"0x001122aabbcc",
        "hash":"0x64ab530a48c64d248b85dd6952539cae03cad7a001ed32ba5d358aca20eef0a8",
        "accessList":[],
        "r":"0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
        "s":"0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a",
        "v":"0x1",
        "maxPriorityFeePerGas":"0x2d79883d2000",
        "maxFeePerGas":"0x2d79883d2000"
    })"_json);
}

TEST_CASE("serialize error", "[silkrpc][to_json]") {
    Error err{100, {"generic error"}};
    nlohmann::json j = err;
    CHECK(j == R"({
        "code":100,
        "message":"generic error"
    })"_json);
}

TEST_CASE("serialize std::set<evmc::address>", "[silkrpc][to_json]") {
    std::set<evmc::address> addresses;

    SECTION("empty addresses set") {
        nlohmann::json j;
        to_json(j, addresses);
        CHECK(j == R"([])"_json);
    }

    SECTION("filled addresses set") {
        addresses.insert(0x07aaec0b237ccf56b03a7c43c1c7a783da560642_address);
        nlohmann::json j;
        to_json(j, addresses);
        CHECK(j == R"(["0x07aaec0b237ccf56b03a7c43c1c7a783da560642"])"_json);
    }
}

TEST_CASE("deserialize block_number_or_hash", "[silkworm::json][from_json]") {
    SECTION("as hash") {
        auto json = R"("0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c")"_json;
        auto bnoh = json.get<BlockNumberOrHash>();

        CHECK(bnoh.is_hash() == true);
        CHECK(bnoh.is_number() == false);
        CHECK(bnoh.is_tag() == false);
        CHECK(bnoh.hash() == 0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32);
    }

    SECTION("as decimal number string") {
        auto json = R"("1966")"_json;
        auto bnoh = json.get<BlockNumberOrHash>();

        CHECK(bnoh.is_hash() == false);
        CHECK(bnoh.is_number() == true);
        CHECK(bnoh.is_tag() == false);
        CHECK(bnoh.number() == 1966);
    }

    SECTION("as hex number string") {
        auto json = R"("0x374f3")"_json;
        auto bnoh = json.get<BlockNumberOrHash>();

        CHECK(bnoh.is_hash() == false);
        CHECK(bnoh.is_number() == true);
        CHECK(bnoh.is_tag() == false);
        CHECK(bnoh.number() == 0x374f3);
    }

    SECTION("as tag string") {
        auto json = R"("latest")"_json;
        auto bnoh = json.get<BlockNumberOrHash>();

        CHECK(bnoh.is_hash() == false);
        CHECK(bnoh.is_number() == false);
        CHECK(bnoh.is_tag() == true);
        CHECK(bnoh.tag() == "latest");
    }

    SECTION("as number") {
        auto json = R"(123456)"_json;
        auto bnoh = json.get<BlockNumberOrHash>();

        CHECK(bnoh.is_hash() == false);
        CHECK(bnoh.is_number() == true);
        CHECK(bnoh.is_tag() == false);
        CHECK(bnoh.number() == 123456);
    }
}

TEST_CASE("serialize zero forks", "[silkworm::json][to_json]") {
    silkworm::rpc::ChainConfig cc{
        0x0000000000000000000000000000000000000000000000000000000000000000_bytes32,
        R"({"chainId":1,"ethash":{}})"_json};
    silkworm::rpc::Forks f{cc};
    nlohmann::json j = f;
    CHECK(j == R"({
        "genesis":"0x0000000000000000000000000000000000000000000000000000000000000000",
        "heightForks":[],
        "timeForks":[]
    })"_json);
}

TEST_CASE("serialize forks", "[silkworm::json][to_json]") {
    silkworm::rpc::ChainConfig cc{
        0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c_bytes32,
        R"({
            "berlinBlock":12244000,
            "byzantiumBlock":4370000,
            "chainId":1,
            "constantinopleBlock":7280000,
            "daoForkBlock":1920000,
            "eip150Block":2463000,
            "eip155Block":2675000,
            "ethash":{},
            "homesteadBlock":1150000,
            "istanbulBlock":9069000,
            "londonBlock":12965000,
            "muirGlacierBlock":9200000,
            "petersburgBlock":7280000,
            "shanghaiTime":1678832736
        })"_json};
    silkworm::rpc::Forks f{cc};
    nlohmann::json j = f;
    CHECK(j == R"({
        "genesis":"0x374f3a049e006f36f6cf91b02a3b0ee16c858af2f75858733eb0e927b5b7126c",
        "heightForks":[1150000,1920000,2463000,2675000,4370000,7280000,9069000,9200000,
  12244000,12965000],
        "timeForks":[1678832736]
    })"_json);
}

TEST_CASE("serialize empty issuance", "[silkworm::json][to_json]") {
    silkworm::rpc::Issuance issuance{};
    nlohmann::json j = issuance;
    CHECK(j == R"({
        "blockReward":null,
        "uncleReward":null,
        "issuance":null,
        "burnt":null,
        "tips":null,
        "totalBurnt":null,
        "totalIssued":null
    })"_json);
}

TEST_CASE("serialize chain_traffic", "[silkworm::json][to_json]") {
    silkworm::rpc::ChainTraffic chain_traffic{4, 5};
    nlohmann::json j = chain_traffic;
    CHECK(j == R"({
        "cumulativeGasUsed":"0x4",
        "cumulativeTransactionsCount":"0x5"
    })"_json);
}

TEST_CASE("serialize NodeInfoPorts", "[silkworm::json][to_json]") {
    silkworm::rpc::NodeInfoPorts ports{6, 7};
    nlohmann::json j = ports;
    CHECK(j == R"({
        "discovery":6,
        "listener":7
    })"_json);
}

TEST_CASE("serialize NodeInfo", "[silkworm::json][to_json]") {
    silkworm::rpc::NodeInfo node_info{"340", "erigon", "enode", "enr", "[::]:30303", R"({"eth": {"network":5, "difficulty":10790000}})"};
    nlohmann::json j = node_info;
    CHECK(j == R"( {
              "enode":"enode",
              "enr":"enr",
              "id":"340",
              "ip":"enode",
              "listenAddr":"[::]:30303",
              "name":"erigon",
              "ports":{"discovery":0,"listener":0},
              "protocols":  { "eth":  {"network":5, "difficulty":10790000}}
    })"_json);
}

TEST_CASE("serialize issuance", "[silkworm::json][to_json]") {
    silkworm::rpc::Issuance issuance{
        "0x0",
        "0x0",
        "0x0",
        "0x0",
        "0x0",
        "0x0",
        "0x0"};
    nlohmann::json j = issuance;
    CHECK(j == R"({
        "blockReward":"0x0",
        "uncleReward":"0x0",
        "issuance":"0x0",
        "burnt":"0x0",
        "tips":"0x0",
        "totalBurnt":"0x0",
        "totalIssued":"0x0"
    })"_json);
}

TEST_CASE("serialize forkchoice updated reply", "[silkworm::json][to_json]") {
    silkworm::rpc::PayloadStatus payload_status{
        .status = "VALID",
        .latest_valid_hash = 0x0000000000000000000000000000000000000000000000000000000000000040_bytes32,
        .validation_error = "some error"};
    silkworm::rpc::ForkChoiceUpdatedReply forkchoice_update_reply{
        .payload_status = payload_status,
        .payload_id = 0x1};

    nlohmann::json j = forkchoice_update_reply;
    CHECK(j == R"({
        "payloadStatus": {
            "status":"VALID",
            "latestValidHash":"0x0000000000000000000000000000000000000000000000000000000000000040",
            "validationError":"some error"
        },
        "payloadId":"0x1"
    })"_json);
}

TEST_CASE("serialize payload status", "[silkworm::json][to_json]") {
    silkworm::rpc::PayloadStatus payload_status{
        .status = "VALID",
        .latest_valid_hash = 0x0000000000000000000000000000000000000000000000000000000000000040_bytes32,
        .validation_error = "some error"};
    nlohmann::json j = payload_status;
    CHECK(j == R"({
        "status":"VALID",
        "latestValidHash":"0x0000000000000000000000000000000000000000000000000000000000000040",
        "validationError":"some error"
    })"_json);
}

TEST_CASE("make empty json content", "[silkworm::json][make_json_content]") {
    const auto j = make_json_content(0, {});
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":0,
        "result":null
    })"_json);
}

TEST_CASE("make json content", "[silkworm::json][make_json_content]") {
    nlohmann::json json_result = {{"currency", "ETH"}, {"value", 4.2}};
    const auto j = make_json_content(123, json_result);
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":123,
        "result":{"currency":"ETH","value":4.2}
    })"_json);
}

TEST_CASE("make empty json error", "[silkworm::json][make_json_error]") {
    const auto j = make_json_error(0, 0, "");
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":0,
        "error":{"code":0,"message":""}
    })"_json);
}

TEST_CASE("make glaze json error", "[make_glaze_json_error]") {
    std::string json;
    make_glaze_json_error(json, 1, 3, "generic_error");
    CHECK(strcmp(json.c_str(),
                 "{\"jsonrpc\":\"2.0\",\
                  \"id\":1,\
                   \"error\":{\"code\":3,\"message\":\"generic_error\"}}"));
}

TEST_CASE("make glaze json error (Revert)", "[make_glaze_json_error]") {
    std::string json;
    const char* data_hex{"c68341b58302c0"};
    silkworm::Bytes data_bytes{*silkworm::from_hex(data_hex)};
    make_glaze_json_error(json, 1, RevertError{{3, "generic_error"}, data_bytes});
    CHECK(strcmp(json.c_str(),
                 "{\"jsonrpc\":\"2.0\",\
                  \"id\":1,\
                   \"error\":{\"code\":3,\"message\":\"generic_error\",\"data\": \"0xc68341b58302c0\"}}"));
}

TEST_CASE("make glaze content (data)", "[make_glaze_json_error]") {
    std::string json;
    const char* data_hex{"c68341b58302d066"};
    silkworm::Bytes data_bytes{*silkworm::from_hex(data_hex)};
    make_glaze_json_content(json, 1, data_bytes);
    CHECK(strcmp(json.c_str(),
                 "{\"jsonrpc\":\"2.0\",\
                  \"id\":1,\
                   \"result\":\"0xc68341b58302d066\"}"));
}

TEST_CASE("make empty json revert error", "[silkworm::json][make_json_error]") {
    const auto j = make_json_error(0, {{0, ""}, silkworm::Bytes{}});
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":0,
        "error":{"code":0,"message":"","data":"0x"}
    })"_json);
}

TEST_CASE("make json error", "[silkworm::json][make_json_error]") {
    const auto j = make_json_error(123, -32000, "revert");
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":123,
        "error":{"code":-32000,"message":"revert"}
    })"_json);
}

TEST_CASE("make json revert error", "[silkworm::json][make_json_error]") {
    const auto j = make_json_error(123, {{3, "execution reverted: Ownable: caller is not the owner"}, *silkworm::from_hex("0x00010203")});
    CHECK(j == R"({
        "jsonrpc":"2.0",
        "id":123,
        "error":{"code":3,"message":"execution reverted: Ownable: caller is not the owner","data":"0x00010203"}
    })"_json);
}

}  // namespace silkworm::rpc
