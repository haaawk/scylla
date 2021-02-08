/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <vector>

#include "cdc/generation.hh"

namespace cdc {

size_t limit_of_streams_in_topology_description();
topology_description limit_number_of_streams_if_needed(topology_description&& desc);

} // namespace cdc

static cdc::topology_description create_description(const std::vector<size_t>& streams_count_per_vnode) {
    std::vector<cdc::token_range_description> result;
    result.reserve(streams_count_per_vnode.size());
    size_t vnode_index = 0;
    int64_t token = std::numeric_limits<int64_t>::min();
    for (size_t streams_count : streams_count_per_vnode) {
        std::vector<cdc::stream_id> streams(streams_count);
        for (size_t idx = 0; idx < streams_count; ++idx) {
            streams[idx] = cdc::stream_id{dht::token::from_int64(token), vnode_index};
            ++token;
        }
        // Niether token_range_end nor sharding_ignore_msb should matter for
        // limit_number_of_streams_if_needed so we're using minimum token
        // and sharding_ignore_msb equal to 0.
        result.push_back(cdc::token_range_description{dht::minimum_token(), std::move(streams), uint8_t{0}});
        ++vnode_index;
    }
    return cdc::topology_description(std::move(result));
}

static void assert_streams_count(const cdc::topology_description& desc, const std::vector<size_t>& expected_count) {
    BOOST_REQUIRE_EQUAL(expected_count.size(), desc.entries().size());

    for (size_t idx = 0; idx < expected_count.size(); ++idx) {
        BOOST_REQUIRE_EQUAL(expected_count[idx], desc.entries()[idx].streams.size());
    }
}

BOOST_AUTO_TEST_CASE(test_cdc_generation_limitting_single_vnode_should_not_limit) {
    cdc::topology_description given = create_description({cdc::limit_of_streams_in_topology_description()});

    cdc::topology_description result = cdc::limit_number_of_streams_if_needed(std::move(given));

    assert_streams_count(result, {cdc::limit_of_streams_in_topology_description()});
}

BOOST_AUTO_TEST_CASE(test_cdc_generation_limitting_single_vnode_should_limit) {
    cdc::topology_description given = create_description({cdc::limit_of_streams_in_topology_description() + 1});

    cdc::topology_description result = cdc::limit_number_of_streams_if_needed(std::move(given));

    assert_streams_count(result, {cdc::limit_of_streams_in_topology_description()});
}

BOOST_AUTO_TEST_CASE(test_cdc_generation_limitting_multiple_vnodes_should_not_limit) {
    size_t total = 0;
    std::vector<size_t> streams_count_per_vnode;
    size_t count_for_next_vnode = 1;
    while (total + count_for_next_vnode <= cdc::limit_of_streams_in_topology_description()) {
        streams_count_per_vnode.push_back(count_for_next_vnode);
        total += count_for_next_vnode;
        ++count_for_next_vnode;
    }
    cdc::topology_description given = create_description(streams_count_per_vnode);

    cdc::topology_description result = cdc::limit_number_of_streams_if_needed(std::move(given));

    assert_streams_count(result, streams_count_per_vnode);
}

BOOST_AUTO_TEST_CASE(test_cdc_generation_limitting_multiple_vnodes_should_limit) {
    size_t total = 0;
    std::vector<size_t> streams_count_per_vnode;
    size_t count_for_next_vnode = 1;
    while (total + count_for_next_vnode <= cdc::limit_of_streams_in_topology_description()) {
        streams_count_per_vnode.push_back(count_for_next_vnode);
        total += count_for_next_vnode;
        ++count_for_next_vnode;
    }
    streams_count_per_vnode.push_back(cdc::limit_of_streams_in_topology_description() - total + 1);
    cdc::topology_description given = create_description(streams_count_per_vnode);

    cdc::topology_description result = cdc::limit_number_of_streams_if_needed(std::move(given));

    assert(streams_count_per_vnode.size() <= cdc::limit_of_streams_in_topology_description());
    size_t per_vnode_limit = cdc::limit_of_streams_in_topology_description() / streams_count_per_vnode.size();
    for (auto& count : streams_count_per_vnode) {
        count = std::min(count, per_vnode_limit);
    }

    assert_streams_count(result, streams_count_per_vnode);
}

