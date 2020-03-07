/*
 * Copyright (C) 2020 ScyllaDB
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

#include "cdc_partitioner.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include "cdc/generation.hh"

namespace dht {

token
cdc_partitioner::get_token(bytes key) const {
    if (key.empty()) {
        return minimum_token();
    }
    cdc::stream_id id(std::move(key));
    return get_token(id.first());
}

token
cdc_partitioner::get_token(int64_t value) const {
    return token(token::kind::key, value);
}

token
cdc_partitioner::get_token(const sstables::key_view& key) const {
    return get_token(bytes(bytes_view(key)));
}

token
cdc_partitioner::get_token(const schema& s, partition_key_view key) const {
    return get_token(std::move(key.explode(s)[0]));
}

using registry = class_registrator<i_partitioner, cdc_partitioner>;
static registry registrator("com.scylladb.dht.CDCPartitioner");
static registry registrator_short_name("CDCPartitioner");

}
