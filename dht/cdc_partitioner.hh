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

#pragma once

#include "i_partitioner.hh"

namespace dht {

class cdc_partitioner final : public i_partitioner {
public:
    cdc_partitioner() = default;
    virtual const sstring name() const { return "com.scylladb.dht.CDCPartitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
    virtual bool preserves_order() const override { return false; }
private:
    token get_token(bytes key) const;
    token get_token(int64_t value) const;
};


}
