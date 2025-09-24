/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.client.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.client.HgNodePartition;
import org.apache.hugegraph.store.client.HgNodePartitionerBuilder;
import org.apache.hugegraph.store.client.HgStoreNodePartitioner;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;

public class FakeNodePartitioner implements HgStoreNodePartitioner {

    private final FakePdServiceProvider pdProvider;

    public FakeNodePartitioner(FakePdServiceProvider pdProvider) {
        this.pdProvider = pdProvider;
    }

    @Override
    public int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey,
                         byte[] endKey) {
        HashSet<HgNodePartition> partitions = null;
        if (HgStoreClientConst.ALL_PARTITION_OWNER == startKey) {
            List<Store> stores = pdProvider.getStores();
            partitions = new HashSet<>(stores.size());
            for (var store : stores) {
                partitions.add(HgNodePartition.of(store.getId(), -1));
            }

        } else if (endKey == HgStoreClientConst.EMPTY_BYTES
                   || startKey == endKey || Arrays.equals(startKey, endKey)) {

            var partition =
                    pdProvider.getPartitionByCode(graphName, PartitionUtils.calcHashcode(startKey));

            var shardGroup = pdProvider.getShardGroup(partition.getId());
            Long storeId = null;
            for (var shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    storeId = shard.getStoreId();
                    break;
                }
            }

            partitions = new HashSet<>();
            partitions.add(HgNodePartition.of(pdProvider.getStoreByID(storeId).getId(),
                                              PartitionUtils.calcHashcode(startKey)));
        } else {
            List<Store> stores = pdProvider.getStores();
            partitions = new HashSet<>(stores.size());
            for (var store : stores) {
                partitions.add(HgNodePartition.of(store.getId(), -1));
            }
        }
        builder.setPartitions(partitions);
        return 0;
    }

    public String partition(String graphName, byte[] startKey) throws PDException {
        var partition =
                pdProvider.getPartitionByCode(graphName, PartitionUtils.calcHashcode(startKey));
        var shardGroup = pdProvider.getShardGroup(partition.getId());
        Long storeId = null;
        for (var shard : shardGroup.getShardsList()) {
            if (shard.getRole() == Metapb.ShardRole.Leader) {
                storeId = shard.getStoreId();
                break;
            }
        }

        return pdProvider.getStoreByID(storeId).getStoreAddress();
    }

    public String partition(String graphName, int code) throws PDException {
        return pdProvider.getStores().get(0).getStoreAddress();
    }

    public List<String> getStores(String graphName) {
        return pdProvider.getStores().stream().map(Store::getStoreAddress)
                         .collect(Collectors.toList());
    }

}
