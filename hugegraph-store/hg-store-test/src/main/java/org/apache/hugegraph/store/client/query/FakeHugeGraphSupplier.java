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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;

public class FakeHugeGraphSupplier implements HugeGraphSupplier {

    private final Map<Id, PropertyKey> propertyKeyMap;

    private Map<Id, VertexLabel> vertexMap;

    private final Map<Id, EdgeLabel> edgeMap;

    private Map<Id, IndexLabel> indexMap;

    public FakeHugeGraphSupplier(Map<Id, PropertyKey> propertyKeyMap,
                                 Map<Id, VertexLabel> vertexMap,
                                 Map<Id, EdgeLabel> edgeMap, Map<Id, IndexLabel> indexMap) {
        this.propertyKeyMap = propertyKeyMap;
        this.vertexMap = vertexMap;
        this.edgeMap = edgeMap;
        this.indexMap = indexMap;
    }

    public void setVertexMap(Map<Id, VertexLabel> vertexMap) {
        this.vertexMap = vertexMap;
    }

    public void setIndexMap(Map<Id, IndexLabel> indexMap) {
        this.indexMap = indexMap;
    }

    @Override
    public List<String> mapPkId2Name(Collection<Id> collection) {
        var result = new ArrayList<String>();
        for (var id : collection) {
            result.add(propertyKeyMap.get(id).name());
        }
        return result;
    }

    @Override
    public List<String> mapIlId2Name(Collection<Id> collection) {
        var result = new ArrayList<String>();
        for (var id : collection) {
            result.add(indexMap.get(id).name());
        }
        return result;
    }

    @Override
    public PropertyKey propertyKey(Id id) {
        return propertyKeyMap.get(id);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        return propertyKeyMap.values();
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id id) {
        return vertexMap.get(id);
    }

    @Override
    public boolean existsLinkLabel(Id id) {
        return edgeMap.containsKey(id);
    }

    @Override
    public VertexLabel vertexLabel(Id id) {
        return vertexMap.get(id);
    }

    @Override
    public VertexLabel vertexLabel(String s) {

        Id id = null;
        for (var entry : propertyKeyMap.entrySet()) {
            if (entry.getValue().name().equals(s)) {
                id = entry.getKey();
            }
        }
        return id != null ? vertexLabel(id) : null;
    }

    @Override
    public EdgeLabel edgeLabel(Id id) {
        return edgeMap.get(id);
    }

    @Override
    public EdgeLabel edgeLabel(String s) {
        Id id = null;
        for (var entry : propertyKeyMap.entrySet()) {
            if (entry.getValue().name().equals(s)) {
                id = entry.getKey();
            }
        }
        return id != null ? edgeLabel(id) : null;
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        return indexMap.get(id);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return indexMap.entrySet().stream().map(e -> e.getValue()).collect(Collectors.toList());
    }

    @Override
    public String name() {
        return "FakeHugeGraphSupplier";
    }

    @Override
    public HugeConfig configuration() {
        return new HugeConfig(new HashMap<>());
    }
}
