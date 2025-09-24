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

package org.apache.hugegraph.store.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.util.MultiKv;
import org.apache.hugegraph.structure.BaseVertex;
import org.junit.Test;

public class SortShuffleSerializerTest {

    @Test
    public void testMkv() throws IOException {
        SortShuffleSerializer<MultiKv> serializer = SortShuffleSerializer.ofMultiKvSerializer();
        List<Object> key = new ArrayList<>();
        key.add(1);
        key.add(1L);
        key.add("abc");
        key.add(1.0);
        key.add(1.0f);
        key.add(BigDecimal.ONE);

        List<Object> value = new ArrayList<>();
        value.add(BigDecimal.TEN);
        value.add("abc");
        value.add(null);

        MultiKv kv;

        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        serializer.write(bs, MultiKv.of(key, value));
        serializer.write(bs, MultiKv.of(key, null));
        serializer.write(bs, MultiKv.of(value, null));
        serializer.write(bs, MultiKv.of(null, null));
        serializer.write(bs, MultiKv.of(new ArrayList<>(), null));

        ByteArrayInputStream bis = new ByteArrayInputStream(bs.toByteArray());
        while ((kv = serializer.read(bis)) != null) {
            System.out.println(kv);
        }
    }

    @Test
    public void testBackendColumn() throws IOException {
        var serializer = SortShuffleSerializer.ofBackendColumnSerializer();

        ByteArrayOutputStream bs = new ByteArrayOutputStream();

        for (int i = 0; i < 100; i++) {
            serializer.write(bs, RocksDBSession.BackendColumn.of(("key" + i).getBytes(),
                                                                 ("value" + i).getBytes()));
        }

        serializer.write(bs, RocksDBSession.BackendColumn.of(null, null));

        ByteArrayInputStream bis = new ByteArrayInputStream(bs.toByteArray());
        RocksDBSession.BackendColumn column;
        while ((column = serializer.read(bis)) != null) {
            System.out.println(column.name + "-->" + column.value);
        }
    }

    @Test
    public void testElement() throws IOException {
        var serializer = SortShuffleSerializer.ofBaseElementSerializer();
        var name = PropertyKey.fromMap(Map.of("id", 1, "name", "name", "data_type", "text"), null);
        var vertexLabel = new VertexLabel(null, IdGenerator.of(1), "person_vertex");
        ByteArrayOutputStream bs = new ByteArrayOutputStream();

        for (int i = 0; i < 100; i++) {
            var vertex = new BaseVertex(IdGenerator.of(Long.valueOf(i)));
            vertex.addProperty(name, "name" + i);
            vertex.schemaLabel(vertexLabel);
            serializer.write(bs, vertex);
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bs.toByteArray());
        BaseVertex vertex;
        while ((vertex = (BaseVertex) serializer.read(bis)) != null) {
            printItem(vertex);
        }
    }

    private void printItem(BaseVertex item) {
        System.out.print(item.id().asString() + "::");
        for (var p : item.properties()) {
            System.out.print(p.propertyKey() + " --> " + p.value() + " ** ");
        }
        System.out.println(item);
    }

}
