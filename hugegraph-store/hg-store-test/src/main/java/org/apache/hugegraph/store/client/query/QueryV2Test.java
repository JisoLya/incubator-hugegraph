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

import static org.apache.hugegraph.testutil.Assert.assertEquals;
import static org.apache.hugegraph.type.HugeType.VERTEX;
import static org.apache.hugegraph.type.HugeType.VERTEX_LABEL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.query.Condition;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.serializer.BytesBuffer;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.client.query.QueryV2Client;
import org.apache.hugegraph.store.core.StoreEngineTestBase;
import org.apache.hugegraph.store.node.grpc.query.AggregativeQueryService;
import org.apache.hugegraph.store.query.PropertyList;
import org.apache.hugegraph.store.query.QueryTypeParam;
import org.apache.hugegraph.store.query.StoreQueryParam;
import org.apache.hugegraph.store.query.StoreQueryType;
import org.apache.hugegraph.store.query.func.AggregationFunctionParam;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.structure.KvElement;
import org.apache.hugegraph.type.define.IndexType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class QueryV2Test extends StoreEngineTestBase {

    private final BinaryElementSerializer serializer = BinaryElementSerializer.getInstance();
    private static final FakeIdGenerator ID_GENERATOR = new FakeIdGenerator();
    private static final String TABLE = "g+v";
    private static final String OLAP_TABLE = "g+olap";

    private static final String INDEX_TABLE = "g+index";

    private final PropertyKey name =
            PropertyKey.fromMap(Map.of("id", 1, "name", "name", "data_type", "text"), null);
    private final PropertyKey age =
            PropertyKey.fromMap(Map.of("id", 2, "name", "age", "data_type", "int"), null);
    private final PropertyKey city =
            PropertyKey.fromMap(Map.of("id", 3, "name", "city", "data_type", "text"), null);
    private final PropertyKey score =
            PropertyKey.fromMap(Map.of("id", 4, "name", "score", "data_type", "double"), null);
    private final PropertyKey gender =
            PropertyKey.fromMap(Map.of("id", 5, "name", "gender", "data_type", "text"), null);

    private final PropertyKey interest =
            PropertyKey.fromMap(Map.of("id", 6, "name", "interest", "data_type", "text"), null);

    private final PropertyKey lang =
            PropertyKey.fromMap(Map.of("id", 7, "name", "lang", "data_type", "text"), null);

    private static FakeHugeGraphSupplier graphSupplier;

    private static VertexLabel vertexLabel;
    private static IndexLabel ageIndexLabel;
    private static IndexLabel cityIndexLabel;
    private static IndexLabel genderIndexLabel;

    private static final Random RANDOM = new Random();

    private static boolean isDataInitialed = false;

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private ManagedChannel channel;

    private Server server;

    private static int countOfCityBeijing = 0;
    private static int countOfCityShanghai = 0;

    private static int countOfAge20 = 0;
    private static int countOfBeijingM = 0;
    private static int countOfNanjingF = 0;

    private BusinessHandler handler;

    private static final List<Integer> AGES = new ArrayList<>();

    private static final Set<Integer> indexCounter = new HashSet<>();

    @Before
    public void init() throws IOException {
        prepareData();

        var name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                                       .directExecutor()
                                       .addService(new AggregativeQueryService())
                                       .build()
                                       .start();
        grpcCleanup.register(server);
        channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(name).directExecutor().build());
        BusinessHandlerImpl.setMockGraphSupplier(graphSupplier);
        // AggregativeQueryObserver.setTestMode();
        QueryV2Client.setTestChannel(channel);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.shutdown();
        channel.shutdown();
        // fail the test if cannot gracefully shut down
        try {
            server.awaitTermination(5, TimeUnit.SECONDS);
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            server.shutdownNow();
            channel.shutdownNow();
        }
    }

    private static String getCity() {
        List<String> cities =
                List.of("Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Wuhan", "NanJing");
        var city = cities.get(RANDOM.nextInt(6));
        if ("Beijing".equals(city)) {
            countOfCityBeijing++;
        } else if ("Shanghai".equals(city)) {
            countOfCityShanghai++;
        }
        return city;
    }

    private static String getInterest() {
        List<String> interest = List.of("Football", "Basketball", "PingBang", "Ski", "Ride");
        return interest.get(RANDOM.nextInt(5));
    }

    private static String getLang() {
        List<String> languages = List.of("JAVA", "Python", "C++", "PHP");
        return languages.get(RANDOM.nextInt(4));
    }

    private static int getAge() {
        return 10 + RANDOM.nextInt(60);
    }

    private static Double getScore() {
        return Math.log(RANDOM.nextDouble());
    }

    private BaseVertex createVertex(int i) {
        var vertex = new BaseVertex(IdGenerator.of(Long.valueOf(i)));
        vertex.addProperty(name, "person_" + i);
        var ageN = getAge();
        AGES.add(ageN);
        vertex.addProperty(age, ageN);
        var cityV = getCity();
        vertex.addProperty(city, cityV);
        vertex.addProperty(score, getScore());

        var genderV = RANDOM.nextInt(10) % 2 == 0 ? "M" : "F";
        vertex.addProperty(gender, genderV);

        if ("Beijing".equals(cityV) && "M".equals(genderV) ||
            "NanJing".equals(cityV) && "F".equals(genderV)) {
            if ("Beijing".equals(cityV) && "M".equals(genderV)) {
                countOfBeijingM++;
            } else {
                countOfNanjingF++;
            }
            indexCounter.add(i);
        }

        if (ageN == 20 || i >= 100 && i <= 199) {
            if (ageN == 20) {
                countOfAge20++;
            }
            indexCounter.add(i);
        }

        vertex.schemaLabel(vertexLabel);
        return vertex;
    }

    private BaseVertex createVertexOlapLang(int i) {
        var vertex = new BaseVertex(IdGenerator.of(Long.valueOf(i)));
        vertex.addProperty(lang, getLang());
        return vertex;
    }

    private BaseVertex createVertexOlapInterest(int i) {
        var vertex = new BaseVertex(IdGenerator.of(Long.valueOf(i)));
        vertex.addProperty(interest, getInterest());
        return vertex;
    }

    private int writeVertex(BaseVertex vertex, boolean isOlap, int code) {
        BackendColumn col;
        if (isOlap) {
            col = serializer.writeOlapVertex(vertex);
        } else {
            col = serializer.writeVertex(vertex);
        }

        if (code == -1) {
            code = PartitionUtils.calcHashcode(BinaryElementSerializer.ownerId(vertex).asBytes());
        }
        this.handler.doPut(DEFAULT_GRAPH, code, isOlap ? OLAP_TABLE : TABLE, col.name, col.value);
        // System.out.println(i + ": code " + code + " name " + Arrays.toString(col.name));
        return code;
    }

    private void writeIndex(BaseVertex vertex, int code) {

        var indexAge = new Index(graphSupplier, ageIndexLabel);
        indexAge.fieldValues(vertex.getPropertyValue(age.id()));
        var indexCity = new Index(graphSupplier, cityIndexLabel);
        indexCity.fieldValues(vertex.getPropertyValue(city.id()));
        var indexGender = new Index(graphSupplier, genderIndexLabel);
        indexGender.fieldValues(vertex.getPropertyValue(gender.id()));

        List.of(indexAge, indexCity, indexGender).forEach(
                index -> {
                    index.elementIds(vertex.id());
                    var col = serializer.writeIndex(index);
                    if (col.value == null) {
                        col.value = new byte[0];
                    }
                    this.handler.doPut(DEFAULT_GRAPH, code, INDEX_TABLE, col.name, col.value);
                }
        );
    }

    private void prepareData() {

        if (isDataInitialed) {
            return;
        }

        // create partition
        pdProvider.setPartitionCount(4);

        handler = getStoreEngine().getBusinessHandler();

        createPartitionEngine(0);
        createPartitionEngine(1);
        createPartitionEngine(2);
        createPartitionEngine(3);

        // initial schema
        var properties = List.of(name, age, city, score, gender, interest, lang);
        var propertyMap = properties.stream().collect(Collectors.toMap(p -> p.id(), p -> p));

        graphSupplier = new FakeHugeGraphSupplier(propertyMap, Map.of(), Map.of(), Map.of());

        vertexLabel = new VertexLabel(graphSupplier, IdGenerator.of(1), "person_vertex");
        var vertexMap =
                properties.stream().collect(Collectors.toMap(SchemaElement::id, p -> vertexLabel));
        graphSupplier.setVertexMap(vertexMap);

        ageIndexLabel = new IndexLabel(graphSupplier, IdGenerator.of(2), "person_by_age");
        ageIndexLabel.indexField(age.id());
        ageIndexLabel.indexType(IndexType.RANGE_INT);
        cityIndexLabel = new IndexLabel(graphSupplier, IdGenerator.of(3), "person_by_age");
        cityIndexLabel.indexField(city.id());
        cityIndexLabel.indexType(IndexType.SECONDARY);
        genderIndexLabel = new IndexLabel(graphSupplier, IdGenerator.of(4), "person_by_age");
        genderIndexLabel.indexField(gender.id());
        genderIndexLabel.indexType(IndexType.SECONDARY);

        var indexes = List.of(ageIndexLabel, cityIndexLabel, genderIndexLabel);
        indexes.forEach(index -> {
            index.baseType(VERTEX_LABEL);
            index.baseValue(vertexLabel.id());
        });

        vertexLabel.indexLabels(ageIndexLabel.id(), cityIndexLabel.id(), genderIndexLabel.id());

        var indexMap = indexes.stream().collect(Collectors.toMap(IndexLabel::id, t -> t));
        graphSupplier.setIndexMap(indexMap);

        // insert data
        for (int i = 0; i < 1000; i++) {
            var vertex = createVertex(i);
            var code = writeVertex(vertex, false, -1);
            writeVertex(createVertexOlapLang(i), true, code);
            writeVertex(createVertexOlapInterest(i), true, code);
            writeIndex(vertex, code);
        }

        isDataInitialed = true;
    }

    private HgStoreSession getSession() {
        var provider = new FakeSessionProvider(pdProvider);
        return provider.getSession(DEFAULT_GRAPH);
    }

    /**
     * scan all
     */
    @Test
    public void testQueryAll() throws PDException {
        StoreQueryParam param = getDefaultParam();

        var iterators = getSession().query(param, null);

        int count = 0;
        for (var itr : iterators) {
            while (itr.hasNext()) {
                var item = (BaseVertex) itr.next();
                // printItem(item);
                count += 1;
            }
            itr.close();
        }

        assertEquals(1000, count);
    }

    /**
     * count test
     */
    @Test
    public void testAgg() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setFuncList(List.of(AggregationFunctionParam.ofCount(),
                                  AggregationFunctionParam.ofSum(
                                          AggregationFunctionParam.FiledType.INTEGER, age.id()),
                                  AggregationFunctionParam.ofAvg(
                                          AggregationFunctionParam.FiledType.DOUBLE, score.id()),
                                  AggregationFunctionParam.ofMax(
                                          AggregationFunctionParam.FiledType.STRING, city.id()),
                                  AggregationFunctionParam.ofMin(
                                          AggregationFunctionParam.FiledType.STRING, city.id())));

        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (KvElement) iterator.next();
                System.out.println(item.getValues());
                assertEquals(1000L, item.getValues().get(0));
            }
        }
    }

    /**
     * 测试简单的聚合函数COUNT和COUNT
     *
     * @throws PDException 如果查询过程中出现PD异常则抛出该异常
     */
    @Test
    public void testAggSimpleCount() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setFuncList(
                List.of(AggregationFunctionParam.ofCount(), AggregationFunctionParam.ofCount()));

        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (KvElement) iterator.next();
                System.out.println(item.getValues());
                assertEquals(1000L, item.getValues().get(0));
            }
        }
    }

    public void testNullIdQuery() throws IllegalArgumentException, PDException {
        StoreQueryParam param = getDefaultParam();
        param.setQueryType(StoreQueryType.PRIMARY_SCAN);
        List<QueryTypeParam> params = new ArrayList<>();
//        params.add(QueryTypeParam.ofIdScanParam(idToBytes(200900L)));
//        params.add(QueryTypeParam.ofIdScanParam(idToBytes(200000L)));
//        params.add(QueryTypeParam.ofIdScanParam(idToBytes(2)));
        param.setQueryParam(params);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);

        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                printItem(item);
            }
        }
    }

    @Test
    public void testSinglePrimaryScan() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setProperties(PropertyList.of(List.of(age.id(), city.id())));
        param.setQueryType(StoreQueryType.PRIMARY_SCAN);
        param.setQueryParam(
                List.of(QueryTypeParam.ofRangeScanParam(idToBytes(256L), idToBytes(266L),
                                                        HgKvStore.SCAN_GTE_BEGIN |
                                                        HgKvStore.SCAN_LT_END)));
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);
        int count = 0;
        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                printItem(item);
                count += 1;
            }
        }

        assertEquals(10, count);
    }

    @Test
    public void testScanAll() throws PDException {
        StoreQueryParam param = getDefaultParam();
        var iterators = getSession().query(param, null);

        int n = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                n += 1;
                if (n > 10) {
                    break;
                }
                printItem(item);
            }
            iterator.close();
        }
    }

    @Test
    public void testScanAllWithLimit() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setLimit(10);
        var iterators = getSession().query(param, null);

        int n = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                n += 1;
                printItem(item);
            }
            iterator.close();
        }
        System.out.println(n);
    }

    @Test
    public void testScanWithNoRead() throws PDException, InterruptedException {
        StoreQueryParam param = getDefaultParam();
        var iterators = getSession().query(param, null);

        for (var iterator : iterators) {
            iterator.hasNext();
            Thread.sleep(1000);

            iterator.close();
        }

    }

    @Test
    public void testScanAllWithEarlyClose() throws PDException {
        StoreQueryParam param = getDefaultParam();
        var iterators = getSession().query(param, null);

        int n = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                n += 1;
                printItem(item);
                if (n > 15) {
                    break;
                }
            }
            iterator.close();
        }
        System.out.println(n);
    }

    /**
     * 多种查询 + 过滤
     */
    @Test
    public void testProjection() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setProperties(PropertyList.of(List.of(age.id(), city.id())));
        param.setQueryType(StoreQueryType.PRIMARY_SCAN);

        List<QueryTypeParam> params = new ArrayList<>();
        // 0 - 255
        params.add(QueryTypeParam.ofPrefixScanParam(new byte[]{8}, 0));
        // 256 ~ 265
        params.add(QueryTypeParam.ofRangeScanParam(idToBytes(256L), idToBytes(266L),
                                                   HgKvStore.SCAN_GTE_BEGIN |
                                                   HgKvStore.SCAN_LT_END));
        // 两个id和prefix can重复
        params.add(QueryTypeParam.ofIdScanParam(idToBytes(1L)));
        params.add(QueryTypeParam.ofIdScanParam(idToBytes(2L)));
        param.setQueryParam(params);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.NONE);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.DEDUP);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);

        var iterators = getSession().query(param, null);
        BaseVertex vertex = null;
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                vertex = item;
                // printItem(item);
                count += 1;
            }
        }

        if (vertex != null) {
            assertEquals(2, vertex.getProperties().size());
        }
        assertEquals(266, count);

        System.out.println("returns: " + count);
    }

    @Test
    public void testFilterAndSort() throws PDException {
        StoreQueryParam param = getDefaultParam();

        param.setOrderBy(List.of(age.id(), score.id()));

        ConditionQuery conditionQuery = new ConditionQuery(VERTEX);
        conditionQuery.query(Condition.eq(city.id(), "Beijing"));
        param.setConditionQuery(conditionQuery);

        var iterators = getSession().query(param, null);
        int count = 0;

        for (var itr : iterators) {
            while (itr.hasNext()) {
                var item = (BaseVertex) itr.next();
                // printItem(item);
                count += 1;
            }
            itr.close();
        }

        System.out.println("returns: " + count);
        System.out.println("count:" + countOfCityBeijing);

        assertEquals(countOfCityBeijing, count);
    }

    @Test
    public void testSortAndLimit() throws PDException {
        StoreQueryParam param = getDefaultParam();
        param.setOrderBy(List.of(age.id(), score.id()));
        param.setSortOrder(StoreQueryParam.SORT_ORDER.DESC);
        param.setLimit(10);

        AGES.sort(Integer::compareTo);

        var iterators = getSession().query(param, null);
        int count = 0;

        for (var itr : iterators) {
            while (itr.hasNext()) {
                var item = (BaseVertex) itr.next();
                var retAge = item.getProperty(age.id()).value();
                assertEquals(AGES.get(AGES.size() - count - 1), retAge);
                // printItem(item);
                count += 1;
            }
            itr.close();
        }
        assertEquals(10, count);
    }

    @Test
    public void testAggWithSort() throws PDException {
        var param = getDefaultParam();
        param.setFuncList(List.of(AggregationFunctionParam.ofCount(),
                                  AggregationFunctionParam.ofAvg(
                                          AggregationFunctionParam.FiledType.DOUBLE, score.id())));
        param.setGroupBy(List.of(gender.id(), age.id()));
        param.setOrderBy(List.of(age.id(), gender.id()));

        var iterators = getSession().query(param, null);
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (KvElement) iterator.next();
                System.out.println(item.getKeys() + "-->" + item.getValues());
                count += 1;
            }
        }

        System.out.println("returns : " + count);
    }

    /**
     * 测试标签组。
     */
    @Test
    public void testLabelGroup() throws PDException {
        var param = getDefaultParam();
        param.setFuncList(List.of(
                // AggregationFunctionParam.ofAvg(AggregationFunctionParam.FiledType.DOUBLE,
                // score.id()),
                AggregationFunctionParam.ofCount()));
        // param.setGroupBy(List.of());
        param.setGroupBySchemaLabel(true);

        var iterators = getSession().query(param, null);
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (KvElement) iterator.next();
                System.out.println(item.getKeys() + "-->" + item.getValues());
                count += 1;
            }
        }

        System.out.println("returns : " + count);
    }

    /**
     * 测试：查询并取样并限制返回数量
     *
     * @throws PDException PDF异常
     */
    @Test
    public void testSampleAndLimit() throws PDException {
        var param = getDefaultParam();
        param.setSampleFactor(0.8);
        param.setLimit(100);

        var iterators = getSession().query(param, null);
        int count = 0;
        for (var itr : iterators) {
            while (itr.hasNext()) {
                var item = (BaseVertex) itr.next();
                // printItem(item);
                count += 1;
            }
            // itr.close();
        }
        // 按照概率应该返回800个，远远大于100, 因为限制了100，超大概率还是相等的
        assertEquals(100, count);
    }

    @Test
    public void testSimpleStopPipeLine() throws PDException {
        var param = getDefaultParam();
        param.setSampleFactor(0.0);

        var iterators = getSession().query(param, null);
        int count = 0;
        for (var itr : iterators) {
            while (itr.hasNext()) {
                var item = (BaseVertex) itr.next();
                // printItem(item);
                count += 1;
            }
            // itr.close();
        }

        System.out.println(count);
        // 按照概率应该返回800个，远远大于100, 因为限制了100，超大概率还是相等的
        assertEquals(0, count);
    }

    @Test
    public void testOlap() throws PDException {
        var param = getDefaultParam();
        age.dataType();
        param.setOlapProperties(List.of(interest.id(), lang.id()));
        param.setProperties(PropertyList.of(List.of(name.id(), interest.id(), lang.id())));

        var iterators = getSession().query(param, null);
        BaseVertex vertex = null;
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                vertex = item;
                // printItem(item);
                count += 1;
            }
        }

        if (vertex != null) {
            assertEquals(3, vertex.getProperties().size());
        }
        assertEquals(1000, count);

        System.out.println("" + count);
    }

    @Test
    public void testOlapWithNoDeserialization() throws PDException {
        var param = getDefaultParam();
        age.dataType();
        param.setOlapProperties(List.of(interest.id(), lang.id()));

        var iterators = getSession().query(param, null);
        BaseVertex vertex = null;
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                vertex = item;
                // printItem(item);
                count += 1;
            }
        }

        if (vertex != null) {
            assertEquals(7, vertex.getProperties().size());
        }
        assertEquals(1000, count);

        System.out.println("returns" + count);
    }

    @Test
    public void testStrictOrderAndOnlyId() throws PDException {
        var param = getDefaultParam();
        param.setSortOrder(StoreQueryParam.SORT_ORDER.STRICT_ORDER);
        param.setQueryType(StoreQueryType.PRIMARY_SCAN);
        param.setQueryParam(List.of(QueryTypeParam.ofIdScanParam(idToBytes(-1L)),
                                    QueryTypeParam.ofIdScanParam(idToBytes(1L)),
                                    QueryTypeParam.ofIdScanParam(idToBytes(3L)),
                                    QueryTypeParam.ofIdScanParam(idToBytes(2L))
        ));

        param.setProperties(PropertyList.empty());
        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                printItem((BaseVertex) iterator.next());
            }
        }
    }

    /**
     * multi index + primary
     */
    @Test
    public void testIndex() throws PDException {
        var param = getDefaultParam();
        param.setQueryType(StoreQueryType.INDEX_SCAN);
        List<List<QueryTypeParam>> indexes = new ArrayList<>();

        // 北京 + M
        indexes.add(List.of(
                QueryTypeParam.ofIndexScanParam(
                        new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105, 110, 103},
                        HgKvStore.SCAN_PREFIX_BEGIN | HgKvStore.SCAN_LTE_END),
                QueryTypeParam.ofIndexScanParam(new byte[]{83, 73, 58, 52, 58, 77, 0},
                                                HgKvStore.SCAN_PREFIX_BEGIN | HgKvStore.SCAN_LTE_END
                )
        ));

        // nanjing + F
        indexes.add(List.of(QueryTypeParam.ofIndexScanParam(
                                    // new byte[] {83, 73, 58, 51, 58, 78, 97, 110, 106, 105,
                                    // 110, 103, 0, 0, 0},
                                    new byte[]{83, 73, 58, 51, 58, 78, 97, 110, 74, 105, 110, 103
                                            , 0, 0, 0},
                                    new byte[]{83, 73, 58, 51, 58, 78, 97, 110, 74, 105, 110, 103
                                            , 0, 127, 127},
                                    HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END),
                            QueryTypeParam.ofIndexScanParam(
                                    new byte[]{83, 73, 58, 52, 58, 70, 0, 0, 0},
                                    new byte[]{83, 73, 58, 52, 58, 70, 0, 127, 127},
                                    HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END
                            )));
        //age = 20
        indexes.add(List.of(QueryTypeParam.ofIndexScanParam(
                new byte[]{-96, 0, 0, 0, 2, -128, 0, 0, 20, 0, 0},
                new byte[]{-96, 0, 0, 0, 2, -128, 0, 0, 20, 127, 127},
                HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LTE_END)));

        // primary: 100 ~ 200
        indexes.add(List.of(QueryTypeParam.ofRangeScanParam(idToBytes(100L), idToBytes(199L),
                                                            HgKvStore.SCAN_GTE_BEGIN |
                                                            HgKvStore.SCAN_LTE_END)));

        param.setIndexes(indexes);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.DEDUP);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);

        System.out.println("index count:" + indexCounter.size());
        System.out.println("count of age 20: " + countOfAge20);
        System.out.println("count of Beijing M: " + countOfBeijingM);
        System.out.println("count of Nanjing F: " + countOfNanjingF);
        int count = 0;
        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                iterator.next();
                // printItem((BaseVertex) iterator.next());
                count++;
            }
        }

        System.out.println("ret count: " + count);
        assertEquals(indexCounter.size(), count);
    }

    // @Test
    public void testGetIndexPrefix() {
        for (int i = 10; i < 1000; i += 50) {
            var vertex = createVertex(i);
            vertex.addProperty(city, "Beijing");
            vertex.addProperty(gender, "M");
            vertex.addProperty(age, 20);

            var indexCity = new Index(graphSupplier, cityIndexLabel);
            indexCity.fieldValues(vertex.getPropertyValue(city.id()));
            indexCity.elementIds(vertex.id());
            var col = serializer.writeIndex(indexCity);
            // System.out.println("city" + Arrays.toString(col.name));

            var indexGender = new Index(graphSupplier, genderIndexLabel);
            indexGender.fieldValues(vertex.getPropertyValue(gender.id()));
            indexGender.elementIds(vertex.id());
            col = serializer.writeIndex(indexGender);
            System.out.println("gender" + Arrays.toString(col.name));

            var indexAge = new Index(graphSupplier, ageIndexLabel);
            indexAge.fieldValues(vertex.getPropertyValue(age.id()));
            indexAge.elementIds(vertex.id());
            col = serializer.writeIndex(indexAge);
            // System.out.println("age" + Arrays.toString(col.name));
        }
    }

    /**
     * index scan + limit
     *
     * @throws PDException
     */
    @Test
    public void testIndexWithLimit() throws PDException {
        var param = getDefaultParam();
        param.setQueryType(StoreQueryType.INDEX_SCAN);
        List<List<QueryTypeParam>> indexes = new ArrayList<>();

        //age = 20
        param.setIndexes(List.of(
                                 List.of(QueryTypeParam.ofIndexScanParam(
                                         new byte[]{-96, 0, 0, 0, 2, -128, 0, 0, 20, 0, 0},
                                         new byte[]{-96, 0, 0, 0, 2, -128, 0, 0, 20, 127, 127},
                                         HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LTE_END))
//                ,List.of(QueryTypeParam.ofIndexScanParam(
//                        new byte[] {-96, 0, 0, 0, 2, -128, 0, 0, 21, 0, 0},
//                        new byte[] {-96, 0, 0, 0, 2, -128, 0, 0, 21, 127, 127},
//                        HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LTE_END))
                         )
        );
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);
        param.setLimit(10);

        System.out.println("count of age 20: " + countOfAge20);
        int count = 0;
        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                iterator.next();
                // printItem((BaseVertex) iterator.next());
                count++;
            }
        }

        System.out.println("ret count: " + count);
        assertEquals(Math.min(countOfAge20, 10), count);
    }

    /**
     * index scan -> none
     */
    @Test
    public void testIndex2() throws PDException {
        var param = getDefaultParam();
        param.setQueryType(StoreQueryType.INDEX_SCAN);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.DEDUP);
        // Beijing +
        param.setIndexes(List.of(List.of(QueryTypeParam.ofIndexScanParam(
                                         new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105,
                                                    110, 103, 0, 0, 0},
                                         new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105,
                                                    110, 103, 0, 127, 127},
                                         HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END)),
                                 List.of(QueryTypeParam.ofIndexScanParam(
                                         new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104,
                                                    97, 105, 0, 0, 0},
                                         new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104,
                                                    97, 105, 0, 127, 127},
                                         HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END))));
        param.setFuncList(List.of(AggregationFunctionParam.ofCount()));
        var iterators = getSession().query(param, null);
        KvElement element = null;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                element = (KvElement) iterator.next();
            }
        }

        if (element != null) {
            Long ret = (Long) element.getValues().get(0);
            assertEquals(countOfCityBeijing + countOfCityShanghai, ret.intValue());
            System.out.println(
                    "beijing:" + countOfCityBeijing + ", shanghai:" + countOfCityShanghai);
        }
    }

    @Test
    public void testLimitDeduplication() throws PDException {
        var param = getDefaultParam();
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.LIMIT_DEDUP);

        // primary scan
        param.setQueryType(StoreQueryType.PRIMARY_SCAN);
        param.setQueryParam(List.of(
                QueryTypeParam.ofRangeScanParam(idToBytes(256L), idToBytes(266L),
                                                HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END),
                QueryTypeParam.ofRangeScanParam(idToBytes(256L), idToBytes(266L),
                                                HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END)
        ));

        var iterators = getSession().query(param, null);
        int count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var element = (BaseVertex) iterator.next();
                printItem(element);
                count += 1;
            }
        }

        assertEquals(10, count);

        // index scan
        param.setQueryType(StoreQueryType.INDEX_SCAN);
        // param.getQueryParam().clear();

        param.setIndexes(
                List.of(List.of(QueryTypeParam.ofIndexScanParam(
                                        new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105,
                                                   110, 103, 0, 0, 0},
                                        new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105,
                                                   110, 103, 0, 127,
                                                   127},
                                        HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END),
                                QueryTypeParam.ofIndexScanParam(
                                        new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105, 110,
                                                   103, 0, 0, 0},
                                        new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105, 110,
                                                   103, 0, 127, 127},
                                        HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END)
                        ),
                        List.of(QueryTypeParam.ofIndexScanParam(
                                        new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104
                                                , 97, 105,
                                                   0, 0, 0},
                                        new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104
                                                , 97, 105,
                                                   0, 127, 127},
                                        HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END),
                                QueryTypeParam.ofIndexScanParam(
                                        new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104,
                                                   97, 105, 0, 0, 0},
                                        new byte[]{83, 73, 58, 51, 58, 83, 104, 97, 110, 103, 104,
                                                   97, 105, 0, 127, 127},
                                        HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END)
                        )
                ));

        iterators = getSession().query(param, null);
        count = 0;
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var element = (BaseVertex) iterator.next();
                count += 1;
            }
        }

        assertEquals(countOfCityBeijing + countOfCityShanghai, count);
    }

    @Test
    public void testBuildIndex() throws Exception {
        Metapb.BuildIndexParam param = Metapb.BuildIndexParam.newBuilder()
                                                             .setGraph(DEFAULT_GRAPH)
                                                             .setLabelId(ByteString.copyFrom(
                                                                     idToBytes(1)))
                                                             .setIsVertexLabel(true)
                                                             .setPrefix(ByteString.EMPTY)
                                                             .setAllIndex(true)
                                                             .build();
        HgStoreEngine.getInstance().getDataManager()
                     .doBuildIndex(param, getPartition(0).getProtoObj());
        HgStoreEngine.getInstance().getDataManager()
                     .doBuildIndex(param, getPartition(1).getProtoObj());
        HgStoreEngine.getInstance().getDataManager()
                     .doBuildIndex(param, getPartition(2).getProtoObj());
        HgStoreEngine.getInstance().getDataManager()
                     .doBuildIndex(param, getPartition(3).getProtoObj());
    }

    /**
     * trans element
     */
    // @Test
    public void testIndex3() throws PDException {
        //todo : 过滤。。。 index query : 有个新参数, index2Element method not implemented
        var param = getDefaultParam();
        param.setQueryType(StoreQueryType.NO_SCAN);
        param.setDedupOption(StoreQueryParam.DEDUP_OPTION.PRECISE_DEDUP);
        param.setLoadPropertyFromIndex(true);
        param.setIndexes(List.of(List.of(QueryTypeParam.ofIndexScanParam(
                new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105, 110, 103, 0, 0, 0},
                new byte[]{83, 73, 58, 51, 58, 66, 101, 105, 106, 105, 110, 103, 0, 127, 127},
                HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LT_END))));

        var iterators = getSession().query(param, null);
        for (var iterator : iterators) {
            while (iterator.hasNext()) {
                var item = (BaseVertex) iterator.next();
                printItem(item);
            }
        }
    }

    private void printItem(BaseVertex item) {
        System.out.print(item.id().asString() + "::");
        for (var p : item.properties()) {
            System.out.print(p.propertyKey() + " --> " + p.value() + " ** ");
        }
        System.out.println(item);
    }

    private byte[] idToBytes(long id) {
        BytesBuffer buffer = BytesBuffer.allocate(2);
        buffer.writeId(IdGenerator.of(id));
        return buffer.bytes();
    }

    private StoreQueryParam getDefaultParam() {
        StoreQueryParam param = new StoreQueryParam();
        param.setQueryId(ID_GENERATOR.nextLongValue().toString());
        param.setGraph(DEFAULT_GRAPH);
        param.setTable("g+v");
        param.setQueryType(StoreQueryType.TABLE_SCAN);
        return param;
    }
}
