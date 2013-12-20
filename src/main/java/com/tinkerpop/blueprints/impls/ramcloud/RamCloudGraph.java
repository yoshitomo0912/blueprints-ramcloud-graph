package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo2.Kryo;
import com.sun.jersey.core.util.Base64;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;
import java.nio.ByteOrder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RamCloudGraph implements IndexableGraph, KeyIndexableGraph, TransactionalGraph, Serializable {

    private final static Logger log = LoggerFactory.getLogger(RamCloudGraph.class);

    private static final ThreadLocal<JRamCloud> RamCloudThreadLocal = new ThreadLocal<JRamCloud>();

    protected JRamCloud rcClient;
    protected long vertTableId; //(vertex_id) --> ( (n,d,ll,l), (n,d,ll,l), ... )
    protected long vertPropTableId; //(vertex_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
    protected long edgePropTableId; //(edge_id) -> ( (kl,k,vl,v), (kl,k,vl,v), ... )
    protected long idxVertTableId;
    protected long idxEdgeTableId;
    protected long kidxVertTableId;
    protected long kidxEdgeTableId;
    protected long instanceTableId;
    private String VERT_TABLE_NAME = "verts";
    private String EDGE_PROP_TABLE_NAME = "edge_props";
    private String VERT_PROP_TABLE_NAME = "vert_props";
    private String IDX_VERT_TABLE_NAME = "idx_vert";
    private String IDX_EDGE_TABLE_NAME = "idx_edge";
    private String KIDX_VERT_TABLE_NAME = "kidx_vert";
    private String KIDX_EDGE_TABLE_NAME = "kidx_edge";
    private final String INSTANCE_TABLE_NAME = "instance";
    private long instanceId;
    private long nextVertexId;
    private final int INSTANCE_ID_RANGE = 10000;
    private String coordinatorLocation;
    private static final Features FEATURES = new Features();
    public final long measureBPTimeProp = Long.valueOf(System.getProperty("benchmark.measureBP", "0"));
    public final long measureRcTimeProp = Long.valueOf(System.getProperty("benchmark.measureRc", "0"));

    public final Set<String> indexedKeys = new HashSet<String>();

    static {
	FEATURES.supportsSerializableObjectProperty = true;
	FEATURES.supportsBooleanProperty = true;
	FEATURES.supportsDoubleProperty = true;
	FEATURES.supportsFloatProperty = true;
	FEATURES.supportsIntegerProperty = true;
	FEATURES.supportsPrimitiveArrayProperty = true;
	FEATURES.supportsUniformListProperty = true;
	FEATURES.supportsMixedListProperty = true;
	FEATURES.supportsLongProperty = true;
	FEATURES.supportsMapProperty = true;
	FEATURES.supportsStringProperty = true;

	FEATURES.supportsDuplicateEdges = false;
	FEATURES.supportsSelfLoops = false;
	FEATURES.isPersistent = false;
	FEATURES.isWrapper = false;
	FEATURES.supportsVertexIteration = true;
	FEATURES.supportsEdgeIteration = true;
	FEATURES.supportsVertexIndex = true;
	FEATURES.supportsEdgeIndex = false;
	FEATURES.ignoresSuppliedIds = true;
	FEATURES.supportsTransactions = false;
	FEATURES.supportsIndices = false;
	FEATURES.supportsKeyIndices = true;
	FEATURES.supportsVertexKeyIndex = true;
	FEATURES.supportsEdgeKeyIndex = false;
	FEATURES.supportsEdgeRetrieval = true;
	FEATURES.supportsVertexProperties = true;
	FEATURES.supportsEdgeProperties = true;
	FEATURES.supportsThreadedTransactions = false;
    }

    static {
	System.loadLibrary("edu_stanford_ramcloud_JRamCloud");
    }

    public RamCloudGraph() {
	this("fast+udp:host=127.0.0.1,port=12246");
    }


    public RamCloudGraph(String coordinatorLocation) {
	this.coordinatorLocation = coordinatorLocation;

	rcClient = getRcClient();

	vertTableId = rcClient.createTable(VERT_TABLE_NAME);
	vertPropTableId = rcClient.createTable(VERT_PROP_TABLE_NAME);
	edgePropTableId = rcClient.createTable(EDGE_PROP_TABLE_NAME);
	idxVertTableId = rcClient.createTable(IDX_VERT_TABLE_NAME);
	idxEdgeTableId = rcClient.createTable(IDX_EDGE_TABLE_NAME);
	kidxVertTableId = rcClient.createTable(KIDX_VERT_TABLE_NAME);
	kidxEdgeTableId = rcClient.createTable(KIDX_EDGE_TABLE_NAME);
	instanceTableId = rcClient.createTable(INSTANCE_TABLE_NAME);

	log.info( "Connected to coordinator at {}", coordinatorLocation);
	log.debug("VERT_TABLE:{}, VERT_PROP_TABLE:{}, EDGE_PROP_TABLE:{}, IDX_VERT_TABLE:{}, IDX_EDGE_TABLE:{}, KIDX_VERT_TABLE:{}, KIDX_EDGE_TABLE:{}", vertTableId, vertPropTableId, edgePropTableId, idxVertTableId, idxEdgeTableId, kidxVertTableId, kidxEdgeTableId);
	nextVertexId = 0;
        initInstance();
    }

    public JRamCloud getRcClient() {
	rcClient = RamCloudThreadLocal.get();
	if (rcClient == null) {
	    rcClient = new JRamCloud(coordinatorLocation);
	    RamCloudThreadLocal.set(rcClient);
	}
	return rcClient;
    }

    @Override
    public Features getFeatures() {
	return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
	long startTime = 0;
	if (measureBPTimeProp == 1) { 
	    startTime = System.nanoTime();
	}
	Long longId;
	if (id == null) {
	    longId = ++nextVertexId;
	} else if (id instanceof Integer) {
	    longId = ((Integer) id).longValue();
	} else if (id instanceof Long) {
	    longId = (Long) id;
	} else if (id instanceof String) {
	    try {
		longId = Long.parseLong((String) id, 10);
	    } catch (NumberFormatException e) {
		log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not a parseable long number: {" + e.toString() + "}");
		return null;
	    }
	} else if (id instanceof byte[]) {
	    try {
		longId = ByteBuffer.wrap((byte[]) id).getLong();
	    } catch (BufferUnderflowException e) {
		log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not a parseable long number: {" + e.toString() + "}");
		return null;
	    }
	} else {
	    log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not supported. Returning null.");
	    return null;
	}

	RamCloudVertex newVertex = new RamCloudVertex(longId, this);

	try {
	    newVertex.create();
	    if (measureBPTimeProp == 1) { 
		long endTime = System.nanoTime();
		log.error("Performance addVertex total time {}", endTime - startTime);
	    }
	    log.info("Added vertex: [id=" + longId + "]");
	    return newVertex;
	} catch (IllegalArgumentException e) {
	    log.error("Tried to create vertex failed {" + newVertex.toString() + "}: {" + e.getMessage() + "}");
	    return null;
	}
    }

    private final void initInstance() {
        //long incrementValue = 1;
        JRamCloud.Object instanceEntry = null;
        try {
            instanceEntry = getRcClient().read(instanceTableId, "nextInstanceId".getBytes());
        } catch (Exception e) {
            if (e instanceof JRamCloud.ObjectDoesntExistException) {
                instanceId = 0;
                getRcClient().write(instanceTableId, "nextInstanceId".getBytes(), ByteBuffer.allocate(0).array());
            }
        }
        if (instanceEntry != null) {
	    long curInstanceId = 1;
	    for (int i = 0 ; i < 100 ; i++) {
		Map<String, Long> propMap = null;
		if (instanceEntry.value == null) {
		    log.warn("Got a null byteArray argument");
		    return;
		} else if (instanceEntry.value.length != 0) {
		    try {
			ByteArrayInputStream bais = new ByteArrayInputStream(instanceEntry.value);
			ObjectInputStream ois = new ObjectInputStream(bais);
			propMap = (Map<String, Long>) ois.readObject();
		    } catch (IOException e) {
			log.error("Got an exception while deserializing element''s property map: {" + e.toString() + "}");
			return;
		    } catch (ClassNotFoundException e) {
			log.error("Got an exception while deserializing element''s property map: {" + e.toString() + "}");
			return;
		    }
		} else {
		    propMap = new HashMap<String, Long>();
		}

		if (propMap.containsKey(INSTANCE_TABLE_NAME)) {
		    curInstanceId = propMap.get(INSTANCE_TABLE_NAME) + 1;
		}

		propMap.put(INSTANCE_TABLE_NAME, curInstanceId);

		byte[] rcValue = null;
		try {
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    ObjectOutputStream oot = new ObjectOutputStream(baos);
		    oot.writeObject(propMap);
		    rcValue = baos.toByteArray();
		} catch (IOException e) {
		    log.error("Got an exception while serializing element''s property map: {" + e.toString() + "}");
		    return;
		}
		JRamCloud.RejectRules rules = rcClient.new RejectRules();
		rules.setNeVersion(instanceEntry.version);
		try {
		    getRcClient().writeRule(instanceTableId, "nextInstanceId".getBytes(), rcValue, rules);
		    instanceId = curInstanceId;
		    break;
		} catch (Exception ex) {
		    log.debug("Cond. Write increment Vertex property: " + ex.toString());
		    instanceEntry = getRcClient().read(instanceTableId, "nextInstanceId".getBytes());
		    continue;
		}
	    }
	}

	nextVertexId = instanceId * INSTANCE_ID_RANGE;
    }

    @Override
    public Vertex getVertex(Object id) throws IllegalArgumentException {
	Long longId;

	if (id == null) {
	    throw ExceptionFactory.vertexIdCanNotBeNull();
	} else if (id instanceof Integer) {
	    longId = ((Integer) id).longValue();
	} else if (id instanceof Long) {
	    longId = (Long) id;
	} else if (id instanceof String) {
	    try {
		longId = Long.parseLong((String) id, 10);
	    } catch (NumberFormatException e) {
		log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() +"} is not a parseable long number: {" + e.toString() + "}");
		return null;
	    }
	} else if (id instanceof byte[]) {
	    try {
		longId = ByteBuffer.wrap((byte[]) id).getLong();
	    } catch (BufferUnderflowException e) {
		log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not a parseable long number: {" + e.toString() + "}");
		return null;
	    }
	} else {
	    log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not supported. Returning null.");
	    return null;
	}

	RamCloudVertex vertex = new RamCloudVertex(longId, this);

	if (vertex.exists()) {
	    return vertex;
	} else {
	    return null;
	}
    }

    @Override
    public void removeVertex(Vertex vertex) {
	((RamCloudVertex) vertex).remove();
    }

    @Override
    public Iterable<Vertex> getVertices() {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(vertPropTableId);
	List<Vertex> vertices = new LinkedList<Vertex>();

	while (tableEnum.hasNext()) {
		vertices.add(new RamCloudVertex(tableEnum.next().key, this));
	}

	return vertices;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {

	long startTime = 0;
	if (measureBPTimeProp == 1) { 
	    startTime = System.nanoTime();
	}
	
	List<Vertex> vertices = new ArrayList<Vertex>();
	List<Object> vertexList = null;
	JRamCloud vertTable = getRcClient();

	int mreadMax = 400;

	if (indexedKeys.contains(key)) {
	    RamCloudKeyIndex KeyIndex = new RamCloudKeyIndex(kidxVertTableId, key, value, this, Vertex.class);
	    vertexList = KeyIndex.getElmIdListForPropValue(value.toString());
	    if (vertexList == null) {
		if (measureBPTimeProp == 1) { 
		    long endTime = System.nanoTime();
		    log.error("Performance getVertices(key {}) does not exists total time {}", key, endTime - startTime);
		}
		return vertices;
	    }

	    final int size = Math.min(mreadMax, vertexList.size());
	    JRamCloud.multiReadObject vertPropTableMread[] = new JRamCloud.multiReadObject[size];

	    int vertexNum = 0;
	    for (Object vert : vertexList) {
		byte[] rckey =
			ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong((Long) vert).array();
		vertPropTableMread[vertexNum] = new JRamCloud.multiReadObject(vertPropTableId, rckey);
		if (vertexNum >= (mreadMax - 1)) {
		    JRamCloud.Object outvertPropTable[] =
			    vertTable.multiRead(vertPropTableMread);
		    for (int i = 0; i < vertexNum; i++) {
			if (outvertPropTable[i] != null) {
			    vertices.add(new RamCloudVertex(outvertPropTable[i].key, this));
			}
		    }
		    vertexNum = 0;
		}
		vertexNum++;
	    }

	    if (vertexNum != 0) {
		long startTime2 = 0;
		if (measureRcTimeProp == 1) {
		    startTime2 = System.nanoTime();
		}
		JRamCloud.Object outvertPropTable[] = vertTable.multiRead(vertPropTableMread);
		if (measureRcTimeProp == 1) { 
		    long endTime2 = System.nanoTime();
		    log.error("Performance index multiread(key {}, number {}) time {}", key, vertexNum, endTime2 - startTime2);
		}
		for (int i = 0; i < vertexNum; i++) {
		    if (outvertPropTable[i] != null) {
			vertices.add(new RamCloudVertex(outvertPropTable[i].key, this));
		    }
		}
	    }
	} else {

	    JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(vertPropTableId);
	    JRamCloud.Object tableEntry;

	    while (tableEnum.hasNext()) {
		tableEntry = tableEnum.next();
		if (tableEntry != null) {
		    //XXX remove temp
		    // RamCloudVertex temp = new RamCloudVertex(tableEntry.key, this);
		    Map<String, Object> propMap = RamCloudElement.convertRcBytesToPropertyMapEx(tableEntry.value);
		    if (propMap.containsKey(key) && propMap.get(key).equals(value)) {
			vertices.add(new RamCloudVertex(tableEntry.key, this));
		    }
		}
	    }
	}
	
	if (measureBPTimeProp == 1) { 
		long endTime = System.nanoTime();
		log.error("Performance getVertices exists total time {}.", endTime - startTime);
	}

	return vertices;
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) throws IllegalArgumentException {
	log.info("Adding edge: [id={" + id + "}, outVertex={" + outVertex + "}, inVertex={" + inVertex + "}, label={" + label + "}]");

	if (label == null) {
	    throw ExceptionFactory.edgeLabelCanNotBeNull();
	}

	RamCloudEdge newEdge = new RamCloudEdge((RamCloudVertex) outVertex, (RamCloudVertex) inVertex, label, this);

	for (int i = 0; i < 5 ;i++) {
	    try {
		newEdge.create();
		return newEdge;
	    } catch (Exception e) {
		log.warn("Tried to create edge failed: {" + newEdge.toString() + "}: {" + e.toString() + "}");

		if (e instanceof NoSuchElementException) {
		    log.warn("addEdge retry " + i);
		    continue;
		}
	    }
	}
	return null;
    }

    @Override
    public Edge getEdge(Object id) throws IllegalArgumentException {
	byte[] bytearrayId;

	if (id == null) {
	    throw ExceptionFactory.edgeIdCanNotBeNull();
	} else if (id instanceof byte[]) {
	    bytearrayId = (byte[]) id;
	} else if (id instanceof String) {
	    bytearrayId = Base64.decode(((String) id));
	} else {
	    log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is not supported. Returning null.");
	    return null;
	}

	if (!RamCloudEdge.isValidEdgeId(bytearrayId)) {
	    log.warn("ID argument {" + id.toString() + "} of type {" + id.getClass() + "} is malformed. Returning null.");
	    return null;
	}

	RamCloudEdge edge = new RamCloudEdge(bytearrayId, this);

	if (edge.exists()) {
	    return edge;
	} else {
	    return null;
	}
    }

    @Override
    public void removeEdge(Edge edge) {
	edge.remove();
    }

    @Override
    public Iterable<Edge> getEdges() {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(edgePropTableId);
	List<Edge> edges = new ArrayList<Edge>();

	while (tableEnum.hasNext()) {
	    edges.add(new RamCloudEdge(tableEnum.next().key, this));
	}

	return edges;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(edgePropTableId);
	List<Edge> edges = new ArrayList<Edge>();
	JRamCloud.Object tableEntry;

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
		// FIXME temp
		RamCloudEdge temp = new RamCloudEdge(tableEntry.key, this);
	    Map<String, Object> propMap = temp.convertRcBytesToPropertyMap(tableEntry.value);
	    if (propMap.containsKey(key) && propMap.get(key).equals(value)) {
		edges.add(temp);
	    }
	}

	return edges;
    }

    @Override
    public GraphQuery query() {
	return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
	getRcClient().dropTable(VERT_TABLE_NAME);
	getRcClient().dropTable(VERT_PROP_TABLE_NAME);
	getRcClient().dropTable(EDGE_PROP_TABLE_NAME);
	getRcClient().dropTable(IDX_VERT_TABLE_NAME);
	getRcClient().dropTable(IDX_EDGE_TABLE_NAME);
	getRcClient().dropTable(KIDX_VERT_TABLE_NAME);
	getRcClient().dropTable(KIDX_EDGE_TABLE_NAME);
	getRcClient().disconnect();
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
	// TODO Auto-generated method stub
    }

    @Override
    public void commit() {
	// TODO Auto-generated method stub
    }

    @Override
    public void rollback() {
	// TODO Auto-generated method stub
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
	throw new UnsupportedOperationException("Not supported yet.");
	//FIXME how to dropKeyIndex
	//new RamCloudKeyIndex(kidxVertTableId, key, this, elementClass);
	//getIndexedKeys(key, elementClass).removeIndex();
    }

    @Override
    public <T extends Element> void createKeyIndex(String key,
	    Class<T> elementClass, Parameter... indexParameters) {
	if (key == null) {
	    return;
	}
	if (this.indexedKeys.contains(key)) {
	    return;
	}
	this.indexedKeys.add(key);
    }

    @Override
    public <T extends Element> Set< String> getIndexedKeys(Class< T> elementClass) {
	if (null != this.indexedKeys) {
	    return new HashSet<String>(this.indexedKeys);
	} else {
	    return Collections.emptySet();
	}
    }

    @Override
    public <T extends Element> Index<T> createIndex(String indexName,
	    Class<T> indexClass, Parameter... indexParameters) {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dropIndex(String indexName) {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String toString() {
	return getClass().getSimpleName().toLowerCase() + "[vertices:" + ((List<Vertex>)getVertices()).size() + " edges:" + ((List<Edge>)getEdges()).size() + "]";
    }

    public static void main(String[] args) {
	RamCloudGraph graph = new RamCloudGraph();

	Vertex a = graph.addVertex(null);
	Vertex b = graph.addVertex(null);
	Vertex c = graph.addVertex(null);
	Vertex d = graph.addVertex(null);
	Vertex e = graph.addVertex(null);
	Vertex f = graph.addVertex(null);
	Vertex g = graph.addVertex(null);

	graph.addEdge(null, a, a, "friend");
	graph.addEdge(null, a, b, "friend1");
	graph.addEdge(null, a, b, "friend2");
	graph.addEdge(null, a, b, "friend3");
	graph.addEdge(null, a, c, "friend");
	graph.addEdge(null, a, d, "friend");
	graph.addEdge(null, a, e, "friend");
	graph.addEdge(null, a, f, "friend");
	graph.addEdge(null, a, g, "friend");

	graph.shutdown();
    }
}
