package com.tinkerpop.blueprints.impls.ramcloud;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.jersey.core.util.Base64;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RamCloudGraph implements IndexableGraph, KeyIndexableGraph, TransactionalGraph, Serializable {

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock write = readWriteLock.writeLock();
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
    private String VERT_TABLE_NAME = "verts";
    private String EDGE_PROP_TABLE_NAME = "edge_props";
    private String VERT_PROP_TABLE_NAME = "vert_props";
    private String IDX_VERT_TABLE_NAME = "idx_vert";
    private String IDX_EDGE_TABLE_NAME = "idx_edge";
    private String KIDX_VERT_TABLE_NAME = "kidx_vert";
    private String KIDX_EDGE_TABLE_NAME = "kidx_edge";
    private String coordinatorLocation;
    private static AtomicLong nextVertexId = new AtomicLong(Long.valueOf(System.getProperty("blueprint.initial", "1")));
    private static final Features FEATURES = new Features();
    public RamCloudIndex index = null;
    public RamCloudKeyIndex KeyIndex = null;

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
	FEATURES.supportsIndices = true;
	FEATURES.supportsKeyIndices = false;
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

	log.info( "Connected to coordinator at {" + coordinatorLocation + "} and created tables {" + vertTableId +"}, {" + vertPropTableId + "}, and {" + edgePropTableId + "}");
    }

    public synchronized JRamCloud getRcClient() {
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
	Long longId;
	if (id == null) {
	    write.lock();
	    try {
		longId = nextVertexId.getAndIncrement();
	    } finally {
		write.unlock();
	    }
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
	    log.info("Adding vertex: [id={" + longId + "}]");
	    return newVertex;
	} catch (IllegalArgumentException e) {
	    log.warn("Tried to create vertex {" + newVertex.toString() + "}: {" + e.getMessage() + "}");
	    return null;
	}
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
	log.info("Removing vertex: [vertex={" + vertex + "}]");

	((RamCloudVertex) vertex).remove();
    }

    @Override
    public Iterable<Vertex> getVertices() {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(vertPropTableId);
	List<Vertex> vertices = new ArrayList<Vertex>();

	while (tableEnum.hasNext()) {
	    vertices.add(new RamCloudVertex(tableEnum.next().key, this));
	}

	return (Iterable<Vertex>) vertices;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {

	List<Vertex> vertices = new ArrayList<Vertex>();
	boolean idx = false;
	List<Object> keyMap = new ArrayList<Object>();

	getIndexedKeys(key, Vertex.class);
	getIndex(key, Vertex.class);
	int mreadMax = 400;

	if (index.exists()) {
	    keyMap = (List<Object>) index.getIndexProperty(value.toString());
	    if (keyMap == null) {
		return (Iterable<Vertex>) vertices;
	    } else {
		idx = true;
	    }
	} else if (KeyIndex.exists()) {
	    keyMap = (List<Object>) KeyIndex.getIndexProperty(value.toString());
	    if (keyMap == null) {
		return (Iterable<Vertex>) vertices;
	    } else {
		idx = true;
	    }
	}

	if (idx) {
	    //System.out.println("keyMap size : " + keyMap.size());
	    final int size = Math.min(mreadMax, keyMap.size());
	    JRamCloud.multiReadObject vertPropTableMread[] = new JRamCloud.multiReadObject[size];

	    int vertexNum = 0;
	    for (Object vert : keyMap) {
		byte[] rckey = 
			ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong((Long) vert).array();
		vertPropTableMread[vertexNum] = new JRamCloud.multiReadObject(vertPropTableId, rckey);
		if (vertexNum >= (mreadMax - 1)) {
		    JRamCloud.Object outvertPropTable[] =
			    getRcClient().multiRead(vertPropTableMread);
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
		JRamCloud.Object outvertPropTable[] = getRcClient().multiRead(vertPropTableMread);
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
		    Map<String, Object> propMap = RamCloudElement.getPropertyMap(tableEntry.value);
		    if (propMap.containsKey(key) && propMap.get(key).equals(value)) {
			vertices.add(new RamCloudVertex(tableEntry.key, this));
		    }
		} 
	    }
	}

	return (Iterable<Vertex>) vertices;
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) throws IllegalArgumentException {
	log.info("Adding edge: [id={" + id + "}, outVertex={" + outVertex + "}, inVertex={" + inVertex + "}, label={" + label + "}]");

	if (label == null) {
	    throw ExceptionFactory.edgeLabelCanNotBeNull();
	}

	RamCloudEdge newEdge = new RamCloudEdge((RamCloudVertex) outVertex, (RamCloudVertex) inVertex, label, this);

	try {
	    newEdge.create();
	    return newEdge;
	} catch (IllegalArgumentException e) {
	    log.warn("Tried to create edge {" + newEdge.toString() + "}: {" + e.getMessage() + "}");
	    return null;
	}
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
	log.info("Removing edge: [edge={" + edge + "}]");

	edge.remove();
    }

    @Override
    public Iterable<Edge> getEdges() {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(edgePropTableId);
	List<Edge> edges = new ArrayList<Edge>();

	while (tableEnum.hasNext()) {
	    edges.add(new RamCloudEdge(tableEnum.next().key, this));
	}

	return (Iterable<Edge>) edges;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(edgePropTableId);
	List<Edge> edges = new ArrayList<Edge>();
	JRamCloud.Object tableEntry;

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    Map<String, Object> propMap = RamCloudElement.getPropertyMap(tableEntry.value);
	    if (propMap.containsKey(key) && propMap.get(key).equals(value)) {
		edges.add(new RamCloudEdge(tableEntry.key, this));
	    }
	}

	return (Iterable<Edge>) edges;
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
	KeyIndex = (RamCloudKeyIndex) getIndexedKeys(key, elementClass);
	KeyIndex.removeIndex();
    }

    @Override
    public <T extends Element> void createKeyIndex(String key,
	    Class<T> elementClass, Parameter... indexParameters) {
	if (key == null) {
	    return;
	}
	if (elementClass == Vertex.class) {
	    KeyIndex = new RamCloudKeyIndex(kidxVertTableId, key, this, elementClass);
	} else if (elementClass == Edge.class) {
	    KeyIndex = new RamCloudKeyIndex(kidxEdgeTableId, key, this, elementClass);
	}
	KeyIndex.create();
	//KeyIndex.reIndexElements(this, getVertices(), new HashSet<String>(Arrays.asList(key)));
    }

    @Override
    public <T extends Element> Set< String> getIndexedKeys(Class< T> elementClass) {
	Set<String> indexkey = new HashSet<String>();
	JRamCloud.Object tableEntry;
	JRamCloud.TableEnumerator tableEnum;

	if (elementClass == Vertex.class) {
	    tableEnum = getRcClient().new TableEnumerator(kidxVertTableId);
	} else {
	    tableEnum = getRcClient().new TableEnumerator(kidxEdgeTableId);
	}

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    String key = new String(tableEntry.key);
	    indexkey.add(key);
	}
	return indexkey;
    }

    public <T extends Element> RamCloudKeyIndex getIndexedKeys(String key, Class<T> elementClass) {

	return KeyIndex = new RamCloudKeyIndex(kidxVertTableId, key, this, elementClass);

    }

    @Override
    public <T extends Element> Index<T> createIndex(String indexName,
	    Class<T> indexClass, Parameter... indexParameters) {
	if (indexName == null) {
	    return null;
	}
	if (indexClass == Vertex.class) {
	    index = new RamCloudIndex(idxVertTableId, indexName, this, indexClass);
	} else if (indexClass == Edge.class) {
	    index = new RamCloudIndex(idxEdgeTableId, indexName, this, indexClass);
	} else {
	    return null;
	}
	index.create();

	return index;
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
	if (indexName == null) {
	    return null;
	}

	if (indexClass.equals(Vertex.class)) {
	    index = new RamCloudIndex(idxVertTableId, indexName, this, indexClass);
	} else if (indexClass.equals(Edge.class)) {
	    index = new RamCloudIndex(idxEdgeTableId, indexName, this, indexClass);
	} else {
	    return null;
	}
	return index;
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
	final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
	JRamCloud.TableEnumerator tableEnum = getRcClient().new TableEnumerator(idxVertTableId);
	JRamCloud.Object tableEntry;

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    list.add(new RamCloudIndex(tableEntry.key, idxVertTableId, this, Vertex.class));
	}

	tableEnum = getRcClient().new TableEnumerator(idxEdgeTableId);

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    list.add(new RamCloudIndex(tableEntry.key, idxEdgeTableId, this, Edge.class));
	}
	return list;
    }

    @Override
    public void dropIndex(String indexName) {
	final Iterator<RamCloudIndex> list = (Iterator<RamCloudIndex>) getIndices();
	while (list.hasNext()) {
	    index = list.next();
	    if (new String(index.rcKey).equals(indexName)) {
		index.removeIndex();
	    }
	}
    }

    //@Override
    public void dropIndex(String indexName, Class indexClass) {
	// Remove ourselves entirely from the vertex table
	index = (RamCloudIndex) getIndex(indexName, indexClass);
	index.removeIndex();
    }

    public static int count(final Iterator<?> iterator) {
	int counter = 0;
	while (iterator.hasNext()) {
	    iterator.next();
	    counter++;
	}
	return counter;
    }

    @Override
    public String toString() {
	return getClass().getSimpleName().toLowerCase() + "[vertices:" + count(getVertices().iterator()) + " edges:" + count(getEdges().iterator()) + "]";
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

    protected class RamCloudKeyIndex<T extends RamCloudElement> extends RamCloudIndex<T> implements Serializable {

	public RamCloudKeyIndex(long tableId, String indexName, RamCloudGraph graph, Class<T> indexClass) {
	    super(tableId, indexName, graph, indexClass);
	}

	public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
	    if (this.exists()) {
		if (oldValue != null) {
		    this.remove(key, oldValue, element);
		    log.info("autoupdate remove object : " + oldValue);
		}
		this.put(key, newValue, element);
	    }
	}

	public void autoRemove(final String key, final Object oldValue, final T element) {
	    if (this.exists()) {
		this.remove(key, oldValue, element);
	    }
	}

	public long reIndexElements(final RamCloudGraph graph, final Iterable<? extends Element> elements, final Set<String> keys) {
	    long counter = 0;
	    for (final Element element : elements) {
		for (final String key : keys) {
		    final Object value = element.removeProperty(key);
		    if (null != value) {
			counter++;
			element.setProperty(key, value);
		    }
		}
	    }
	    return counter;
	}
    }
}
