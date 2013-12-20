package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo2.Kryo;
import com.esotericsoftware.kryo2.io.ByteBufferInput;
import com.esotericsoftware.kryo2.io.ByteBufferOutput;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

// FIXME Index instance should be representing an Index table, not a IndexTable K-V pair
public class RamCloudIndex<T extends Element> implements Index<T>, Serializable {

    private final static Logger log = LoggerFactory.getLogger(RamCloudGraph.class);
    private RamCloudGraph graph;
    private long tableId;
    private String indexName;
    protected byte[] rcKey;
    private Class<T> indexClass;
    // FIXME this should not be defined here
    private long indexVersion;

    private static final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
                 Kryo kryo = new Kryo();
                 kryo.setRegistrationRequired(true);
                 kryo.register(Long.class);
                 kryo.register(String.class);
                 kryo.register(TreeMap.class);
                 kryo.register(ArrayList.class);
                 kryo.setReferences(false);
                 return kryo;
        }
    };


    public RamCloudIndex(long tableId, String indexName, Object propValue, RamCloudGraph graph, Class<T> indexClass) {
	this.tableId = tableId;
	this.graph = graph;
	this.rcKey = indexToRcKey(indexName, propValue);
	this.indexName = indexName;
	this.indexClass = indexClass;
    }

    public RamCloudIndex(long tableId, byte[] rcKey, RamCloudGraph graph, Class<T> indexClass) {
	this.tableId = tableId;
	this.graph = graph;
	this.rcKey = rcKey;
	this.indexName = rcKeyToIndexName(rcKey);
	this.indexClass = indexClass;
    }

    public boolean exists() {
	long startTime = 0;
	
	try {
	    JRamCloud.Object vertTableEntry;
	    JRamCloud vertTable = graph.getRcClient();

	    if (graph.measureRcTimeProp == 1) {
		startTime = System.nanoTime();
	    }
	    //vertTableEntry = graph.getRcClient().read(tableId, rcKey);
	    vertTableEntry = vertTable.read(tableId, rcKey);
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance index exists(indexName {}) read time {}", indexName, endTime - startTime);
	    }
	    indexVersion = vertTableEntry.version;
	    return true;
	} catch (Exception e) {
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance index exists(indexName {}) exception read time {}", indexName, endTime - startTime);
	    }
	    log.debug("IndexTable entry for " + indexName + " does not exists(): " + new String(rcKey) + "@" + tableId + " [" + this.toString() + "]");
	    return false;
	}
    }

    public void create() {
	if (!exists()) {
	    JRamCloud.RejectRules rules = graph.getRcClient().new RejectRules();
	    rules.setExists();
	    try {
		JRamCloud vertTable = graph.getRcClient();

		long startTime = 0;
		if (graph.measureRcTimeProp == 1) {
		    startTime = System.nanoTime();
		}
		//graph.getRcClient().writeRule(tableId, rcKey, ByteBuffer.allocate(0).array(), rules);
		vertTable.writeRule(tableId, rcKey, ByteBuffer.allocate(0).array(), rules);
		if (graph.measureRcTimeProp == 1) {
		    long endTime = System.nanoTime();
		    log.error("Performance index create(indexName {}) write time {}", indexName, endTime - startTime);
		}
	    } catch (Exception e) {
		log.info(toString() + ": Write create index list: " + e.toString());
	    }
	}
    }

    public static byte[] indexToRcKey(String key, Object propValue) {
	try {
	    String s = key + "=" + propValue;
	    return ByteBuffer.allocate(s.getBytes().length).put(s.getBytes("UTF-8")).array();
	} catch (UnsupportedEncodingException ex) {
	    log.error("indexToRcKey({}, {}) failed with exception {}", key, propValue, ex);
	}
	return null;
    }

    public static String rcKeyToIndexName(byte[] rcKey) {
	try {
	    String s = new String(rcKey, "UTF-8");
	    return s.substring(0, s.indexOf('='));
	} catch (UnsupportedEncodingException ex) {
	    log.error("rcKeyToIndexName({}) failed with exception {}", rcKey, ex);
	}
	return null;
    }

    @Override
    public String getIndexName() {
	return this.indexName;
    }

    @Override
    public Class<T> getIndexClass() {
	return this.indexClass;
    }

    @Override
    public void put(String key, Object value, T element) {
	getSetProperty(key, value, element.getId());
    }

    public void getSetProperty(String key, Object propValue, Object elmId) {
	if (elmId == null) {
	    // FIXME Throw appropriate Exception
	    log.error("Element Id cannot be null");
	    return;
	    //throw ExceptionFactory.vertexIdCanNotBeNull();
	    //throw ExceptionFactory.edgeIdCanNotBeNull();
	}

	long startTime = 0;
	if (graph.measureBPTimeProp == 1) {
	    startTime = System.nanoTime();
	}

	create();

	// FIXME give more meaningful loop variable
	for (int i = 0; i < 100; i++) {
	    Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	    List<Object> values = map.get(propValue);
	    if (values == null) {
		values = new ArrayList<Object>();
		map.put(propValue, values);
	    }
	    if (!values.contains(elmId)) {
		values.add(elmId);
	    }

	    long serStartTime = System.nanoTime();
	    byte[] rcValue = convertIndexPropertyMapToRcBytes(map);
	    long serEndTime = System.nanoTime();
	    log.error("Performance index kryo serialization [id={}] {} size {}", elmId, serEndTime - serStartTime, rcValue.length);

	    if (rcValue.length != 0) {
		if (writeWithRules(rcValue)) {
		    break;
		} else {
		    log.debug("getSetProperty(String " + propValue + ", Object " + elmId + ") cond. write failure RETRYING " + (i + 1));
		    if (i == 100) {
			log.error("getSetProperty(String key, Object value) cond. write failure Gaveup RETRYING");
		    }
		}
	    }
	}

	if (graph.measureBPTimeProp == 1) {
	    long endTime = System.nanoTime();
	    log.error("Performance index setProperty total time {}", endTime - startTime);
	}
    }

    @Override
    public CloseableIterable<T> get(String string, Object value) {
	// FIXME Do we need this implemented
	throw new RuntimeException("Not implemented yet");
	//return getElmIdListForPropValue(value);
    }

    @Override
    public CloseableIterable<T> query(String string, Object o) {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long count(String key, Object value) {
	Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	List<Object> values = map.get(value);
	if (null == values) {
	    return 0;
	} else {
	    return values.size();
	}
    }

    @Override
    public void remove(String propName, Object propValue, T element) {

	if (propName == null) {
	    throw ExceptionFactory.propertyKeyCanNotBeNull();
	}

	if (propName.equals("")) {
	    throw ExceptionFactory.propertyKeyCanNotBeEmpty();
	}

	if (propName.equals("id")) {
	    throw ExceptionFactory.propertyKeyIdIsReserved();
	}

	if (!propName.equals(indexName)) {
	    log.error("Index name mismatch indexName:{}, remove({},{},...). SOMETHING IS WRONG", indexName, propName, propValue);
	}

	// FIXME better loop variable name
	for (int i = 0; i < 100; ++i) {
	    Map<Object, List<Object>> map = readIndexPropertyMapFromDB();

	    if (map.containsKey(propValue)) {
		List<Object> idList = map.get(propValue);
		if (null != idList) {
		    idList.remove(element.getId());
		    if (idList.isEmpty()) {
			log.debug("remove({},{},...) called, and list became empty.", propName, propValue);
			map.remove(propValue);
		    }
		}
	    } else {
		// propValue not found
		log.warn("remove({},{},...) called on '{}' index table, but was not found on index. SOMETHING MAY BE WRONG", propName, propValue, this.indexName);
		// no change to DB so exit now
		return;
	    }
	    long startTime = System.nanoTime();
	    byte[] rcValue = convertIndexPropertyMapToRcBytes(map);
	    long endTime = System.nanoTime();
	    log.error("Performance index kryo serialization for removal key {} {} size {}", element, endTime - startTime, rcValue.length);

	    if (rcValue.length == 0) {
		return;
	    }

	    if (writeWithRules(rcValue)) {
		break;
	    } else {
		log.debug("remove({}, {}, T element) write failure RETRYING {}", propName, propValue, (i + 1));
		if (i + 1 == 100) {
		    log.error("remove({}, {}, T element) write failed completely. gave up RETRYING", propName, propValue);
		}
	    }
	}

    }

    public void removeElement(T element) {
	removeElement(this.tableId, element, this.graph);
    }

    // FIXME this methods should not be defined here
    public static <T extends Element> void removeElement(long tableId, T element, RamCloudGraph graph) {
	JRamCloud.TableEnumerator tableEnum = graph.getRcClient().new TableEnumerator(tableId);

	while (tableEnum.hasNext()) {
	    JRamCloud.Object tableEntry = tableEnum.next();
	    Map<Object, List<Object>> indexValMap = convertRcBytesToIndexPropertyMap(tableEntry.value);

	    boolean madeChange = false;
	    Iterator<Map.Entry<Object, List<Object>>> indexValMapIt = indexValMap.entrySet().iterator();
	    while (indexValMapIt.hasNext()) {
		Map.Entry<Object, List<Object>> entry = indexValMapIt.next();
		List<Object> idList = entry.getValue();
		madeChange |= idList.remove(element.getId());
		if (idList.isEmpty()) {
		    madeChange = true;
		    indexValMapIt.remove();
		}
	    }
	    if (madeChange == false) {
		continue;
	    }

	    byte[] rcValue = convertIndexPropertyMapToRcBytes(indexValMap);
	    if (rcValue.length == 0) {
		// nothing to write
		continue;
	    }
	    if (writeWithRules(tableId, tableEntry.key, rcValue, tableEntry.version, graph)) {
		// cond. write success
		continue;
	    } else {
		// cond. write failure
		// FIXME Dirty hack
		for (int retry = 100; retry >= 0; --retry) {
		    RamCloudKeyIndex idx = new RamCloudKeyIndex(tableId, tableEntry.key, graph, element.getClass());
		    Map<Object, List<Object>> rereadMap = idx.readIndexPropertyMapFromDB();

		    boolean madeChangeOnRetry = false;
		    Iterator<Map.Entry<Object, List<Object>>> rereadIndexValMapIt = rereadMap.entrySet().iterator();
		    while (rereadIndexValMapIt.hasNext()) {
			Map.Entry<Object, List<Object>> entry = rereadIndexValMapIt.next();
			List<Object> idList = entry.getValue();
			madeChangeOnRetry |= idList.remove(element.getId());
			if (idList.isEmpty()) {
			    madeChangeOnRetry = true;
			    rereadIndexValMapIt.remove();
			}
		    }
		    if (madeChangeOnRetry == false) {
			log.debug("removeElement({}, {}, ...) no more write required. SOMETHING MAY BE WRONG", tableId, element);
			break;
		    }

		    if (idx.writeWithRules(convertIndexPropertyMapToRcBytes(rereadMap))) {
			log.debug("removeElement({}, {}, ...) cond. key/value {} write failure RETRYING {}", tableId, element, rereadMap, retry);
			// cond. re-write success
			break;
		    }
		    if (retry == 0) {
			log.error("removeElement({}, {}, ...) cond. write failed completely. Gave up RETRYING", tableId, element);
			// XXX may be we should throw some king of exception here?
		    }
		}
	    }
	}
    }

    public Map<Object, List<Object>> readIndexPropertyMapFromDB() {
	//log.debug("getIndexPropertyMap() ");
	JRamCloud.Object propTableEntry;
	long startTime = 0;
	
	try {
	    JRamCloud vertTable = graph.getRcClient();
	    if (graph.measureRcTimeProp == 1) {
		startTime = System.nanoTime();
	    }
	    //propTableEntry = graph.getRcClient().read(tableId, rcKey);
	    propTableEntry = vertTable.read(tableId, rcKey);
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance readIndexPropertyMapFromDB(indexName {}) read time {}", indexName, endTime - startTime);
	    }
	    indexVersion = propTableEntry.version;
	} catch (Exception e) {
	    indexVersion = 0;
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance readIndexPropertyMapFromDB(indexName {}) exception read time {}", indexName, endTime - startTime);
	    }
	    log.warn("readIndexPropertyMapFromDB() Element does not have a index property table entry! tableId :" + tableId + " indexName : " + indexName + " " + e);
	    return null;
	}

	return convertRcBytesToIndexPropertyMap(propTableEntry.value);
    }

    public static Map<Object, List<Object>> convertRcBytesToIndexPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.error("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
            long startTime = System.nanoTime();
	    ByteBufferInput input = new ByteBufferInput(byteArray);
	    TreeMap map = kryo.get().readObject(input, TreeMap.class);
            long endTime = System.nanoTime();
            log.error("Performance index kryo deserialization [id=N/A] {} size {}", endTime - startTime, byteArray.length);
	    return map;
	} else {
	    return new TreeMap<Object, List<Object>>();
	}
    }

    public static byte[] convertIndexPropertyMapToRcBytes(Map<Object, List<Object>> map) {
        //long startTime = System.nanoTime();
	ByteBufferOutput output = new ByteBufferOutput(1024*1024);
	kryo.get().writeObject(output, map);
	byte[] bytes = output.toBytes();
        //long endTime = System.nanoTime();
        //log.error("Performance index kryo serialization {}", endTime - startTime);
	return bytes;
    }

    protected boolean writeWithRules(byte[] rcValue) {
	return writeWithRules(this.tableId, this.rcKey, rcValue, this.indexVersion, this.graph);
    }

    private static boolean writeWithRules(long tableId, byte[] rcKey, byte[] rcValue, long expectedVersion, RamCloudGraph graph) {
	JRamCloud.RejectRules rules = graph.getRcClient().new RejectRules();

	if (expectedVersion == 0) {
	    rules.setExists();
	} else {
	    rules.setNeVersion(expectedVersion);
	}

	try {
	    JRamCloud vertTable = graph.getRcClient();
	    long startTime = 0;
	    if (graph.measureRcTimeProp == 1) {
		startTime = System.nanoTime();
	    }
	    vertTable.writeRule(tableId, rcKey, rcValue, rules);
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance writeWithRules(indexName {}) write time {}", rcKeyToIndexName(rcKey), endTime - startTime);
	    }
	} catch (Exception e) {
	    log.debug("Cond. Write index property: " + new String(rcKey) + " failed " + e.toString() + " expected version: " + expectedVersion);
	    return false;
	}
	return true;
    }

    public List<Object> getElmIdListForPropValue(Object propValue) {
	Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	if (map == null) {
	    log.error("IndexPropertyMap was null. " + this.indexName + " : " + propValue);
	    return null;
	}
	return map.get(propValue);
    }

    public Set<Object> getIndexPropertyKeys() {
	Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	return map.keySet();
    }

    public <T> T removeIndexProperty(String key) {
	for (int i = 0; i < 100; ++i) {
	    Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	    T retVal = (T) map.remove(key);
	    byte[] rcValue = convertIndexPropertyMapToRcBytes(map);
	    if (rcValue.length != 0) {
		if (writeWithRules(rcValue)) {
		    return retVal;
		} else {
		    log.info("removeIndexProperty({}, {}, ...) cond. key/value write failure RETRYING {}", tableId, retVal, (i + 1));
		}
	    }
	}
	// XXX ?Is this correct
	return null;
    }

    public void removeIndex() {
	log.info("Removing Index: " + indexName + " was version " + indexVersion + " [" + this + "]");
	graph.getRcClient().remove(tableId, rcKey);
    }
}
