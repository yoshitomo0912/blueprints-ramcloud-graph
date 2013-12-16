package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private long indexVersion ;

    public RamCloudIndex(long tableId, String indexName, RamCloudGraph graph, Class<T> indexClass) {
	this.tableId = tableId;
	this.rcKey = indexToRcKey(indexName);
	this.graph = graph;
	this.indexName = indexName;
	this.indexClass = indexClass;
    }

    public RamCloudIndex(long tableId, byte[] rcKey, RamCloudGraph graph, Class<T> indexClass) {
	this.tableId = tableId;
	this.rcKey = rcKey;
	this.graph = graph;
	this.indexName = new String(rcKey);
	this.indexClass = indexClass;
    }

    public boolean exists() {
	try {
	    JRamCloud.Object vertTableEntry;
	    vertTableEntry = graph.getRcClient().read(tableId, rcKey);
	    log.debug(indexName +" exists(): "+new String(rcKey)+"@"+tableId+" Update version " + indexVersion + " -> " + vertTableEntry.version + "["+this.toString()+"]");
	    indexVersion = vertTableEntry.version;
	    return true;
	} catch (Exception e) {
		//log.info(e.toString() + ": exists() Exception thrown ");
	    return false;
	}
    }

    public void create() {
	if (!exists()) {
	    JRamCloud.RejectRules rules = graph.getRcClient().new RejectRules();
	    rules.setExists();
	    try {
		graph.getRcClient().writeRule(tableId, rcKey, ByteBuffer.allocate(0).array(), rules);
	    } catch (Exception e) {
		log.info(toString() + ": Write create index list: " + e.toString());
	    }
	}
    }

    private static byte[] indexToRcKey(String indexName) {
	return ByteBuffer.allocate(indexName.length()).order(ByteOrder.LITTLE_ENDIAN).put(indexName.getBytes()).array();
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
	getSetProperty(value, element.getId());
    }

    public void getSetProperty(Object propValue, Object elmId) {
	if (elmId == null) {
		// FIXME Throw appropriate Exception
		log.error("Element Id cannot be null");
		return;
		//throw ExceptionFactory.vertexIdCanNotBeNull();
		//throw ExceptionFactory.edgeIdCanNotBeNull();
	}

	// FIXME give more meaningful loop variable
	for (int i = 0 ; i < 100 ; i++) {
	    Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	    List<Object> values = new ArrayList<Object>();

	    if (map.containsKey(propValue)) {
		boolean found = false;
		for (Map.Entry<Object, List<Object>> entry : map.entrySet()) {
		    if (entry.getKey().equals(propValue)) {
			values = entry.getValue();
			found = true;
			break;
		    }
		}
		if (found) {
		    if (!values.contains(elmId)) {
			values.add(elmId);
		    }
		}
	    } else {
		values.add(elmId);
	    }

	    map.put(propValue, values);

	    byte[] rcValue = convertIndexPropertyMapToRcBytes(map);
	    if (rcValue.length != 0) {
		if (writeWithRules(rcValue)) {
		    break;
		} else {
		    log.debug("getSetProperty(String key, Object value) cond. write failure RETRYING " + (i+1));
		    if ( i == 4 ) {
		    	log.error("getSetProperty(String key, Object value) cond. write failure Gaveup RETRYING");
		    }
		}
	    }
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
	if (propValue == null) {
		//FIXME Is this check required?
		throw ExceptionFactory.propertyValueCanNotBeNull();
	}

	if (propName == null) {
	    throw ExceptionFactory.propertyKeyCanNotBeNull();
	}

	if (propName.equals("")) {
	    throw ExceptionFactory.propertyKeyCanNotBeEmpty();
	}

	if (propName.equals("id")) {
	    throw ExceptionFactory.propertyKeyIdIsReserved();
	}

	if( !propName.equals(indexName) ) {
		log.error("Index name mismatch indexName:{}, remove({},{},...). SOMETHING IS WRONG", indexName, propName, propValue);
	}

	// FIXME better loop variable name
	for (int i = 0; i < 100; ++i) {
		Map<Object, List<Object>> map = readIndexPropertyMapFromDB();

		if (map.containsKey(propValue)) {
			List<Object> objects = map.get(propValue);
			if (null != objects) {
				objects.remove(element.getId());
				if (objects.isEmpty()) {
					map.remove(propValue);
				}
			}
		} else {
			// propValue not found
			log.warn("remove({},{},...) called, but was not found on index. SOMETHING MAY BE WRONG", propName, propValue);
			// no change to DB so exit now
			return;
		}
		byte[] rcValue = convertIndexPropertyMapToRcBytes(map);
		if (rcValue.length == 0) return;

		if (writeWithRules(rcValue)) {
			break;
		} else {
			log.debug("remove({}, {}, T element) write failure RETRYING {}", propName, propValue, (i + 1));
			if ( i+1 == 100 ) {
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
	log.debug("removeElement({}, {}, ...)", tableId, element  );
	JRamCloud.TableEnumerator tableEnum = graph.getRcClient().new TableEnumerator(tableId);

	while (tableEnum.hasNext()) {
		JRamCloud.Object tableEntry = tableEnum.next();
		Map<Object, List<Object>> indexValMap = convertRcBytesToIndexPropertyMap(tableEntry.value);

		boolean madeChange = false;
		Iterator<Map.Entry<Object, List<Object>>> indexValMapIt = indexValMap.entrySet().iterator();
		while( indexValMapIt.hasNext() ) {
			Map.Entry<Object, List<Object>> entry = indexValMapIt.next();
			List<Object> idList = entry.getValue();
			madeChange |= idList.remove(element.getId());
			if (idList.isEmpty()) {
				madeChange = true;
				indexValMapIt.remove();
			}
		}
		if ( madeChange == false ) continue;

		byte[] rcValue = convertIndexPropertyMapToRcBytes(indexValMap);
		if (rcValue.length == 0) {
			// nothing to write
			continue;
		}
		log.debug("removeElement({}, {}, ...) - writing changes", tableId, element );
		if (writeWithRules( tableId, tableEntry.key, rcValue, tableEntry.version, graph )) {
			// cond. write success
			continue;
		} else {
			// cond. write failure
			// FIXME Dirty hack
			for ( int retry = 100 ; retry >= 0 ; --retry ) {
				log.debug("removeElement({}, {}, ...) cond. write failure RETRYING {}", tableId, element, retry );
				RamCloudIndex<T> idx = new RamCloudIndex<T>(tableId, tableEntry.key, graph, (Class<T>)element.getClass() );
				Map<Object, List<Object>> rereadMap = idx.readIndexPropertyMapFromDB();

				boolean madeChangeOnRetry = false;
				Iterator<Map.Entry<Object, List<Object>>> rereadIndexValMapIt = rereadMap.entrySet().iterator();
				while( rereadIndexValMapIt.hasNext() ) {
					Map.Entry<Object, List<Object>> entry = rereadIndexValMapIt.next();
					List<Object> idList = entry.getValue();
					madeChangeOnRetry |= idList.remove(element.getId());
					if (idList.isEmpty()) {
						madeChangeOnRetry = true;
						rereadIndexValMapIt.remove();
					}
				}
				if ( madeChangeOnRetry == false ){
					log.debug("removeElement({}, {}, ...) no more write required. SOMETHING MAY BE WRONG", tableId, element );
					break;
				}

				if ( idx.writeWithRules(convertIndexPropertyMapToRcBytes(rereadMap)) ) {
					// cond. re-write success
					break;
				}
				if ( retry == 0 ) {
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

	try {
	    propTableEntry = graph.getRcClient().read(tableId, rcKey);
		log.debug("getIndexPropertyMap() " + indexName + " Update version " + indexVersion + " -> " + propTableEntry.version + " ["+this.toString()+"]");
	    indexVersion = propTableEntry.version;
	} catch (Exception e) {
	    indexVersion = 0;
	    log.warn(e.toString() + " Element does not have a index property table entry! tableId :"+ tableId + " indexName : " + indexName );
	    return null;
	}

	return convertRcBytesToIndexPropertyMap(propTableEntry.value);
    }

    public static Map<Object, List<Object>> convertRcBytesToIndexPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.error("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    try {
		ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Map<Object, List<Object>> map = (Map<Object, List<Object>>) ois.readObject();
		return map;
	    } catch (IOException e) {
		log.error("Got an IOException while deserializing element''s property map: {"+ e.toString() + "}");
		return null;
	    } catch (ClassNotFoundException e) {
		log.error("Got a ClassNotFoundException while deserializing element''s property map: {"+ e.toString() + "}");
		return null;
	    }
	} else {
	    return new HashMap<Object, List<Object>>();
	}
    }

    public static byte[] convertIndexPropertyMapToRcBytes(Map<Object, List<Object>> map) {
	byte[] rcValue = null;

	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024*1024);
	    ObjectOutputStream oot = new ObjectOutputStream(baos);
	    oot.writeObject(map);
	    rcValue = baos.toByteArray();
	} catch (IOException e) {
	    log.error("Got an exception while serializing element''s property map: {"+ e.toString() + "}");
	}

	return rcValue;

    }

    private boolean writeWithRules(byte[] rcValue) {
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
	    graph.getRcClient().writeRule(tableId, rcKey, rcValue, rules);
	} catch (Exception e) {
	    log.debug("Cond. Write index property: " + new String(rcKey) + " failed " + e.toString() + " expected version: " + expectedVersion);
	    return false;
	}
    	return true;
    }

    public List<Object> getElmIdListForPropValue(Object propValue) {
	Map<Object, List<Object>> map = readIndexPropertyMapFromDB();
	if ( map == null ) {
		log.error("IndexPropertyMap was null. " + this.indexName +" : " + propValue);
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
					log.info("write failure " + (i + 1));
					// TODO ERROR message
				}
			}
		}
		// XXX ?Is this correct
		return null;
	}

    public void removeIndex() {
	log.info("Removing Index: " + indexName + " was version " + indexVersion + " [" + this +"]");
	graph.getRcClient().remove(tableId, rcKey);
    }
}
