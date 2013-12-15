package com.tinkerpop.blueprints.impls.ramcloud;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import edu.stanford.ramcloud.JRamCloud;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RamCloudIndex<T extends Element> implements Index<T>, Serializable {

    private final static Logger log = LoggerFactory.getLogger(RamCloudGraph.class);
    protected byte[] rcKey;
    private RamCloudGraph graph;
    private long tableId;
    private String indexName;
    private Class<T> indexClass;

    private long indexVersion ;

    public RamCloudIndex(long tableId, String indexName, RamCloudGraph graph, Class<T> indexClass) {
	this.tableId = tableId;
	this.rcKey = indexToRcKey(indexName);
	this.graph = graph;
	this.indexName = indexName;
	this.indexClass = indexClass;
    }

    public RamCloudIndex(byte[] rcKey, long tableId, RamCloudGraph graph, Class<T> indexClass) {
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
		log.info(toString() + ": exists() Update version " + indexVersion + " -> " + vertTableEntry.version );
	    indexVersion = vertTableEntry.version;
	    return true;
	} catch (Exception e) {
		log.info(e.toString() + ": exists() Exception thrown ");
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
	getSetProperty(value.toString(), element.getId());
    }

    public void getSetProperty(String key, Object value) {
	if (value == null) {
	    throw ExceptionFactory.propertyValueCanNotBeNull();
	}

	if (key == null) {
	    throw ExceptionFactory.propertyKeyCanNotBeNull();
	}

	if (key.equals("")) {
	    throw ExceptionFactory.propertyKeyCanNotBeEmpty();
	}

	if (key.equals("id")) {
	    throw ExceptionFactory.propertyKeyIdIsReserved();
	}

	for (int i = 0 ; i < 100 ; i++) {
	    log.info("Before getIndexPropertyMap()");
	    Map<String, List<Object>> map = getIndexPropertyMap();
	    log.info("After getIndexPropertyMap()");
	    List<Object> values = new ArrayList<Object>();

	    if (map.containsKey(key)) {
		boolean found = false;
		for (Map.Entry<String, List<Object>> entry : map.entrySet()) {
		    if (entry.getKey().equals(key)) {
			values = entry.getValue();
			found = true;
			break;
		    }
		}
		if (found) {
		    if (!values.contains(value)) {
			values.add(value);
		    }
		}
	    } else {
		values.add(value);
	    }

	    map.put(key, values);

	    byte[] rcValue = setIndexPropertyMap(map);
	    if (rcValue.length != 0) {
		if (writeWithRules(rcValue)) {
		    break;
		} else {
		    log.info("getSetProperty(String key, Object value) cond. write  failure RETRYING " + (i+1));
		}
	    }
	}
    }

    @Override
    public CloseableIterable<T> get(String string, Object value) {
	return getIndexProperty(value.toString());
    }

    @Override
    public CloseableIterable<T> query(String string, Object o) {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long count(String key, Object value) {
	Map<String, List<Object>> map = getIndexPropertyMap();
	List<Object> values = map.get(value);
	if (null == values) {
	    return 0;
	} else {
	    return values.size();
	}
    }

    @Override
    public void remove(String key, Object value, T element) {
	if (value == null) {
	    throw ExceptionFactory.propertyValueCanNotBeNull();
	}

	if (key == null) {
	    throw ExceptionFactory.propertyKeyCanNotBeNull();
	}

	if (key.equals("")) {
	    throw ExceptionFactory.propertyKeyCanNotBeEmpty();
	}

	if (key.equals("id")) {
	    throw ExceptionFactory.propertyKeyIdIsReserved();
	}

       	for (int i = 0; i < 100; ++i) {
		Map<String, List<Object>> map = getIndexPropertyMap();

		if (map.isEmpty()) {
		    return;
		} else if (map.containsKey(value)) {
			List<Object> objects = map.get(value);
	    		if (null != objects) {
				objects.remove(element.getId());
				if (objects.isEmpty()) {
				    map.remove(value);
				}
			}
		}
		byte[] rcValue = setIndexPropertyMap(map);
		if (rcValue.length != 0) {
			if (writeWithRules(rcValue)) {
				break;
			} else {
				log.info("remove(String key, Object value, T element) write failure RETRYING" + (i + 1));
				// TODO ERROR message
			}
		}
	}

    }

    public void removeElement(T element) {
	JRamCloud.TableEnumerator tableEnum = graph.getRcClient().new TableEnumerator(graph.kidxVertTableId);

	JRamCloud.Object tableEntry;
	List<Object> values = new ArrayList<Object>();

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    // FIXME remove loop
	    for (int i = 0 ; i < 100 ; i++) {
		Map<String, List<Object>> propMap = getIndexPropertyMap(tableEntry.value);
		List<Map.Entry<String, List<Object>>> toRemove = new LinkedList<Map.Entry<String, List<Object>>>();

		for (Map.Entry<String, List<Object>> map : propMap.entrySet()) {
		    values = map.getValue();
		    values.remove(element.getId());
		    if (values.isEmpty()) {
			toRemove.add(map);
		    }
		}
		for (Map.Entry<String, List<Object>> map : toRemove ) {
		    propMap.remove(map.getKey());
		}

                byte[] rcValue = setIndexPropertyMap(propMap);
		if (rcValue.length != 0) {
		    if (writeWithRules(rcValue)) {
			break;
		    } else {
			log.info("write failure " + (i + 1));
			for (Map.Entry<String, List<Object>> map : toRemove ) {
				this.remove(indexName, map.getKey(), element);
			}
			return;
		    }
		}
	    }
	}
    }

    public Map<String, List<Object>> getIndexPropertyMap() {
	log.info("getIndexPropertyMap() ");
	JRamCloud.Object propTableEntry;

	try {
	    propTableEntry = graph.getRcClient().read(tableId, rcKey);
		log.info(toString() + ": getIndexPropertyMap() " + indexName + "Update version " + indexVersion + " -> " + propTableEntry.version );
	    indexVersion = propTableEntry.version;
	} catch (Exception e) {
	    indexVersion = 0;
	    log.info(e.toString() + " Element does not have a index property table entry! tableId :"+ tableId + " indexName : " + indexName );
	    return null;
	}

	return getIndexPropertyMap(propTableEntry.value);
    }

    public static Map<String, List<Object>> getIndexPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.error("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    try {
		ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Map<String, List<Object>> map = (Map<String, List<Object>>) ois.readObject();
		return map;
	    } catch (IOException e) {
		log.error("Got an IOException while deserializing element''s property map: {"+ e.toString() + "}");
		return null;
	    } catch (ClassNotFoundException e) {
		log.error("Got a ClassNotFoundException while deserializing element''s property map: {"+ e.toString() + "}");
		return null;
	    }
	} else {
	    return new HashMap<String, List<Object>>();
	}
    }

    public byte[] setIndexPropertyMap(Map<String, List<Object>> map) {
	byte[] rcValue = null;

	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectOutputStream oot = new ObjectOutputStream(baos);
	    oot.writeObject(map);
	    rcValue = baos.toByteArray();
	} catch (IOException e) {
	    log.info("Got an exception while serializing element''s property map: {"+ e.toString() + "}");
	}

	return rcValue;

    }

    private boolean writeWithRules(byte[] rcValue) {
	JRamCloud.RejectRules rules = graph.getRcClient().new RejectRules();

	if (indexVersion == 0) {
	    rules.setExists();
	} else {
	    rules.setNeVersion(indexVersion);
	}

	try {
	    graph.getRcClient().writeRule(tableId, rcKey, rcValue, rules);
	} catch (Exception e) {
	    log.info(toString() + ": Write index property: " + indexName + " " + e.toString() + " version " + indexVersion);
	    return false;
	}
    	return true;
    }

    public <T> T getIndexProperty(String key) {
	Map<String, List<Object>> map = getIndexPropertyMap();
	if ( map == null ) {
		log.error("IndexPropertyMap was null");
		return null;
	}
	return (T) map.get(key);
    }

    public Set<String> getIndexPropertyKeys() {
	Map<String, List<Object>> map = getIndexPropertyMap();
	return map.keySet();
    }

	public <T> T removeIndexProperty(String key) {
		for (int i = 0; i < 100; ++i) {
			Map<String, List<Object>> map = getIndexPropertyMap();
			T retVal = (T) map.remove(key);
			byte[] rcValue = setIndexPropertyMap(map);
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
	    log.info(toString() + ": Removing Index: " + indexName + " was version " + indexVersion);
	graph.getRcClient().remove(tableId, rcKey);
    }
}
