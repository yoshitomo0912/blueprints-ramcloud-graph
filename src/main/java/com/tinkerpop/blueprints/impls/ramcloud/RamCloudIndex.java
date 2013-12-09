package com.tinkerpop.blueprints.impls.ramcloud;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.WrappingCloseableIterable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

import edu.stanford.ramcloud.JRamCloud;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RamCloudIndex<T extends Element> implements Index<T>, Serializable {

    private static final Logger logger = Logger.getLogger(RamCloudGraph.class.getName());
    protected byte[] rcKey;
    private RamCloudGraph graph;
    private long tableId;
    private String indexName;
    private Class<T> indexClass;

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
	    graph.getRcClient().read(tableId, rcKey);
	    return true;
	} catch (Exception e) {
	    return false;
	}
    }

    public void create() throws IllegalArgumentException {
	if (!exists()) {
	    graph.getRcClient().write(tableId, rcKey, ByteBuffer.allocate(0).array());

	} else {
	    
	    //throw ExceptionFactory.vertexWithIdAlreadyExists(rcKey);
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

	Map<String, List<Object>> map = getIndexPropertyMap();
	List<Object> values = new ArrayList<Object>();

	System.out.println("map size1 : " + map.size());
	
	if (map.containsKey(key)) {
	    for (Map.Entry<String, List<Object>> entry : map.entrySet()) {
		if (!entry.getKey().equals(key)) {
		    continue;
		}
		values = entry.getValue();
		System.out.println("values : " + values);
		if (!values.contains(value)) {
		    values.add(value);
		}
		break;
	    }
	} else {
	    values.add(value);
	}
	
	map.put(key, values);

	System.out.println("map size2 : " + map.size());
	
	setIndexPropertyMap(map);
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
	setIndexPropertyMap(map);

    }

    public void removeElement(Object element) {
	JRamCloud.TableEnumerator tableEnum = graph.getRcClient().new TableEnumerator(tableId);

	JRamCloud.Object tableEntry;
	List<Object> values = new ArrayList<Object>();

	while (tableEnum.hasNext()) {
	    tableEntry = tableEnum.next();
	    Map<String, List<Object>> propMap = getIndexPropertyMap(tableEntry.value);
	    for (Map.Entry<String, List<Object>> map : propMap.entrySet()) {
		values = map.getValue();
		values.remove(element);
		propMap.put(map.getKey(), values);
	    }
	}
    }

    public Map<String, List<Object>> getIndexPropertyMap() {
	JRamCloud.Object propTableEntry;

	try {
	    propTableEntry = graph.getRcClient().read(tableId, rcKey);
	} catch (Exception e) {
	    logger.log(Level.WARNING, "Element does not have a property table entry!");
	    return null;
	}

	return getIndexPropertyMap(propTableEntry.value);
    }

    public static Map<String, List<Object>> getIndexPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    logger.log(Level.WARNING, "Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    try {
		ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Map<String, List<Object>> map = (Map<String, List<Object>>) ois.readObject();
		return map;
	    } catch (IOException e) {
		logger.log(Level.WARNING, "Got an exception while deserializing element''s property map: {0}", e.toString());
		return null;
	    } catch (ClassNotFoundException e) {
		logger.log(Level.WARNING, "Got an exception while deserializing element''s property map: {0}", e.toString());
		return null;
	    }
	} else {
	    return new ConcurrentHashMap<String, List<Object>>();
	}
    }

    public void setIndexPropertyMap(Map<String, List<Object>> map) {
	byte[] rcValue;

	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectOutputStream oot = new ObjectOutputStream(baos);
	    oot.writeObject(map);
	    rcValue = baos.toByteArray();
	} catch (IOException e) {
	    logger.log(Level.WARNING, "Got an exception while serializing element''s property map: {0}", e.toString());
	    return;
	}
	graph.getRcClient().write(tableId, rcKey, rcValue);
    }

    public <T> T getIndexProperty(String key) {
	Map<String, List<Object>> map = getIndexPropertyMap();
	return (T) map.get(key);
    }

    public Set<String> getIndexPropertyKeys() {
	Map<String, List<Object>> map = getIndexPropertyMap();
	return map.keySet();
    }

    public <T> T removeIndexProperty(String key) {
	Map<String, List<Object>> map = getIndexPropertyMap();
	T retVal = (T) map.remove(key);
	setIndexPropertyMap(map);
	return retVal;
    }

    public void removeIndex() {
	graph.getRcClient().remove(tableId, rcKey);
    }
}
