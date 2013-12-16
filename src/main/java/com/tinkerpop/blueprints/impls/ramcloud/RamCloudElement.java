package com.tinkerpop.blueprints.impls.ramcloud;

import com.tinkerpop.blueprints.Edge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraph.RamCloudKeyIndex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RamCloudElement implements Element, Serializable {

    private final static Logger log = LoggerFactory.getLogger(RamCloudGraph.class);
    private byte[] rcPropTableKey;
    private long rcPropTableId;
    private RamCloudGraph graph;

    public RamCloudElement() {
    }

    public RamCloudElement(byte[] rcPropTableKey, long rcPropTableId, JRamCloud rcClient, RamCloudGraph graph) {
	this.rcPropTableKey = rcPropTableKey;
	this.rcPropTableId = rcPropTableId;
	this.graph = graph;
    }

    public Map<String, Object> getPropertyMap() {
	JRamCloud.Object propTableEntry;

	try {
	    propTableEntry = graph.getRcClient().read(rcPropTableId, rcPropTableKey);
	    if( propTableEntry.value.length > 1024*1024*0.9 ) {
	    	log.warn("Element[id={}] property map size is near 1MB limit!", new String(rcPropTableKey) );
	    }
	} catch (Exception e) {
	    log.warn("Element does not have a property table entry!");
	    return null;
	}

	return getPropertyMap(propTableEntry.value);
    }

    public static Map<String, Object> getPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.warn("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    try {
		ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Map<String, Object> map = (Map<String, Object>) ois.readObject();
		return map;
	    } catch (IOException e) {
		log.error("Got an exception while deserializing element's property map: {" + e.toString() + "}");
		return null;
	    } catch (ClassNotFoundException e) {
		log.error("Got an exception while deserializing element's property map: {" + e.toString() + "}");
		return null;
	    }
	} else {
	    return new HashMap<String, Object>();
	}
    }

    public void setPropertyMap(Map<String, Object> map) {
	byte[] rcValue;

	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectOutputStream oot = new ObjectOutputStream(baos);
	    oot.writeObject(map);
	    rcValue = baos.toByteArray();
	} catch (IOException e) {
	    log.error("Got an exception while serializing element''s property map: {" + e.toString() + "}");
	    return;
	}

	graph.getRcClient().write(rcPropTableId, rcPropTableKey, rcValue);
    }

    @Override
    public <T> T getProperty(String key) {
	Map<String, Object> map = getPropertyMap();
	return (T) map.get(key);
    }

    @Override
    public Set<String> getPropertyKeys() {
	Map<String, Object> map = getPropertyMap();
	return map.keySet();
    }

    @Override
    public void setProperty(String key, Object value) {
	Object oldValue;
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

	if (this instanceof RamCloudEdge && key.equals("label")) {
	    throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
	}

	Map<String, Object> map = getPropertyMap();
	oldValue = map.put(key, value);
	setPropertyMap(map);

	if (this instanceof RamCloudVertex) {
		RamCloudKeyIndex keyIndex = graph.getIndexedKeys(key, Vertex.class);
		keyIndex.autoUpdate(key, value, oldValue, this);
	} else {
		RamCloudKeyIndex keyIndex =graph.getIndexedKeys(key, Edge.class);
		keyIndex.autoUpdate(key, value, oldValue, this);
	}
    }

    @Override
    public <T> T removeProperty(String key) {
	Map<String, Object> map = getPropertyMap();
	T retVal = (T) map.remove(key);
	setPropertyMap(map);

	if (this instanceof RamCloudVertex) {
		RamCloudKeyIndex keyIndex = graph.getIndexedKeys(key, Vertex.class);
		keyIndex.autoRemove(key, retVal.toString(), this);
	} else {
		RamCloudKeyIndex keyIndex = graph.getIndexedKeys(key, Edge.class);
		keyIndex.autoRemove(key, retVal.toString(), this);
	}

	return retVal;
    }

    @Override
    public void remove() {
	graph.getRcClient().remove(rcPropTableId, rcPropTableKey);
    }

    @Override
    public Object getId() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public String toString() {
	return "RamCloudElement [rcPropTableKey=" + Arrays.toString(rcPropTableKey)
		+ ", rcPropTableId=" + rcPropTableId + "]";
    }
}
