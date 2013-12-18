package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo2.Kryo;
import com.esotericsoftware.kryo2.io.ByteBufferInput;
import com.esotericsoftware.kryo2.io.ByteBufferOutput;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;

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

    protected Map<String, Object> getPropertyMap() {
	JRamCloud.Object propTableEntry;

	try {
	    propTableEntry = graph.getRcClient().read(rcPropTableId, rcPropTableKey);
	    if (propTableEntry.value.length > 1024 * 1024 * 0.9) {
		log.warn("Element[id={}] property map size is near 1MB limit!", new String(rcPropTableKey));
	    }
	} catch (Exception e) {
	    log.warn("Element does not have a property table entry!");
	    return null;
	}

	return convertRcBytesToPropertyMap(propTableEntry.value);
    }

    public static Map<String, Object> convertRcBytesToPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.warn("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    Kryo kryo = new Kryo();
	    ByteBufferInput input = new ByteBufferInput(byteArray);
	    TreeMap map = kryo.readObject(input, TreeMap.class);
	    return map;
	} else {
	    return new TreeMap<String, Object>();
	}
    }

    private void setPropertyMap(Map<String, Object> map) {
	byte[] rcValue;

	Kryo kryo = new Kryo();
	ByteBufferOutput output = new ByteBufferOutput(1024 * 1024);
	kryo.writeObject(output, map);
	output.flush();
	rcValue = output.toBytes();
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
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, value, graph, Vertex.class);
	    keyIndex.autoUpdate(key, value, oldValue, this);
	} else {
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, value, graph, Edge.class);
	    keyIndex.autoUpdate(key, value, oldValue, this);
	}
    }

    @Override
    public <T> T removeProperty(String key) {
	Map<String, Object> map = getPropertyMap();
	T retVal = (T) map.remove(key);
	setPropertyMap(map);

	if (this instanceof RamCloudVertex) {
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, retVal, graph, Vertex.class);
	    keyIndex.autoRemove(key, retVal.toString(), this);
	} else {
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, retVal, graph, Edge.class);
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
