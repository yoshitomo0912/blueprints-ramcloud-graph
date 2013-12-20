package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.Serializable;
import java.util.ArrayList;
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

    private static final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
                 Kryo kryo = new Kryo();
                 kryo.setRegistrationRequired(true);
                 kryo.register(String.class);
                 kryo.register(Long.class);
                 kryo.register(Integer.class);
                 kryo.register(Short.class);
                 kryo.register(Byte.class);
                 kryo.register(TreeMap.class);
                 kryo.register(ArrayList.class);
                 kryo.setReferences(false);
                 return kryo;
        }
    };

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
	    JRamCloud vertTable = graph.getRcClient();
	    long startTime = 0;
	    if (graph.measureRcTimeProp == 1) {
		startTime = System.nanoTime();
	    }
	    propTableEntry = vertTable.read(rcPropTableId, rcPropTableKey);
	    if (graph.measureRcTimeProp == 1) {
		long endTime = System.nanoTime();
		log.error("Performance getPropertyMap read time {}", endTime - startTime);
	    }
	    if (propTableEntry.value.length > 1024 * 1024 * 0.9) {
		log.warn("Element[id={}] property map size is near 1MB limit!", new String(rcPropTableKey));
	    }
	} catch (Exception e) {
	    log.warn("Element does not have a property table entry!");
	    return null;
	}

	return convertRcBytesToPropertyMap(propTableEntry.value);
    }

    public static Map<String, Object> convertRcBytesToPropertyMapEx(byte[] byteArray) {
	if (byteArray == null) {
	    log.warn("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
	    ByteBufferInput input = new ByteBufferInput(byteArray);
	    TreeMap map = kryo.get().readObject(input, TreeMap.class);
	    return map;
	} else {
	    return new TreeMap<String, Object>();
	}
    }

    public Map<String, Object> convertRcBytesToPropertyMap(byte[] byteArray) {
	if (byteArray == null) {
	    log.warn("Got a null byteArray argument");
	    return null;
	} else if (byteArray.length != 0) {
            long startTime = System.nanoTime();
	    ByteBufferInput input = new ByteBufferInput(byteArray);
	    TreeMap map = kryo.get().readObject(input, TreeMap.class);
            long endTime = System.nanoTime();
            log.error("Performance element kryo deserialization key {} {} size {}", this.toString(), endTime - startTime, byteArray.length);
	    return map;
	} else {
	    return new TreeMap<String, Object>();
	}
    }

    private void setPropertyMap(Map<String, Object> map) {
	byte[] rcValue;

        long startKryoTime = System.nanoTime();
	ByteBufferOutput output = new ByteBufferOutput(1024 * 1024);
	kryo.get().writeObject(output, map);
        long midKryoTime = System.nanoTime();
	rcValue = output.toBytes();
        long endKryoTime = System.nanoTime();
        log.error("Performance element kryo serialization key {} mid {}, total {}, size {}", this.toString(), midKryoTime - startKryoTime, endKryoTime - startKryoTime, rcValue.length);
	
	long startTime = 0;
	JRamCloud vertTable = graph.getRcClient();
	if (graph.measureRcTimeProp == 1) {
	    startTime = System.nanoTime();
	}
	vertTable.write(rcPropTableId, rcPropTableKey, rcValue);
	if (graph.measureRcTimeProp == 1) {
	    long endTime = System.nanoTime();
	    log.error("Performance setPropertyMap write time key {} {}", this.toString(), endTime - startTime);
	}
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

	long startTime = 0;
	if (graph.measureBPTimeProp == 1) { 
	    startTime = System.nanoTime();
	}
	
	Map<String, Object> map = getPropertyMap();
	oldValue = map.put(key, value);
	setPropertyMap(map);

	boolean ret = false;
	if (this instanceof RamCloudVertex) {
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, value, graph, Vertex.class);
	    ret = keyIndex.autoUpdate(key, value, oldValue, this);
	} else {
	    RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, key, value, graph, Edge.class);
	    keyIndex.autoUpdate(key, value, oldValue, this);
	}
	
	if (graph.measureBPTimeProp == 1) {
	    long endTime = System.nanoTime();
	    if (ret) {
		log.error("Performance vertex setProperty(key {}) which is index total time {}", key, endTime - startTime);
	    } else {
		log.error("Performance vertex setProperty(key {}) does not index time {}", key, endTime - startTime);
	    }
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
