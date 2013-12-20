package com.tinkerpop.blueprints.impls.ramcloud;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraphProtos.EdgeListProtoBuf;
import com.tinkerpop.blueprints.impls.ramcloud.RamCloudGraphProtos.EdgeProtoBuf;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.stanford.ramcloud.JRamCloud;
import edu.stanford.ramcloud.JRamCloud.WrongVersionException;

public class RamCloudVertex extends RamCloudElement implements Vertex, Serializable {

	private final static Logger log = LoggerFactory.getLogger(RamCloudGraph.class);
	private static final long serialVersionUID = 7526472295622776147L;
	protected long id;
	protected byte[] rcKey;
	private RamCloudGraph graph;

	private Versioned<EdgeListProtoBuf> cachedAdjEdgeList;

	public RamCloudVertex(long id, RamCloudGraph graph) {
		super(idToRcKey(id), graph.vertPropTableId, graph.getRcClient(), graph);

		this.id = id;
		this.rcKey = idToRcKey(id);
		this.graph = graph;
		cachedAdjEdgeList = new Versioned<EdgeListProtoBuf>(EdgeListProtoBuf.newBuilder().build());
	}

	public RamCloudVertex(byte[] rcKey, RamCloudGraph graph) {
		super(rcKey, graph.vertPropTableId, graph.getRcClient(), graph);

		this.id = rcKeyToId(rcKey);
		this.rcKey = rcKey;
		this.graph = graph;
		cachedAdjEdgeList = new Versioned<EdgeListProtoBuf>(EdgeListProtoBuf.newBuilder().build());
	}

	/*
	 * Vertex interface implementation
	 */
	@Override
	public Edge addEdge(String label, Vertex inVertex) {
		return graph.addEdge(null, this, inVertex, label);
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		return new ArrayList<Edge>(getEdgeList(direction, labels));
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		List<RamCloudEdge> edges = getEdgeList(direction, labels);
		List<Vertex> neighbors = new LinkedList<Vertex>();
		for (RamCloudEdge edge : edges) {
			neighbors.add(edge.getNeighbor(this));
		}
		return neighbors;
	}

	@Override
	public VertexQuery query() {
		return new DefaultVertexQuery(this);
	}

	/*
	 * RamCloudElement overridden methods
	 */
	@Override
	public Object getId() {
		return id;
	}

	@Override
	public void remove() {
		Set<RamCloudEdge> edges = getEdgeSet();

		// neighbor vertex -> List of Edges to remove
		Map<RamCloudVertex, List<RamCloudEdge>> vertexToEdgesMap = new HashMap<RamCloudVertex, List<RamCloudEdge>>( edges.size() );

		// Batch edges together by neighbor vertex
		for (RamCloudEdge edge : edges) {
			RamCloudVertex neighbor = (RamCloudVertex) edge.getNeighbor(this);
			List<RamCloudEdge> edgeList = vertexToEdgesMap.get(neighbor);

			if (edgeList == null) {
				edgeList = new LinkedList<RamCloudEdge>();
			}

			edgeList.add(edge);
			vertexToEdgesMap.put(neighbor, edgeList);
		}

		// Remove batches of edges at a time by neighbor vertex
		for (Entry<RamCloudVertex, List<RamCloudEdge>> entry : vertexToEdgesMap.entrySet()) {
			// Skip over loopback edges to ourself
			if (!entry.getKey().equals(this)) {
				entry.getKey().removeEdgesFromAdjList(entry.getValue());
			}

			// Remove this batch of edges from the edge property table
			for (RamCloudEdge edge : entry.getValue()) {
				edge.removeProperties();
			}
		}

		Map<String,Object> props = this.getPropertyMap();
		for( Map.Entry<String,Object> entry : props.entrySet() ) {
			if ( !graph.indexedKeys.contains(entry.getKey() ) ) continue;
			RamCloudKeyIndex keyIndex = new RamCloudKeyIndex(graph.kidxVertTableId, entry.getKey(), entry.getValue(), graph, Vertex.class);
			keyIndex.remove(entry.getKey(), entry.getValue(), this);
		}

		// Remove ourselves entirely from the vertex table
		graph.getRcClient().remove(graph.vertTableId, rcKey);

		super.remove();
	}

	/*
	 * Object overridden methods
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		RamCloudVertex other = (RamCloudVertex) obj;
		return (id == other.id);
	}

	@Override
	public int hashCode() {
		return Long.valueOf(id).hashCode();
	}

	@Override
	public String toString() {
		return "RamCloudVertex [id=" + id + "]";
	}

	/*
	 * RamCloudVertex specific methods
	 */
	private static byte[] idToRcKey(long id) {
		return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(id).array();
	}

	private static long rcKeyToId(byte[] rcKey) {
		return ByteBuffer.wrap(rcKey).order(ByteOrder.LITTLE_ENDIAN).getLong();
	}

	void addEdgeToAdjList(RamCloudEdge edge) {
		List<RamCloudEdge> edgesToAdd = new ArrayList<RamCloudEdge>(1);
		edgesToAdd.add(edge);
		addEdgesToAdjList(edgesToAdd);
	}
	void removeEdgeFromAdjList(RamCloudEdge edge) {
		List<RamCloudEdge> edgesToRemove = new ArrayList<RamCloudEdge>(1);
		edgesToRemove.add(edge);
		removeEdgesFromAdjList(edgesToRemove);
	}

	private void addEdgesToAdjList(List<RamCloudEdge> edgesToAdd) {
		updateEdgeAdjList(edgesToAdd, true);
	}
	private void removeEdgesFromAdjList(List<RamCloudEdge> edgesToAdd) {
		updateEdgeAdjList(edgesToAdd, false);
	}

	/** Conditionally update Adj. Edge List
	 * */
	private void updateEdgeAdjList(List<RamCloudEdge> edgesToModify, boolean add) {
		final int MAX_RETRIES = 100;
		for (int retry = 1 ; retry <= MAX_RETRIES ; ++retry ) {

			long expected_version = this.cachedAdjEdgeList.getVersion();
			if ( expected_version == 0L && add == false ) {
				updateCachedAdjEdgeList();
				expected_version = this.cachedAdjEdgeList.getVersion();
			}
			Set<RamCloudEdge> edges = buildEdgeSetFromProtobuf(this.cachedAdjEdgeList.getValue(), Direction.BOTH);
			//log.debug( (add?"Adding":"Removing") + " edges to: {"+ edges+ "}");

			try {
				if ( add ) {
					if (edges.addAll(edgesToModify) == false) {
						log.warn("{" + toString() + "}: There aren't any changes to edges ({" + edgesToModify.toString() + "})");
						return;
					}
				} else {
					if (edges.removeAll(edgesToModify) == false) {
						log.warn("{" + toString() + "}: There aren't any changes to edges ({" + edgesToModify.toString() + "})");
						return;
					}
				}

				EdgeListProtoBuf edgeList = buildProtoBufFromEdgeSet(edges);
				JRamCloud.RejectRules rules = graph.getRcClient().new RejectRules();
				if ( expected_version == 0L ) {
					rules.setExists();
				} else {
					rules.setNeVersion(expected_version);
				}
				long updated_version = graph.getRcClient().writeRule(graph.vertTableId, rcKey, edgeList.toByteArray(), rules);
				this.cachedAdjEdgeList.setValue(edgeList, updated_version);
				return;
			} catch (UnsupportedOperationException e) {
				log.error("{" + toString() + "}: Failed to modify a set of edges ({" + edgesToModify.toString() + "}): {" + e.toString() + "}");
				return;
			} catch (ClassCastException e) {
				log.error("{" + toString() + "}: Failed to modify a set of edges ({" + edgesToModify.toString() + "}): {" + e.toString() + "}");
				return;
			} catch (NullPointerException e) {
				log.error("{" + toString() + "}: Failed to modify a set of edges ({" + edgesToModify.toString() + "}): {" + e.toString() + "}");
				return;
			} catch (Exception e) {
				// FIXME Workaround for native method exception declaration bug
				if ( e instanceof WrongVersionException ) {
					log.debug("Conditional Updating EdgeList failed for {} modifing {} Retrying [{}]", this, edgesToModify, retry);
					updateCachedAdjEdgeList();
				} else {
					log.debug("Cond. Write to modify adj edge list failed, exception thrown {}", e);
					updateCachedAdjEdgeList();
				}
			}
		}
	}

	/** Get all adj.edge list
	 * Method is exposed to package namespace to do Vertex removal efficiently;
	 */
	Set<RamCloudEdge> getEdgeSet() {
		return getVersionedEdgeSet(Direction.BOTH).getValue();
	}

	private Versioned<EdgeListProtoBuf> updateCachedAdjEdgeList() {
		JRamCloud.Object vertTableEntry;
		EdgeListProtoBuf edgeList;

		try {
			vertTableEntry = graph.getRcClient().read(graph.vertTableId, rcKey);
		} catch (Exception e) {
			log.error("{" + toString() + "}: Error reading vertex table entry: {" + e.toString() + "}");
			return null;
		}

		try {
			edgeList = EdgeListProtoBuf.parseFrom(vertTableEntry.value);
			Versioned<EdgeListProtoBuf> updatedEdgeList = new Versioned<EdgeListProtoBuf>(edgeList, vertTableEntry.version);
			this.cachedAdjEdgeList = updatedEdgeList;
			return updatedEdgeList;
		} catch (InvalidProtocolBufferException e) {
			log.error("{" + toString() + "}: Read malformed edge list: {" + e.toString() + "}");
			return null;
		}
	}

	private Versioned<Set<RamCloudEdge>> getVersionedEdgeSet(Direction direction, String... labels) {
		Versioned<EdgeListProtoBuf> cachedEdgeList = updateCachedAdjEdgeList();
		return new Versioned<Set<RamCloudEdge>>(buildEdgeSetFromProtobuf(cachedEdgeList.getValue(), direction, labels), cachedEdgeList.getVersion() );
	}

	private Set<RamCloudEdge> buildEdgeSetFromProtobuf(EdgeListProtoBuf edgeList,
			Direction direction, String... labels) {
		long startTime = System.nanoTime();
		Set<RamCloudEdge> edgeSet = new HashSet<RamCloudEdge>( edgeList.getEdgeCount() );
		for (EdgeProtoBuf edge : edgeList.getEdgeList()) {
			if ((direction.equals(Direction.BOTH) || (edge.getOutgoing() ^ direction.equals(Direction.IN)))
					&& (labels.length == 0 || Arrays.asList(labels).contains(edge.getLabel()))) {
				RamCloudVertex  neighbor = new RamCloudVertex(edge.getNeighborId(), graph);
				if (edge.getOutgoing()) {
					edgeSet.add(new RamCloudEdge(this, neighbor, edge.getLabel(), graph));
				} else {
					edgeSet.add(new RamCloudEdge(neighbor, this, edge.getLabel(), graph));
				}
			}
		}

                long endTime = System.nanoTime();
                log.error("Performance buildEdgeSetFromProtobuf key {}, {}, size={}", this.toString(), endTime - startTime, edgeList.getSerializedSize());
		return edgeSet;
	}



	private EdgeListProtoBuf buildProtoBufFromEdgeSet(Set<RamCloudEdge> edgeSet) {
		long startTime = System.nanoTime();

		EdgeListProtoBuf.Builder edgeListBuilder = EdgeListProtoBuf.newBuilder();
		EdgeProtoBuf.Builder edgeBuilder = EdgeProtoBuf.newBuilder();

		for (Edge edge : edgeSet) {
			if (edge.getVertex(Direction.OUT).equals(this) || edge.getVertex(Direction.IN).equals(this)) {
				if (edge.getVertex(Direction.OUT).equals(edge.getVertex(Direction.IN))) {
					edgeBuilder.setNeighborId(id);
					edgeBuilder.setOutgoing(true);
					edgeBuilder.setLabel(edge.getLabel());
					edgeListBuilder.addEdge(edgeBuilder.build());

					edgeBuilder.setOutgoing(false);
					edgeListBuilder.addEdge(edgeBuilder.build());
				} else {
					if (edge.getVertex(Direction.OUT).equals(this)) {
						edgeBuilder.setNeighborId((Long) edge.getVertex(Direction.IN).getId());
						edgeBuilder.setOutgoing(true);
						edgeBuilder.setLabel(edge.getLabel());
						edgeListBuilder.addEdge(edgeBuilder.build());
					} else {
						edgeBuilder.setNeighborId((Long) edge.getVertex(Direction.OUT).getId());
						edgeBuilder.setOutgoing(false);
						edgeBuilder.setLabel(edge.getLabel());
						edgeListBuilder.addEdge(edgeBuilder.build());
					}
				}
			} else {
				log.warn("{" + toString() + "}: Tried to add an edge unowned by this vertex ({" + edge.toString() + "})");
			}
		}

		EdgeListProtoBuf buf = edgeListBuilder.build();
                long endTime = System.nanoTime();
                log.error("Performance buildProtoBufFromEdgeSet key {}, {}, size={}", this.toString(), endTime - startTime, buf.getSerializedSize());
		return buf;
	}

	@Deprecated
	private List<RamCloudEdge> getEdgeList() {
		return getEdgeList(Direction.BOTH);
	}

	private List<RamCloudEdge> getEdgeList(Direction direction, String... labels) {

		Versioned<EdgeListProtoBuf> cachedEdgeList = updateCachedAdjEdgeList();
		List<RamCloudEdge> edgeList = new ArrayList<RamCloudEdge>(cachedEdgeList.getValue().getEdgeCount());

		for (EdgeProtoBuf edge : cachedEdgeList.getValue().getEdgeList()) {
			if ((direction.equals(Direction.BOTH) || (edge.getOutgoing() ^ direction.equals(Direction.IN)))
					&& (labels.length == 0 || Arrays.asList(labels).contains(edge.getLabel()))) {
				RamCloudVertex neighbor = new RamCloudVertex(edge.getNeighborId(), graph);
				if (edge.getOutgoing()) {
					edgeList.add(new RamCloudEdge(this, neighbor, edge.getLabel(), graph));
				} else {
					edgeList.add(new RamCloudEdge(neighbor, this, edge.getLabel(), graph));
				}
			}
		}

		return edgeList;
	}

	protected boolean exists() {
		boolean vertTableEntryExists = false;
		boolean vertPropTableEntryExists = false;
		
		long startTime = 0;
		    
		if (graph.measureRcTimeProp == 1) { 
		    startTime = System.nanoTime();
		}

		try {
		        JRamCloud vertTable = graph.getRcClient();
			if (graph.measureRcTimeProp == 1) {
			    startTime = System.nanoTime();
			}
			vertTable.read(graph.vertTableId, rcKey);
			if (graph.measureRcTimeProp == 1) {
			    long endTime = System.nanoTime();
			    log.error("Performance vertexTable exists read total time {}", endTime - startTime);
			}
			vertTableEntryExists = true;
		} catch (Exception e) {
			// Vertex table entry does not exist
		    if (graph.measureRcTimeProp == 1) {
			long endTime = System.nanoTime();
			log.error("Performance vertexTable does not exists read total time {}", endTime - startTime);
		    }
		}

		try {
		        JRamCloud vertTable = graph.getRcClient();
			if (graph.measureRcTimeProp == 1) {
			    startTime = System.nanoTime();
			}
			vertTable.read(graph.vertPropTableId, rcKey);
			if (graph.measureRcTimeProp == 1) {
			    long endTime = System.nanoTime();
			    log.error("Performance vertexPropTable exists read total time {}", endTime - startTime);
			}
			vertPropTableEntryExists = true;
		} catch (Exception e) {
			// Vertex property table entry does not exist
		    if (graph.measureRcTimeProp == 1) {
			long endTime = System.nanoTime();
			log.error("Performance vertexPropTable does not exists read total time {}", endTime - startTime);
		    }
		}

		if (vertTableEntryExists && vertPropTableEntryExists) {
			return true;
		} else if (!vertTableEntryExists && !vertPropTableEntryExists) {
			return false;
		} else {
			log.warn("{" + toString() + "}: Detected RamCloudGraph inconsistency: vertTableEntryExists={" + vertTableEntryExists + "}, vertPropTableEntryExists={" + vertPropTableEntryExists + "}.");
			return true;
		}
	}

	protected void create() throws IllegalArgumentException {
		// TODO: Existence check costs extra (presently 2 reads), could use option to turn on/off
		if (!exists()) {
			JRamCloud vertTable = graph.getRcClient();
			long startTime = 0;
			if (graph.measureRcTimeProp == 1) {
			    startTime = System.nanoTime();
			}
			vertTable.write(graph.vertTableId, rcKey, ByteBuffer.allocate(0).array());
			vertTable.write(graph.vertPropTableId, rcKey, ByteBuffer.allocate(0).array());
			if (graph.measureRcTimeProp == 1) {
			    long endTime = System.nanoTime();
			    log.error("Performance vertex/vertexPropTable initial total time {}", endTime - startTime);
			}
		} else {
			throw ExceptionFactory.vertexWithIdAlreadyExists(id);
		}
	}

	public void debugPrintEdgeList() {
		List<RamCloudEdge> edgeList = getEdgeList();

		log.debug(toString() + ": Debug Printing Edge List...");
		for (RamCloudEdge edge : edgeList) {
			System.out.println(edge.toString());
		}
	}
}
