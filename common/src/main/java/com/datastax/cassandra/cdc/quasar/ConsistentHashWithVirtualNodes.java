package com.datastax.cassandra.cdc.quasar;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consistent hash with virtual nodes,
 * see https://programmer.help/blogs/consistency-hash-algorithm-principle-and-java-implementation.html
 */
public class ConsistentHashWithVirtualNodes
{
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashWithVirtualNodes.class);

    /**
     * Number of virtual nodes per real node.
     */
    final int numberOfVirtualNodes;

    //Virtual node, key represents the hash value of virtual node, value represents the name of virtual node
    SortedMap<Integer, String> virtualNodes;

    // locks for thread-safety
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    public ConsistentHashWithVirtualNodes(int numberOfNode) {
        this(16, numberOfNode);
    }

    public ConsistentHashWithVirtualNodes(int numberOfVirtualNodes, int numberOfNodes) {
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.virtualNodes = computeVirtualNodes(numberOfVirtualNodes, numberOfNodes);
    }

    public void updateNumberOfNode(int numberOfNodes) {
        w.lock();
        try {
            this.virtualNodes = computeVirtualNodes(this.numberOfVirtualNodes, numberOfNodes);
        }
        finally
        {
            w.unlock();
        }

    }

    TreeMap<Integer, String> computeVirtualNodes(int numberOfVirtualNode, int numberOfNodes) {
        TreeMap<Integer, String> virtualNodes = new TreeMap<>();
        //Adding virtual nodes makes traversing LinkedList more efficient using foreach loops
        for (int n = 0; n < numberOfNodes; n++){
            for(int i=0; i < numberOfVirtualNode; i++){
                String virtualNodeName = n + ":" + i;
                int hash = Murmur3HashFunction.hash(virtualNodeName);
                logger.debug("virtualNode={} hash={}", virtualNodeName, hash);
                virtualNodes.put(hash, virtualNodeName);
            }
        }
        return virtualNodes;
    }

    public int getOrdinal(int hash) {
        r.lock();
        try
        {
            // Get all Map s that are larger than the Hash value
            SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);
            String virtualNode;
            if (subMap.isEmpty())
            {
                //If there is no one larger than the hash value of the key, start with the first node
                Integer i = virtualNodes.firstKey();
                //Return to the corresponding server
                virtualNode = virtualNodes.get(i);
            }
            else
            {
                //The first Key is the nearest node clockwise past the node.
                Integer i = subMap.firstKey();
                //Return to the corresponding server
                virtualNode = subMap.get(i);
            }
            //The virtual Node virtual node name needs to be intercepted
            if (virtualNode != null && virtualNode.length() > 0)
            {
                logger.debug("virtualNode={} for hash={}", virtualNode, hash);
                return Integer.parseInt(virtualNode.substring(0, virtualNode.indexOf(":")));
            }
            throw new IllegalStateException("virtualNode not found for hash=" + hash);
        }
        finally
        {
            r.unlock();
        }
    }
}
