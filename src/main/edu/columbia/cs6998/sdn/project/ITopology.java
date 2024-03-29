package main.edu.columbia.cs6998.sdn.project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Niket Kandya on 12/10/13.
 */
public interface ITopology {
    short getNextHop(String ipAddress, String name);
}

class RouteRREntity {
    private List<FinalRoute> routes;
    private short currentInstance;

    RouteRREntity(List<FinalRoute> routes, short currentInstance) {
        this.routes = routes;
        this.currentInstance = currentInstance;
    }

    public List<FinalRoute> getRoutes() {
        return routes;
    }

    public short getCurrentInstance() {
        return currentInstance;
    }

    public void incrementInstance() {
        currentInstance = (short) ((currentInstance + 1) % routes.size());
    }

    @Override
    public String toString() {
        String retVal = "";
        int i = 0;
        for (FinalRoute route : routes) {
            retVal += "Route# " + i + "=\n" + route.toString();
        }
        return retVal;
    }
}

class FinalRoute {
    private ArrayList<Link> route;
    private int cost;

    FinalRoute() {
        route = new ArrayList<>();
    }

    public FinalRoute(FinalRoute tempRoute) {
        route = new ArrayList<>(tempRoute.getRoute());
        cost = tempRoute.getCost();
    }

    public ArrayList<Link> getRoute() {
        return route;
    }

    public int getCost() {
        return cost;
    }

    short getFirstHopPort() {
        if (!route.isEmpty())
            return route.get(1).getSrcPort();
        else
            return -1; //This should never happen.
    }

    String getFirstHopName() {
        if (!route.isEmpty())
            return route.get(0).getPair().getDstEndHost().getName();
        else
            return null; //This should never happen.
    }

    String getLastName() {
        if (!route.isEmpty())
            return route.get(route.size() - 1).getPair().getDstEndHost().getName();
        else
            return null; //This should never happen.
    }

    void append(Node node) {
        if(route.isEmpty()) {
            route.add(new Link(new NodeNodePair(null, node)));
        } else {
            Node dstEndHost = route.get(route.size() - 1).getPair().getDstEndHost();
            Link link = new Link(new NodeNodePair(dstEndHost, node));
            for (Map.Entry<Short, Link> entry : dstEndHost.getNeighbors().entrySet()) {
                if (entry.getValue().getPair().getDstEndHost().getName().equals(node.getName())) {
                    link.setSrcPort(entry.getKey());
                    link.setDstPort(entry.getValue().getDstPort());
                }
            }
            route.add(link);
        }
    }

    public void append(ArrayList<Link> param_route) {
        route.addAll(param_route);
    }

    @Override
    public String toString() {
        String retVal = "";
        for (Link link : route) {
            retVal += link.toString();
        }
        return retVal;
    }
}

class Link {
    private NodeNodePair pair;
    private short srcPort;
    private short dstPort;
    private Double cost;

    public Link(NodeNodePair nodeNodePair, Double cost, short srcPort, short dstPort) {
        pair = nodeNodePair;
        this.cost = cost;
        this.srcPort = srcPort;
        this.dstPort = dstPort;
    }

    public Link(NodeNodePair nodeNodePair, short srcPort, short dstPort) {
        this(nodeNodePair, 0.0, srcPort, dstPort);
    }

    public short getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(short srcPort) {
        this.srcPort = srcPort;
    }

    public short getDstPort() {
        return dstPort;
    }

    public void setDstPort(short dstPort) {
        this.dstPort = dstPort;
    }

    Link(NodeNodePair pair) {
        this(pair, 0.0, (short) 0, (short) 0);
    }

    public NodeNodePair getPair() {
        return pair;
    }

    public Double getCost() {
        return cost;
    }

    @Override
    public String toString() {
        String retVal = "(";
        Node srcNode = getPair().getSrcNode();
        Node dstEndHost = getPair().getDstEndHost();
        if(srcNode != null) {
            retVal += srcNode.getName() + "," + getSrcPort();
        }
        retVal += ")->(" + dstEndHost.getName() + "," + getDstPort() + ")";
        return retVal;
    }
}

class NodeNodePair {
    private Node srcNode;
    private Node dstEndHost;

    NodeNodePair(Node srcNode, Node dstEndHost) {
        this.srcNode = srcNode;
        this.dstEndHost = dstEndHost;
    }

    public Node getSrcNode() {
        return srcNode;
    }

    public Node getDstEndHost() {
        return dstEndHost;
    }

    @Override
    public String toString() {
        String retVal = "(";
        if (srcNode != null)
            retVal += srcNode.getName();
        retVal += "->";
        if (dstEndHost != null) 
        	retVal += dstEndHost.getName();
        retVal += ")";
        return retVal;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dstEndHost == null) ? 0 : dstEndHost.getName().hashCode());
		result = prime * result + ((srcNode == null) ? 0 : srcNode.getName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		NodeNodePair other = (NodeNodePair) obj;
		if (dstEndHost == null) {
			if (other.dstEndHost != null)
				return false;
		} else if (!dstEndHost.getName().equals(other.dstEndHost.getName()))
			return false;
		if (srcNode == null) {
			if (other.srcNode != null)
				return false;
		} else if (!srcNode.getName().equals(other.srcNode.getName()))
			return false;
		return true;
	}
}

class Node {
    //IP Address of the host
    
    String ipAddress;

    //Check if the node is an end hostflowMod
    Boolean isHost;

    //Check if the switch is a border switch
    Boolean isBorderSwitch;

    //List of ports this node has
    Map<Short, String> ports;

    //Map of port to neighboring nodes
    Map<Short, Link> neighbors;

    String name;

    Node() {
        ports = new HashMap<>();
        neighbors = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public Boolean getIsHost() {
        return isHost;
    }

    public Boolean getIsBorderSwitch() {
        return isBorderSwitch;
    }

    public Map<Short, String> getPorts() {
        return ports;
    }

    public Map<Short, Link> getNeighbors() {
        return neighbors;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addPort(short i, String macAddress) {
        ports.put(i, macAddress);
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setIsHost(Boolean isHost) {
        this.isHost = isHost;
    }

    public void addNeighbor(short port, Link link) {
        neighbors.put(port, link);
    }

    @Override
    public String toString() {
        String retval;
        retval = name + " ->\n";
        for (Map.Entry<Short, Link> entry : getNeighbors().entrySet()) {
            retval += "(" + entry.getKey() + "," +
                    entry.getValue().getPair().getSrcNode().getName() + "-" +
                    entry.getValue().getPair().getDstEndHost().getName() + ")";
        }
        retval += "\n";
        return retval;
    }
}