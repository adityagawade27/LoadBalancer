package main.edu.columbia.cs6998.sdn.project;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Niket Kandya on 12/10/13.
 */
public interface ITopology {
    short getNextHop(String ipAddress, Node srcNode);
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

    short getFirstHop() {
        if (!route.isEmpty())
            return route.get(0).getDstPort();
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
            route.add(new Link(new NodeNodePair(dstEndHost, node)));
        }
    }

    public void append(ArrayList<Link> param_route) {
        route.addAll(param_route);
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
}

class Node {
    //IP Address of the host
    
    String ipAddress;

    //Mac address of the host
    String macAddress;

    //Check if the node is an end hostflowMod
    Boolean isHost;

    //Check if the switch is a border switch
    Boolean isBorderSwitch;

    //List of ports this node has
    List<Short> ports;

    //Map of port to neighboring nodes
    Map<Short, Link> neighbors;

    String name;

    private String IPAddress;

    Node() {
        ports = new ArrayList<>();
        neighbors = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public Boolean getIsHost() {
        return isHost;
    }

    public Boolean getIsBorderSwitch() {
        return isBorderSwitch;
    }

    public List<Short> getPorts() {
        return ports;
    }

    public Map<Short, Link> getNeighbors() {
        return neighbors;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public void addPort(short i) {
        ports.add(i);
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