package main.edu.columbia.cs6998.sdn.project;

import com.sun.istack.internal.Nullable;

import java.util.ArrayList;
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

    Short getFirstId() {
        if (!route.isEmpty())
            return route.get(0).getPair().getDstEndHost().getId();
        else
            return -1; //This should never happen.
    }

    Short getLastId() {
        if (!route.isEmpty())
            return route.get(route.size() - 1).getPair().getDstEndHost().getId();
        else
            return -1; //This should never happen.
    }

    void append(Node node) {
        Node dstEndHost = route.get(route.size() - 1).getPair().getDstEndHost();
        route.add(new Link(new NodeNodePair(dstEndHost, node)));
    }

    public void append(ArrayList<Link> param_route) {
        route.addAll(param_route);
    }
}

class Link {
    private NodeNodePair pair;
    private Double cost;

    Link(NodeNodePair pair) {
        this(pair, 0.0);
    }

    Link(NodeNodePair pair, Double cost) {
        this.pair = pair;
        this.cost = cost;
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
    @Nullable
    String ipAddress;

    //Mac address of the host
    String macAddress;

    //Check if the node is an end host
    Boolean isHost;

    //Check if the switch is a border switch
    Boolean isBorderSwitch;

    //List of ports this node has
    List<Short> ports;

    //Map of port to neighboring nodes
    Map<Short, Link> neighbors;

    //Unique id for this node
    Short id;

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

    public Short getId() {
        return id;
    }
}