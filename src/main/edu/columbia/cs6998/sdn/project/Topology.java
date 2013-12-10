package main.edu.columbia.cs6998.sdn.project;

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by Niket Kandya on 12/9/13.
 */

public class Topology implements ITopology {

    public HashMap<String, Node> getTopology() {
        return topology;
    }

    private HashMap<String, Node> topology;
    private HashMap<String, String> ipToNodeMap;
    private ArrayList<Node> endHosts;
    private ArrayList<Node> switches;
    private HashMap<NodeNodePair, RouteRREntity> routes;

    private static Topology instance;

    public static Topology getInstance() {
        if (instance == null) {
            instance = new Topology();
        }
        return instance;
    }

    public HashMap<NodeNodePair, RouteRREntity> getRoutes() {
        return routes;
    }

    private Topology() {
        topology = new HashMap<>();
        ipToNodeMap = new HashMap<>();
        endHosts = new ArrayList<>();
        switches = new ArrayList<>();
        routes = new HashMap<>();

        initialize();
    }

    
    private void initialize() {
        readFromFile();
        preprocessLinks();
        //calculateRoutes();
    }

    private void calculateRoutes() {
        for (Node swtch : switches) {
            for (Node host : endHosts) {
                List<FinalRoute> localRoutes = calcRoutes(swtch, host);
                routes.put(new NodeNodePair(swtch, host), new RouteRREntity(localRoutes, (short) 0));
            }
        }
    }

    private void preprocessLinks() {
        for (Node value : topology.values()) {
            if (!value.getIsHost()) {
                switches.add(value);
            } else {
                endHosts.add(value);
            }
        }
    }

    private void readFromFile() {
        InputStream in = getClass().getResourceAsStream("sample.output");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = null;
            Node node1, node2;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                if (!topology.containsKey(split[0]))
                    node1 = addNewNode(split[0], split[1], split[2], split[3]);
                else {
                    node1 = topology.get(split[0]);
                    node1.addPort(Short.parseShort(split[1]));
                }

                if (!topology.containsKey(split[4]))
                    node2 = addNewNode(split[4], split[5], split[6], split[7]);
                else {
                    node2 = topology.get(split[4]);
                    node2.addPort(Short.parseShort(split[5]));
                }

                Link link1 = new Link(new NodeNodePair(node1, node2),
                        Short.parseShort(split[1]), Short.parseShort(split[5]));
                Link link2 = new Link(new NodeNodePair(node2, node1),
                        Short.parseShort(split[5]), Short.parseShort(split[1]));

                node1.addNeighbor(link1.getSrcPort(), link1);
                node2.addNeighbor(link2.getSrcPort(), link2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Node addNewNode(String s, String s1, String s2, String s3) {
        Node node = new Node();
        node.setName(s);
        node.setMacAddress(s2);
        node.setIpAddress(s3);
        if (node.getIpAddress().equals("127.0.0.1"))
            node.setIsHost(false);
        else
            node.setIsHost(true);
        node.addPort(Short.parseShort(s1));
        topology.put(node.getName(), node);
        ipToNodeMap.put(node.getIpAddress(),node.getName());
        return node;
    }

    public String getMacAddressFromIP(String ipAddress) {
        return getTopology().get(ipToNodeMap.get(ipAddress)).getMacAddress();
    }

    //Ideally this should be calculated from the route
    //and not the neighbor info, but will work for now
    public boolean isNextHop(Node node, String ipAddress) {
        boolean found = false;
        Map<Short, Link> neighbors = node.getNeighbors();
        for (Link link : neighbors.values()) {
            if(link.getPair().getDstEndHost().getName().equals(ipToNodeMap.get(ipAddress))) {
                found = true;
                break;
            }
        }
        return found;
    }

    public static void main(String args[]) {
        HashMap<String, Node> nodeHashMap = getInstance().getTopology();
        System.out.println(nodeHashMap);
        //HashMap<NodeNodePair, RouteRREntity> rrEntityHashMap = getInstance().getRoutes();
        //System.out.println(rrEntityHashMap);
    }

    private List<FinalRoute> calcRoutes(Node swtch, Node host) {
        System.out.println("Entering calcRoutes");
        List<FinalRoute> retVal = new ArrayList<>();
        Deque<FinalRoute> queue = new ArrayDeque<>();
        String last;
        FinalRoute tempRoute = new FinalRoute();
        FinalRoute newRoute;
        tempRoute.append(swtch);
        queue.push(tempRoute);

        while (!queue.isEmpty()) {
            tempRoute = queue.remove();
            last = tempRoute.getLastName();
            if (last == host.getName()) {
                retVal.add(new FinalRoute(tempRoute));
            }
            for (Link link : host.getNeighbors().values()) {
                String id = link.getPair().getDstEndHost().getName();
                if (!tempRoute.getRoute().contains(id)) {
                    newRoute = new FinalRoute();
                    newRoute.append(tempRoute.getRoute());
                    queue.push(newRoute);
                }
            }
        }
        System.out.println("Exiting calcRoutes");
        return retVal;
    }

    @Override
    public short getNextHop(String dstIP, Node srcNode) {
        NodeNodePair pair = new NodeNodePair(srcNode, topology.get(ipToNodeMap.get(dstIP)));
        RouteRREntity routeRREntity = routes.get(pair);
        List<FinalRoute> finalRoutes = routeRREntity.getRoutes();
        if (finalRoutes.size() == 1) {
            return finalRoutes.get(0).getFirstHop();
        } else {
            return selectNext(routeRREntity, srcNode).getFirstHop();
        }
    }

    private FinalRoute selectNext(RouteRREntity rrEntity, Node srcNode) {
        short currentInstance = rrEntity.getCurrentInstance();
        FinalRoute finalRoute;

        //Search for available legal path
        do {
            finalRoute = rrEntity.getRoutes().get(currentInstance);
            if (finalRoute.getFirstHopName() != srcNode.getName())
                break;
            rrEntity.incrementInstance();
            finalRoute = null;
        } while (rrEntity.getCurrentInstance() != currentInstance);

        //Return path same as incoming
        if (finalRoute == null) {
            //Should not happen;
        }

        return finalRoute;
    }
}
