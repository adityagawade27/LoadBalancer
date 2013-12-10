package main.edu.columbia.cs6998.sdn.project;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by Niket Kandya on 12/9/13.
 */

public class Topology implements ITopology {

    private HashMap<String, Node> topology;
    private HashMap<String, Short> ipToNodeMap;
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

    private Topology() {
        topology = new HashMap<String, Node>();
        ipToNodeMap = new HashMap<String, Short>();
        initialize();
    }

    private void initialize() {
        //Read the topology from the file here
        readFromFile();
        preprocessLinks();
        for (Node swtch : switches) {
            for (Node host : endHosts) {
                List<FinalRoute> localRoutes = getRoutes(swtch, host);
                routes.put(new NodeNodePair(swtch, host), new RouteRREntity(localRoutes, (short) 0));
            }
        }
    }

    private void preprocessLinks() {
        for (String name : topology.keySet()) {

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
        return node;
    }

    public static void main(String args[]) {
        Topology.getInstance();
    }

    private List<FinalRoute> getRoutes(Node swtch, Node host) {
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