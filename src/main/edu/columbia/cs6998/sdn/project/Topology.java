package main.edu.columbia.cs6998.sdn.project;

import java.util.*;

/**
 * Created by Niket Kandya on 12/9/13.
 */

public class Topology implements ITopology {

    private HashMap<Short, Node> topology;
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
        topology = new HashMap<Short, Node>();
        ipToNodeMap = new HashMap<String, Short>();
        initialize();
    }

    private void initialize() {
        //Read the topology from the file here
        for (Node swtch : switches) {
            for (Node host : endHosts) {
                List<FinalRoute> localRoutes = getRoutes(swtch, host);
                routes.put(new NodeNodePair(swtch, host), new RouteRREntity(localRoutes, (short) 0));
            }
        }
    }

    private List<FinalRoute> getRoutes(Node swtch, Node host) {
        List<FinalRoute> retVal = new ArrayList<>();
        Deque<FinalRoute> queue = new ArrayDeque<>();
        Short last;
        FinalRoute tempRoute = new FinalRoute();
        FinalRoute newRoute;
        tempRoute.append(swtch);
        queue.push(tempRoute);

        while (!queue.isEmpty()) {
            tempRoute = queue.remove();
            last = tempRoute.getLastId();
            if (last == host.getId()) {
                retVal.add(new FinalRoute(tempRoute));
            }
            for (Link link : host.getNeighbors().values()) {
                Short id = link.getPair().getDstEndHost().getId();
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
            return finalRoutes.get(0).getFirstId();
        } else {
            return selectNext(routeRREntity, srcNode).getFirstId();
        }
    }

    private FinalRoute selectNext(RouteRREntity rrEntity, Node srcNode) {
        short currentInstance = rrEntity.getCurrentInstance();
        FinalRoute finalRoute;

        //Search for available legal path
        do {
            finalRoute = rrEntity.getRoutes().get(currentInstance);
            if (finalRoute.getFirstId() != srcNode.getId())
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