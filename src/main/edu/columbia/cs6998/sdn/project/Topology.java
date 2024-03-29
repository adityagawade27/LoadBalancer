package main.edu.columbia.cs6998.sdn.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;

/**
 * Created by Niket Kandya on 12/9/13.
 */

public class Topology implements ITopology {

	private HashMap<String, Node> topology;
	private HashMap<String, String> ipToNodeMap;
	private HashMap<String, String> macToNodeMap;
	private ArrayList<Node> endHosts;
	private ArrayList<Node> switches;
	private HashMap<NodeNodePair, RouteRREntity> routes;
	private ArrayList<AppServer> appServers; 

	public final static String LOAD_BALANCER_IP = "10.0.0.254";
	public final static String LOAD_BALANCER_MAC =  "00:00:00:00:00:FE";
	private String FILE_TOPOLOGY_INFO = "/home/mininet/LoadBalancer/info/topology.info";
	private String FILE_APPSERV_INFO = "/home/mininet/LoadBalancer/info/appservers.info";

	private static Topology instance;

	public HashMap<String, Node> getTopology() {
		return topology;
	}



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
		macToNodeMap = new HashMap<>();
		endHosts = new ArrayList<>();
		switches = new ArrayList<>();
		routes = new HashMap<>();
		appServers = new ArrayList<>();

		initialize();
	}


	private void initialize() {
		readFromTopoFile();
		readFromAppservFile();
		preprocessLinks();
		calculateRoutes();
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

	private void readFromAppservFile() {

		URL url=null;

		try {
			url = new File(FILE_APPSERV_INFO).toURI().toURL();
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(url.openStream()));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(":");
				appServers.add(new AppServer(split[1], Short.parseShort(split[2])));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void readFromTopoFile() {
		URL url=null;

		try {
			url = new File(FILE_TOPOLOGY_INFO).toURI().toURL();
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
			String line = null;
			Node node1, node2;
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				if (!topology.containsKey(split[0]))
					node1 = addNewNode(split[0], split[1], split[2], split[3]);
				else {
					node1 = topology.get(split[0]);
					node1.addPort(Short.parseShort(split[1]), split[2]);
				}

				if (!topology.containsKey(split[4]))
					node2 = addNewNode(split[4], split[5], split[6], split[7]);
				else {
					node2 = topology.get(split[4]);
					node2.addPort(Short.parseShort(split[5]), split[6]);
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
		node.setIpAddress(s3);
		if (node.getIpAddress().equals("127.0.0.1"))
			node.setIsHost(false);
		else
			node.setIsHost(true);
		node.addPort(Short.parseShort(s1), s2);
		topology.put(node.getName(), node);
		ipToNodeMap.put(node.getIpAddress(),node.getName());
		return node;
	}

	public ArrayList<AppServer> getAppServList(short port) {
		ArrayList<AppServer> ret = new ArrayList<AppServer>();
		
		for(AppServer serv : appServers){
			//short = serv.getPort();
//			System.out.println(serv.getPort()+" "+serv.getIp());
			if(serv.getPortClass() == port) {
				ret.add(serv);
			}
		}
		
		return ret;
		
	}
	
	public short getAppServerPort(String ip) {
		for(AppServer app: appServers){
			if(app.getIp().equals(ip))
				return app.getPort();
		}
		return -1;
	}
	
	/* Returns the port number if its the app server else returns -1 */
	public short isAppServer(String ip,short port) {
		for(AppServer app : appServers) {
			if(app.getIp().equals(ip) && app.getPort() == port){
				return (short) app.getPortClass();
			}
		}
		return -1;
	}
	
	public String getMacFromPort(String name, short port) {
		return topology.get(name).getPorts().get(port);
	}

	public String getMacAddressFromIP(String ipAddress) {
		if (ipAddress.equals(LOAD_BALANCER_IP))
			return LOAD_BALANCER_MAC;

		Node node = getTopology().get(ipToNodeMap.get(ipAddress));
		ArrayList<String> macAddresses = new ArrayList<>(node.getPorts().values());
		return macAddresses.get(0);
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
		HashMap<NodeNodePair, RouteRREntity> rrEntityHashMap = getInstance().getRoutes();
		System.out.println(rrEntityHashMap);
	}

	private List<FinalRoute> calcRoutes(Node swtch, Node host) {
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
				continue;
			}
			for (Link link : getTopology().get(last).getNeighbors().values()) {
				String id = link.getPair().getDstEndHost().getName();
				if (!routeContainsNode(tempRoute, id)) {
					newRoute = new FinalRoute();
					newRoute.append(tempRoute.getRoute());
					newRoute.append(getTopology().get(id));
					queue.push(newRoute);
				}
			}
		}
		return retVal;
	}

	private boolean routeContainsNode(FinalRoute tempRoute, String id) {
		ArrayList<Link> route = tempRoute.getRoute();
		for (Link link : route) {
			if (link.getPair().getDstEndHost().getName().equals(id)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public short getNextHop(String dstIP, String name) {
		Node srcNode = topology.get(name);
		NodeNodePair pair = new NodeNodePair(srcNode, topology.get(ipToNodeMap.get(dstIP)));
		RouteRREntity routeRREntity = routes.get(pair);
		List<FinalRoute> finalRoutes = routeRREntity.getRoutes();

		if (finalRoutes.size() == 1) {
			return finalRoutes.get(0).getFirstHopPort();
		} else {
			return selectNext(routeRREntity, srcNode).getFirstHopPort();
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
