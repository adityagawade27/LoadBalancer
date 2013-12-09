package main.edu.columbia.cs6998.sdn.project;

import java.util.ArrayList;
import java.util.List;

public class RoundRobinServers implements LoadBalancerAlgo {

	private int lastServer;
	private List<Server> servers;

	public RoundRobinServers() {

		servers = new ArrayList<Server>();
		lastServer = 0;
	}

	@Override
	public Server getNextServer() {

		lastServer = (lastServer + 1) % servers.size();
		return servers.get(lastServer);

	}

	public void addServer(Server server) {

		servers.add(server);
	}

	public void addServers(List<Server> servers) {

		servers.addAll(servers);
	}

	public List<Server> getServers() {

		return servers;
	}

}
