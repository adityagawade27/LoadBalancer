/**
 * Copyright 2013, Columbia University.
 * Homework 1, COMS E6998-8 Fall 2013
 * Software Defined Networking
 * Originally created by YoungHoon Jung, Columbia University
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 **/

/**
 * Floodlight
 * A BSD licensed, Java based OpenFlow controller
 *
 * Floodlight is a Java based OpenFlow controller originally written by David Erickson at Stanford
 * University. It is available under the BSD license.
 *
 * For documentation, forums, issue tracking and more visit:
 *
 * http://www.openflowhub.org/display/Floodlight/Floodlight+Home
 **/

package main.edu.columbia.cs6998.sdn.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;

import org.openflow.protocol.OFError;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionDataLayerDestination;
import org.openflow.protocol.action.OFActionDataLayerSource;
import org.openflow.protocol.action.OFActionNetworkLayerDestination;
import org.openflow.protocol.action.OFActionNetworkLayerSource;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.statistics.OFAggregateStatisticsRequest;
import org.openflow.protocol.statistics.OFFlowStatisticsReply;
import org.openflow.protocol.statistics.OFFlowStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.openflow.util.HexString;
import org.openflow.util.LRULinkedHashMap;
import org.openflow.util.U16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadBalancer implements IFloodlightModule, IOFMessageListener {

	protected static Logger log = LoggerFactory.getLogger(LoadBalancer.class);

	// Module dependencies
	protected IFloodlightProviderService floodlightProvider;

	// more flow-mod defaults
	protected static final short IDLE_TIMEOUT_DEFAULT = 10;
	protected static final short HARD_TIMEOUT_DEFAULT = 3;
	protected static final short PRIORITY_DEFAULT = 100;

	// Load Balancer Component
	private final static int LOAD_BALANCER_IP = IPv4
			.toIPv4Address("10.0.0.100");
	private final static byte[] LOAD_BALANCER_MAC = Ethernet
			.toMACAddress("00:00:00:00:00:FE");

	private Map<Integer, Long> trafficStats;
	private Map<Short, RoundRobinServers> portNumberServersMap;

	/**
	 * @param floodlightProvider
	 *            the floodlightProvider to set
	 */
	public void setFloodlightProvider(
			IFloodlightProviderService floodlightProvider) {
		this.floodlightProvider = floodlightProvider;
	}

	@Override
	public String getName() {
		return "LoadBalancer";
	}

	/**
	 * Processes a OFPacketIn message. If the switch has learned the MAC to port
	 * mapping for the pair it will write a FlowMod for. If the mapping has not
	 * been learned the we will flood the packet.
	 * 
	 * @param sw
	 * @param pi
	 * @param cntx
	 * @return
	 */
	private Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi,
			FloodlightContext cntx) {

		// Read in packet data headers by using OFMatch
		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());
		Integer destIPAddress = match.getNetworkDestination();

		Server server = new Server();
		server.setIP(IPv4.fromIPv4Address(destIPAddress));

		if (destIPAddress == LOAD_BALANCER_IP) {

			server = getDestServer(sw, pi);
			processRuleAndPushPacket(server, sw, pi,true);
		} else {

			// server = getLeastLoadedPath(server, sw, pi); // TODO
			processRuleAndPushPacket(server, sw, pi,false);

		}

		return Command.CONTINUE;
	}

	private Server getDestServer(IOFSwitch sw, OFPacketIn pi) {

		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());

		Server server = portNumberServersMap.get(
				match.getTransportDestination()).getNextServer();
		return server;

	}

	private void processRuleAndPushPacket(Server forwardServer,
			IOFSwitch sw, OFPacketIn pi, Boolean rewrite) {

		OFFlowMod rule = new OFFlowMod();
		rule.setType(OFType.FLOW_MOD);
		rule.setCommand(OFFlowMod.OFPFC_ADD);

		OFMatch match = new OFMatch();
		match.loadFromPacket(pi.getPacketData(), pi.getInPort());
		match.setWildcards(~OFMatch.OFPFW_ALL);
		match.setNetworkDestination(forwardServer.getIP());

		rule.setMatch(match);
		rule.setIdleTimeout(IDLE_TIMEOUT_DEFAULT);
		rule.setHardTimeout(HARD_TIMEOUT_DEFAULT);
		rule.setBufferId(OFPacketOut.BUFFER_ID_NONE);

		short actionsLength = 0;
		ArrayList<OFAction> actions = null;
		if (rewrite) {

			actions = new ArrayList<OFAction>();

			OFAction rewriteMAC = new OFActionDataLayerDestination(
					forwardServer.getMAC().getBytes());
			actions.add(rewriteMAC);

			OFAction rewriteIP = new OFActionNetworkLayerDestination(
					forwardServer.getIP());
			actions.add(rewriteIP);

			OFAction outputTo = new OFActionOutput(forwardServer.getPort());
			actions.add(outputTo);

			rule.setActions(actions);

			actionsLength = (short) (OFActionDataLayerDestination.MINIMUM_LENGTH
					+ OFActionNetworkLayerDestination.MINIMUM_LENGTH 
					+ OFActionOutput.MINIMUM_LENGTH);
		}

		rule.setLength((short) (OFFlowMod.MINIMUM_LENGTH + actionsLength));

		try {
			sw.write(rule, null);
		} catch (Exception e) {
			e.printStackTrace();
		}

		pushPacket(sw, pi, actions, actionsLength);

	}

	private void pushPacket(IOFSwitch sw, OFPacketIn pi,
			ArrayList<OFAction> actions, short actionsLength) {

		// create an OFPacketOut for the pushed packet
		OFPacketOut po = (OFPacketOut) floodlightProvider.getOFMessageFactory()
				.getMessage(OFType.PACKET_OUT);

		// Update the inputPort and bufferID
		po.setInPort(pi.getInPort());
		po.setBufferId(pi.getBufferId());

		// Set the actions to apply for this packet
		po.setActions(actions);
		po.setActionsLength(actionsLength);

		// Set data if it is included in the packet in but buffer id is NONE
		if (pi.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
			byte[] packetData = pi.getPacketData();
			po.setLength(U16.t(OFPacketOut.MINIMUM_LENGTH
					+ po.getActionsLength() + packetData.length));
			po.setPacketData(packetData);
		} else {
			po.setLength(U16.t(OFPacketOut.MINIMUM_LENGTH
					+ po.getActionsLength()));
		}

		log.debug("Push packet to switch: " + po);

		// Push the packet to the switch
		try {
			sw.write(po, null);
		} catch (IOException e) {
			log.error("failed to write packetOut: ", e);
		}
	}

	/**
	 * Processes a flow removed message. We will delete the learned MAC mapping
	 * from the switch's table.
	 * 
	 * @param sw
	 *            The switch that sent the flow removed message.
	 * @param flowRemovedMessage
	 *            The flow removed message.
	 * @return Whether to continue processing this message or stop.
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedExceptionOFMatch
	 *             match = flowRemovedMessage.getMatch();
	 */
	private Command processFlowRemovedMessage(IOFSwitch sw,
			OFFlowRemoved flowRemovedMessage) throws IOException,
			InterruptedException, ExecutionException {

		Long sourceMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerSource());
		Long destMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerDestination());

		// Collect Statistics for each flow that was removed
		trafficStats.clear();
		List<Server> serverList = new ArrayList<Server>();
		for (Map.Entry<Short, RoundRobinServers> entry : portNumberServersMap
				.entrySet()) {
			serverList.addAll(entry.getValue().getServers());

		}
		for (Server server : serverList) {

			if (flowRemovedMessage.getMatch().getNetworkDestination() == server
					.getIP()) {

				Long count = trafficStats.get(server.getIP()) + 1;
				trafficStats.put(server.getIP(), count);
			}

		}

		if (log.isTraceEnabled()) {
			log.trace("{} flow entry removed {}", sw,
					HexString.toHexString(sourceMac));
		}

		log.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(sourceMac));
		log.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(destMac));

		return Command.CONTINUE;
	}

	@SuppressWarnings("unchecked")
	// IOFMessageListener
	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		switch (msg.getType()) {
		case PACKET_IN:
			return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);

		case FLOW_REMOVED:
			try {
				return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
			} catch (IOException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}

		case ERROR:
			log.info("received an error {} from switch {}", (OFError) msg, sw);
			return Command.CONTINUE;
		default:
			break;
		}
		log.error("received an unexpected message {} from switch {}", msg, sw);
		return Command.CONTINUE;
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return false;
	}

	// IFloodlightModule

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {

		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		trafficStats = new TreeMap<Integer, Long>();
		portNumberServersMap = new HashMap<Short, RoundRobinServers>();

		initializeServers();

	}

	private void initializeServers() {

		// Hardcoding Server Addresses for now

		List<Server> servers = new ArrayList<Server>();

		Server h3 = new Server();
		h3.setIP("10.0.0.3");

		Server h4 = new Server();
		h4.setIP("10.0.0.4");

		Server h5 = new Server();
		h5.setIP("10.0.0.5");

		Server h6 = new Server();
		h6.setIP("10.0.0.6");

		servers.add(h3);
		servers.add(h4);
		servers.add(h5);
		servers.add(h6);

		RoundRobinServers rrservers = new RoundRobinServers();
		rrservers.addServers(servers);

		portNumberServersMap.put((short) 8080, rrservers);
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
		floodlightProvider.addOFMessageListener(OFType.ERROR, this);
	}
}