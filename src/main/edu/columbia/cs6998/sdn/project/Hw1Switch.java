/**
 *    Copyright 2013, Columbia University.
 *    Homework 1, COMS E6998-8 Fall 2013
 *    Software Defined Networking
 *    Originally created by YoungHoon Jung, Columbia University
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License"); you may
 *    not use this file except in compliance with the License. You may obtain
 *    a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
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
import java.util.concurrent.ConcurrentHashMap;
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
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.statistics.OFAggregateStatisticsRequest;
import org.openflow.protocol.statistics.OFFlowStatisticsReply;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.openflow.util.HexString;
import org.openflow.util.LRULinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hw1Switch implements IFloodlightModule, IOFMessageListener {
	protected static Logger log = LoggerFactory.getLogger(Hw1Switch.class);

	// Module dependencies
	protected IFloodlightProviderService floodlightProvider;

	// Stores the learned state for each switch
	protected Map<IOFSwitch, Map<Long, Short>> macToSwitchPortMap;

	// Stores the accumulated packet length for each host node
	protected Map<Long, Map<IOFSwitch, Long>> totalDataLengths;
	// Stores the MAC address of hosts to block: <Macaddr, blockedTime>
	protected Map<Long, Long> blacklist;

	// flow-mod - for use in the cookie
	public static final int HW1_SWITCH_APP_ID = 10;
	// LOOK! This should probably go in some class that encapsulates
	// the app cookie management
	public static final int APP_ID_BITS = 12;
	public static final int APP_ID_SHIFT = (64 - APP_ID_BITS);
	public static final long HW1_SWITCH_COOKIE = (long) (HW1_SWITCH_APP_ID & ((1 << APP_ID_BITS) - 1)) << APP_ID_SHIFT;

	// more flow-mod defaults
	protected static final short IDLE_TIMEOUT_DEFAULT = 10;
	protected static final short HARD_TIMEOUT_DEFAULT = 3;
	protected static final short PRIORITY_DEFAULT = 100;

	// for managing our map sizes
	protected static final int MAX_MACS_PER_SWITCH = 1000;

	// maxinum allowed transmission size
	protected static final int FIREWALL_MAX_ALLOW_LENGTH = (15 * 1024 * 1024);
	// time duration the firewall will block each node for
	protected static final int FIREWALL_BLOCK_TIME_DUR = (10 * 1000);

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
		return "hw1switch";
	}

	/**
	 * Adds a host to the MAC->SwitchPort mapping
	 * 
	 * @param sw
	 *            The switch to add the mapping to
	 * @param mac
	 *            The MAC address of the host to add
	 * @param portVal
	 *            The switchport that the host is on
	 */
	protected void addToPortMap(IOFSwitch sw, long mac, short portVal) {
		Map<Long, Short> swMap = macToSwitchPortMap.get(sw);

		if (swMap == null) {
			// May be accessed by REST API so we need to make it thread safe
			swMap = Collections
					.synchronizedMap(new LRULinkedHashMap<Long, Short>(
							MAX_MACS_PER_SWITCH));
			macToSwitchPortMap.put(sw, swMap);
		}
		swMap.put(mac, portVal);
	}

	/**
	 * Removes a host from the MAC->SwitchPort mapping
	 * 
	 * @param sw
	 *            The switch to remove the mapping from
	 * @param mac
	 *            The MAC address of the host to remove
	 */
	protected void removeFromPortMap(IOFSwitch sw, long mac) {
		Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
		if (swMap != null)
			swMap.remove(mac);
	}

	/**
	 * Get the port that a MAC is associated with
	 * 
	 * @param sw
	 *            The switch to get the mapping from
	 * @param mac
	 *            The MAC address to get
	 * @return The port the host is on
	 */
	public Short getFromPortMap(IOFSwitch sw, long mac) {
		Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
		if (swMap != null)
			return swMap.get(mac);

		// if none found
		return null;
	}

	/**
	 * Clears the MAC -> SwitchPort map for all switches
	 */
	public void clearLearnedTable() {
		macToSwitchPortMap.clear();
	}

	/**
	 * Clears the MAC -> SwitchPort map for a single switch
	 * 
	 * @param sw
	 *            The switch to clear the mapping for
	 */
	public void clearLearnedTable(IOFSwitch sw) {
		Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
		if (swMap != null)
			swMap.clear();
	}

	/**
	 * Writes a OFFlowMod to a switch.
	 * 
	 * @param sw
	 *            The switch tow rite the flowmod to.
	 * @param command
	 *            The FlowMod actions (add, delete, etc).
	 * @param bufferId
	 *            The buffer ID if the switch has buffered the packet.
	 * @param match
	 *            The OFMatch structure to write.
	 * @param outPort
	 *            The switch port to output it to.
	 */
	private void writeFlowMod(IOFSwitch sw, short command, int bufferId,
			OFMatch match, short outPort) {
		// from openflow 1.0 spec - need to set these on a struct ofp_flow_mod:
		// struct ofp_flow_mod {
		// struct ofp_header header;
		// struct ofp_match match; /* Fields to match */
		// uint64_t cookie; /* Opaque controller-issued identifier. */
		//
		// /* Flow actions. */
		// uint16_t command; /* One of OFPFC_*. */
		// uint16_t idle_timeout; /* Idle time before discarding (seconds). */
		// uint16_t hard_timeout; /* Max time before discarding (seconds). */
		// uint16_t priority; /* Priority level of flow entry. */
		// uint32_t buffer_id; /* Buffered packet to apply to (or -1).
		// Not meaningful for OFPFC_DELETE*. */
		// uint16_t out_port; /* For OFPFC_DELETE* commands, require
		// matching entries to include this as an
		// output port. A value of OFPP_NONE
		// indicates no restriction. */
		// uint16_t flags; /* One of OFPFF_*. */
		// struct ofp_action_header actions[0]; /* The action length is inferred
		// from the length field in the
		// header. */
		// };
		OFFlowMod flowMod = (OFFlowMod) floodlightProvider
				.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
		flowMod.setMatch(match);
		flowMod.setCookie(Hw1Switch.HW1_SWITCH_COOKIE);
		flowMod.setCommand(command);
		flowMod.setIdleTimeout(Hw1Switch.IDLE_TIMEOUT_DEFAULT);
		flowMod.setHardTimeout(Hw1Switch.HARD_TIMEOUT_DEFAULT);
		flowMod.setPriority(Hw1Switch.PRIORITY_DEFAULT);
		flowMod.setBufferId(bufferId);
		flowMod.setOutPort((command == OFFlowMod.OFPFC_DELETE) ? outPort
				: OFPort.OFPP_NONE.getValue());
		flowMod.setFlags((command == OFFlowMod.OFPFC_DELETE) ? 0
				: (short) (1 << 0)); // OFPFF_SEND_FLOW_REM

		// set the ofp_action_header/out actions:
		// from the openflow 1.0 spec: need to set these on a struct
		// ofp_action_output:
		// uint16_t type; /* OFPAT_OUTPUT. */
		// uint16_t len; /* Length is 8. */
		// uint16_t port; /* Output port. */
		// uint16_t max_len; /* Max length to send to controller. */
		// type/len are set because it is OFActionOutput,
		// and port, max_len are arguments to this constructor
		flowMod.setActions(Arrays.asList((OFAction) new OFActionOutput(outPort,
				(short) 0xffff)));
		flowMod.setLength((short) (OFFlowMod.MINIMUM_LENGTH + OFActionOutput.MINIMUM_LENGTH));

		if (log.isTraceEnabled()) {
			log.trace("{} {} flow mod {}",
					new Object[] {
							sw,
							(command == OFFlowMod.OFPFC_DELETE) ? "deleting"
									: "adding", flowMod });
		}

		// and write it out
		try {
			sw.write(flowMod, null);
		} catch (IOException e) {
			log.error("Failed to write {} to switch {}", new Object[] {
					flowMod, sw }, e);
		}
	}

	/**
	 * Writes an OFPacketOut message to a switch.
	 * 
	 * @param sw
	 *            The switch to write the PacketOut to.
	 * @param packetInMessage
	 *            The corresponding PacketIn.
	 * @param egressPort
	 *            The switchport to output the PacketOut.
	 */
	private void writePacketOutForPacketIn(IOFSwitch sw,
			OFPacketIn packetInMessage, short egressPort) {

		// from openflow 1.0 spec - need to set these on a struct
		// ofp_packet_out:
		// uint32_t buffer_id; /* ID assigned by datapath (-1 if none). */
		// uint16_t in_port; /* Packet's input port (OFPP_NONE if none). */
		// uint16_t actions_len; /* Size of action array in bytes. */
		// struct ofp_action_header actions[0]; /* Actions. */
		/* uint8_t data[0]; *//*
							 * Packet data. The length is inferred from the
							 * length field in the header. (Only meaningful if
							 * buffer_id == -1.)
							 */

		OFPacketOut packetOutMessage = (OFPacketOut) floodlightProvider
				.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
		short packetOutLength = (short) OFPacketOut.MINIMUM_LENGTH; // starting
																	// length

		// Set buffer_id, in_port, actions_len
		packetOutMessage.setBufferId(packetInMessage.getBufferId());
		packetOutMessage.setInPort(packetInMessage.getInPort());
		packetOutMessage
				.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
		packetOutLength += OFActionOutput.MINIMUM_LENGTH;

		// set actions
		List<OFAction> actions = new ArrayList<OFAction>(1);
		actions.add(new OFActionOutput(egressPort, (short) 0));
		packetOutMessage.setActions(actions);

		// set data - only if buffer_id == -1
		if (packetInMessage.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
			byte[] packetData = packetInMessage.getPacketData();
			packetOutMessage.setPacketData(packetData);
			packetOutLength += (short) packetData.length;
		}

		// finally, set the total length
		packetOutMessage.setLength(packetOutLength);

		// and write it out
		try {
			sw.write(packetOutMessage, null);
		} catch (IOException e) {
			log.error("Failed to write {} to switch {}: {}", new Object[] {
					packetOutMessage, sw, e });
		}
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
		Long sourceMac = Ethernet.toLong(match.getDataLayerSource());
		Long destMac = Ethernet.toLong(match.getDataLayerDestination());
		boolean blocked = false;

		// learn the port for this MAC
		this.addToPortMap(sw, sourceMac, pi.getInPort());

		Map<IOFSwitch, Long> switchToLengthMap = totalDataLengths
				.get(sourceMac);
		if (switchToLengthMap == null) {
			switchToLengthMap = new HashMap<IOFSwitch, Long>();
			totalDataLengths.put(sourceMac, switchToLengthMap);
		}
		Long currentLength = switchToLengthMap.get(sw);
		Long blockedSince = blacklist.get(sourceMac);
		if (currentLength == null)
			currentLength = (long) 0;

		if (blockedSince != null) {
			if ((System.currentTimeMillis() - blockedSince) > FIREWALL_BLOCK_TIME_DUR) {
				switchToLengthMap.clear();
				blacklist.remove(sourceMac);
				currentLength = (long) 0;
				//log.info("Unblocking [" + HexString.toHexString(sourceMac)
					//	+ "] at " + System.currentTimeMillis());
			} else
				blocked = true;
		}

		currentLength += pi.getTotalLength();
		switchToLengthMap.put(sw, currentLength);

		if (currentLength >= FIREWALL_MAX_ALLOW_LENGTH
				&& blacklist.get(sourceMac) == null) {
			//log.info("Blocking   [" + HexString.toHexString(sourceMac)
			//		+ "] at " + System.currentTimeMillis());
			blacklist.put(sourceMac, System.currentTimeMillis());
			blocked = true;
		}

		if (blocked)
			return Command.CONTINUE;

		//log.info("PacketIn at " + System.currentTimeMillis()
		//		+ " match {} pi {} leng {} ",
		//		new Object[] { match, pi, pi.getTotalLength() });

		/*
		 * // Only when it works as a hub // Now the switch will flood the
		 * packet to all of its ports this.writePacketOutForPacketIn(sw, pi,
		 * OFPort.OFPP_FLOOD.getValue());
		 */

		// Learning Switch Implementation
		// Now output flow-mod and/or packet
		Short outPort = getFromPortMap(sw, destMac);
		if (outPort == null) {
			// If we haven't learned the port for the dest MAC, flood it
			// Don't flood broadcast packets if the broadcast is disabled.
			// XXX For Hw1Switch this doesn't do much. The sourceMac is removed
			// from port map whenever a flow expires, so you would still see
			// a lot of floods.
			this.writePacketOutForPacketIn(sw, pi, OFPort.OFPP_FLOOD.getValue());
		} else if (outPort == match.getInputPort()) {
			log.info(
					"ignoring packet that arrived on same port as learned destination:"
							+ " switch {} dest MAC {} port {}", new Object[] {
							sw, HexString.toHexString(destMac), outPort });
		} else {
			// Add flow table entry matching source MAC, dest MAC and input port
			// that sends to the port we previously learned for the dest MAC.
			match.setWildcards(((Integer) sw
					.getAttribute(IOFSwitch.PROP_FASTWILDCARDS)).intValue()
					& ~OFMatch.OFPFW_IN_PORT
					& ~OFMatch.OFPFW_DL_VLAN
					& ~OFMatch.OFPFW_DL_SRC
					& ~OFMatch.OFPFW_DL_DST
					& ~OFMatch.OFPFW_NW_SRC_MASK & ~OFMatch.OFPFW_NW_DST_MASK);
			this.writeFlowMod(sw, OFFlowMod.OFPFC_ADD, pi.getBufferId(), match,
					outPort);
			//log.info("{} flow entry installed {}", sw, match);
		}

		return Command.CONTINUE;
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
	 */
	private Command processFlowRemovedMessage(IOFSwitch sw,
			OFFlowRemoved flowRemovedMessage) {
		if (flowRemovedMessage.getCookie() != Hw1Switch.HW1_SWITCH_COOKIE) {
			return Command.CONTINUE;
		}

		
		Long sourceMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerSource());
		Long destMac = Ethernet.toLong(flowRemovedMessage.getMatch()
				.getDataLayerDestination());

		
		
		if (log.isTraceEnabled()) {
			//	log.trace("{} flow entry removed {}", sw,
			//		HexString.toHexString(sourceMac));
		}
		
		Map<IOFSwitch, Long> switchToLengthMap = totalDataLengths
				.get(sourceMac);
		if (switchToLengthMap == null) {
			switchToLengthMap = new HashMap<IOFSwitch, Long>();
			totalDataLengths.put(sourceMac, switchToLengthMap);
		}
		Long currentLength = switchToLengthMap.get(sw);
		currentLength += flowRemovedMessage.getByteCount();
		switchToLengthMap.put(sw, currentLength);

		if (currentLength >= FIREWALL_MAX_ALLOW_LENGTH
				&& blacklist.get(sourceMac) == null) {
			log.info("Blocking   [" + HexString.toHexString(sourceMac)
					+ "] at " + System.currentTimeMillis());
			blacklist.put(sourceMac, System.currentTimeMillis());
		}

		/*log.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(sourceMac));
		log.info(
				"{} flow entry removed {} used Bytes:"
						+ flowRemovedMessage.getByteCount(), sw,
				HexString.toHexString(destMac));*/
		OFMatch match = flowRemovedMessage.getMatch();
		// When a flow entry expires, it means the device with the matching
		// source
		// MAC address either stopped sending packets or moved to a different
		// port. If the device moved, we can't know where it went until it sends
		// another packet, allowing us to re-learn its port. Meanwhile we remove
		// it from the macToPortMap to revert to flooding packets to this
		// device.
		this.removeFromPortMap(sw, Ethernet.toLong(match.getDataLayerSource()));

		return Command.CONTINUE;
	}

	
	// IOFMessageListener

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		switch (msg.getType()) {
		case PACKET_IN:
			return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);

		case FLOW_REMOVED:
			return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);

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
		macToSwitchPortMap = new ConcurrentHashMap<IOFSwitch, Map<Long, Short>>();
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		totalDataLengths = new ConcurrentHashMap<Long, Map<IOFSwitch, Long>>();
		blacklist = new HashMap<Long, Long>();
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
		floodlightProvider.addOFMessageListener(OFType.ERROR, this);
	}
}
