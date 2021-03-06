use std::cmp::{min, max};
use std::collections::VecDeque;
use std::net::{ToSocketAddrs, SocketAddr, UdpSocket};
use std::io::{Result, Error, ErrorKind};
use util::{now_microseconds, ewma};
use packet::{Packet, PacketType, Encodable, Decodable, ExtensionType, HEADER_SIZE};
use rand;

// For simplicity's sake, let us assume no packet will ever exceed the
// Ethernet maximum transfer unit of 1500 bytes.
const BUF_SIZE: usize = 1500;
const GAIN: f64 = 1.0;
const ALLOWED_INCREASE: u32 = 1;
const TARGET: i64 = 100_000; // 100 milliseconds
const MSS: u32 = 1400;
const MIN_CWND: u32 = 2;
const INIT_CWND: u32 = 2;
const INITIAL_CONGESTION_TIMEOUT: u64 = 1000; // one second
const MIN_CONGESTION_TIMEOUT: u64 = 500; // 500 ms
const MAX_CONGESTION_TIMEOUT: u64 = 60_000; // one minute
const BASE_HISTORY: usize = 10; // base delays history size

#[derive(Debug)]
pub enum SocketError {
    ConnectionClosed,
    ConnectionReset,
    InvalidPacket,
    InvalidReply,
    // PendingAcknowledgments,
}

impl From<SocketError> for Error {
    fn from(error: SocketError) -> Error {
        use self::SocketError::*;
        match error {
            ConnectionClosed => Error::new(ErrorKind::NotConnected,
                                           "The socket is closed"),
            ConnectionReset => Error::new(ErrorKind::ConnectionReset,
                                          "Connection reset by remote peer"),
            InvalidPacket => Error::new(ErrorKind::Other,
                                        "Error parsing packet"),
            InvalidReply => Error::new(ErrorKind::ConnectionRefused,
                                       "The remote peer sent an invalid reply"),
            // PendingAcknowledgments => Error::new(ErrorKind::Other,
            //                                      "Received FIN with pending unacknowledged packets")
        }
    }
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
enum SocketState {
    New,
    Connected,
    SynSent,
    FinReceived,
    FinSent,
    ResetReceived,
    Closed,
}

type TimestampSender = i64;
type TimestampReceived = i64;

struct DelayDifferenceSample {
    received_at: TimestampReceived,
    difference: TimestampSender,
}

/// A structure that represents a uTP (Micro Transport Protocol) connection between a local socket
/// and a remote socket.
///
/// The socket will be closed when the value is dropped (either explicitly or when it goes out of
/// scope).
///
/// # Examples
///
/// ```no_run
/// use utp::UtpSocket;
///
/// let mut socket = UtpSocket::bind("127.0.0.1:1234").unwrap();
///
/// let mut buf = [0; 1000];
/// let (amt, _src) = socket.recv_from(&mut buf).ok().unwrap();
///
/// let mut buf = &mut buf[..amt];
/// buf.reverse();
/// let _ = socket.send_to(buf).unwrap();
///
/// // Explicitly close the socket. You can either call `close` on the socket,
/// //explicitly drop it or just let it go out of scope.
/// socket.close();
/// ```
pub struct UtpSocket {
    /// The wrapped UDP socket
    socket: UdpSocket,

    /// Remote peer
    connected_to: SocketAddr,

    /// Sender connection identifier
    sender_connection_id: u16,

    /// Receiver connection identifier
    receiver_connection_id: u16,

    /// Sequence number for the next packet
    seq_nr: u16,

    /// Sequence number of the latest acknowledged packet sent by the remote peer
    ack_nr: u16,

    /// Socket state
    state: SocketState,

    /// Received but not acknowledged packets
    incoming_buffer: Vec<Packet>,

    /// Sent but not yet acknowledged packets
    send_window: Vec<Packet>,

    /// Packets not yet sent
    unsent_queue: VecDeque<Packet>,

    /// How many ACKs did the socket receive for packet with sequence number equal to `ack_nr`
    duplicate_ack_count: u32,

    /// Sequence number of the latest packet the remote peer acknowledged
    last_acked: u16,

    /// Timestamp of the latest packet the remote peer acknowledged
    last_acked_timestamp: u32,

    /// Sequence number of the last packet removed from the incoming buffer
    last_dropped: u16,

    /// Round-trip time to remote peer
    rtt: i32,

    /// Variance of the round-trip time to the remote peer
    rtt_variance: i32,

    /// Data from the latest packet not yet returned in `recv_from`
    pending_data: Vec<u8>,

    /// Bytes in flight
    curr_window: u32,

    /// Window size of the remote peer
    remote_wnd_size: u32,

    /// Rolling window of packet delay to remote peer
    base_delays: VecDeque<i64>,

    /// Rolling window of the difference between sending a packet and receiving its acknowledgement
    current_delays: Vec<DelayDifferenceSample>,

    /// Difference between timestamp of the latest packet received and time of reception
    their_delay: u32,

    /// Start of the current minute for sampling purposes
    last_rollover: i64,

    /// Current congestion timeout in milliseconds
    congestion_timeout: u64,

    /// Congestion window in bytes
    cwnd: u32,
}

impl UtpSocket {
    /// Creates a new UTP socket from the given address.
    ///
    /// The address type can be any implementor of the `ToSocketAddr` trait. See its documentation
    /// for concrete examples.
    ///
    /// If more than one valid address is specified, only the first will be used.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<UtpSocket> {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let connection_id = rand::random::<u16>();
        UdpSocket::bind(addr).map(|s|
            UtpSocket {
                socket: s,
                connected_to: addr,
                receiver_connection_id: connection_id,
                sender_connection_id: connection_id + 1,
                seq_nr: 1,
                ack_nr: 0,
                state: SocketState::New,
                incoming_buffer: Vec::new(),
                send_window: Vec::new(),
                unsent_queue: VecDeque::new(),
                duplicate_ack_count: 0,
                last_acked: 0,
                last_acked_timestamp: 0,
                last_dropped: 0,
                rtt: 0,
                rtt_variance: 0,
                pending_data: Vec::new(),
                curr_window: 0,
                remote_wnd_size: 0,
                current_delays: Vec::new(),
                base_delays: VecDeque::with_capacity(BASE_HISTORY),
                their_delay: 0,
                last_rollover: 0,
                congestion_timeout: INITIAL_CONGESTION_TIMEOUT,
                cwnd: INIT_CWND * MSS,
            })
    }

    /// Returns the socket address that this socket was created from.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Opens a connection to a remote host by hostname or IP address.
    ///
    /// The address type can be any implementor of the `ToSocketAddr` trait. See its documentation
    /// for concrete examples.
    ///
    /// If more than one valid address is specified, only the first will be used.
    pub fn connect<A: ToSocketAddrs>(other: A) -> Result<UtpSocket> {
        let addr = other.to_socket_addrs().unwrap().next().unwrap();
        let my_addr = match addr {
            SocketAddr::V4(_) => "0.0.0.0:0",
            SocketAddr::V6(_) => ":::0",
        };
        let mut socket = UtpSocket::bind(my_addr).unwrap();
        socket.connected_to = addr;

        let mut packet = Packet::new();
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(socket.receiver_connection_id);
        packet.set_seq_nr(socket.seq_nr);

        let mut len = 0;
        let mut buf = [0; BUF_SIZE];

        // let syn_timeout = socket.congestion_timeout;
        for _ in (0u8..5) {
            packet.set_timestamp_microseconds(now_microseconds());

            // Send packet
            debug!("Connecting to {}", socket.connected_to);
            try!(socket.socket.send_to(&packet.to_bytes()[..], socket.connected_to));
            socket.state = SocketState::SynSent;
            debug!("sent {:?}", packet);

            // Validate response
            // socket.socket.set_read_timeout(Some(syn_timeout));
            match socket.socket.recv_from(&mut buf) {
                // Ok((_read, src)) if src != socket.connected_to => continue,
                Ok((read, src)) => { socket.connected_to = src; len = read; break; },
                // Err(ref e) if e.kind == TimedOut => {
                //     debug!("Timed out, retrying");
                //     syn_timeout *= 2;
                //     continue;
                // },
                Err(e) => return Err(e),
            };
        }

        let addr = socket.connected_to;
        let packet = try!(Packet::from_bytes(&buf[..len]).or(Err(SocketError::InvalidPacket)));
        debug!("received {:?}", packet);
        try!(socket.handle_packet(&packet, addr));

        debug!("connected to: {}", socket.connected_to);

        return Ok(socket);
    }

    /// Gracefully closes connection to peer.
    ///
    /// This method allows both peers to receive all packets still in
    /// flight.
    pub fn close(&mut self) -> Result<()> {
        try!(self.flush());

        // Nothing to do if the socket's already closed or not connected
        if self.state == SocketState::Closed ||
            self.state == SocketState::New ||
            self.state == SocketState::SynSent {
            return Ok(());
        }

        let mut packet = Packet::new();
        packet.set_connection_id(self.sender_connection_id);
        packet.set_seq_nr(self.seq_nr);
        packet.set_ack_nr(self.ack_nr);
        packet.set_timestamp_microseconds(now_microseconds());
        packet.set_type(PacketType::Fin);

        // Send FIN
        try!(self.socket.send_to(&packet.to_bytes()[..], self.connected_to));
        self.state = SocketState::FinSent;

        // Receive JAKE
        let mut buf = [0; BUF_SIZE];
        while self.state != SocketState::Closed {
            try!(self.recv(&mut buf));
        }

        Ok(())
    }

    /// Receives data from socket.
    ///
    /// On success, returns the number of bytes read and the sender's address.
    /// Returns 0 bytes read after receiving a FIN packet when the remaining
    /// inflight packets are consumed.
    pub fn recv_from(&mut self, buf: &mut[u8]) -> Result<(usize,SocketAddr)> {
        let read = self.flush_incoming_buffer(buf);

        if read > 0 {
            return Ok((read, self.connected_to));
        } else {
            // If the socket received a reset packet and all data has been flushed, then it can't
            // receive anything else
            if self.state == SocketState::ResetReceived {
                return Err(Error::from(SocketError::ConnectionReset));
            }

            loop {
                // A closed socket with no pending data can only "read" 0 new bytes.
                if self.state == SocketState::Closed {
                    return Ok((0, self.connected_to));
                }

                match self.recv(buf) {
                    Ok((0, _src)) => continue,
                    Ok(x) => return Ok(x),
                    Err(e) => return Err(e)
                }
            }
        }
    }

    fn recv(&mut self, buf: &mut[u8]) -> Result<(usize,SocketAddr)> {
        let mut b = [0; BUF_SIZE + HEADER_SIZE];
        // if self.state != SocketState::New {
        //     debug!("setting read timeout of {} ms", self.congestion_timeout);
        //     self.socket.set_read_timeout(Some(self.congestion_timeout));
        // }
        let (read, src) = match self.socket.recv_from(&mut b) {
            // Err(ref e) if e.kind == TimedOut => {
            //     debug!("recv_from timed out");
            //     self.congestion_timeout = self.congestion_timeout * 2;
            //     self.cwnd = MSS;
            //     self.send_fast_resend_request();
            //     return Ok((0, self.connected_to));
            // },
            Ok(x) => x,
            Err(e) => return Err(e),
        };
        let packet = match Packet::from_bytes(&b[..read]) {
            Ok(packet) => packet,
            Err(e) => {
                debug!("{}", e);
                debug!("Ignoring invalid packet");
                return Ok((0, self.connected_to));
            }
        };
        debug!("received {:?}", packet);

        if let Some(pkt) = try!(self.handle_packet(&packet, src)) {
                let mut pkt = pkt;
                pkt.set_wnd_size(BUF_SIZE as u32);
                try!(self.socket.send_to(&pkt.to_bytes()[..], src));
                debug!("sent {:?}", pkt);
        }

        if packet.get_type() == PacketType::Data && packet.seq_nr().wrapping_sub(self.last_dropped) > 0 {
            self.insert_into_buffer(packet);
        }

        // Flush incoming buffer if possible
        let read = self.flush_incoming_buffer(buf);

        Ok((read, src))
    }

    fn prepare_reply(&self, original: &Packet, t: PacketType) -> Packet {
        let mut resp = Packet::new();
        resp.set_type(t);
        let self_t_micro: u32 = now_microseconds();
        let other_t_micro: u32 = original.timestamp_microseconds();
        resp.set_timestamp_microseconds(self_t_micro);
        resp.set_timestamp_difference_microseconds((self_t_micro - other_t_micro));
        resp.set_connection_id(self.sender_connection_id);
        resp.set_seq_nr(self.seq_nr);
        resp.set_ack_nr(self.ack_nr);

        resp
    }

    /// Removes a packet in the incoming buffer and updates the current acknowledgement number.
    fn advance_incoming_buffer(&mut self) -> Option<Packet> {
        if !self.incoming_buffer.is_empty() {
            let packet = self.incoming_buffer.remove(0);
            debug!("Removed packet from incoming buffer: {:?}", packet);
            self.ack_nr = packet.seq_nr();
            self.last_dropped = self.ack_nr;
            Some(packet)
        } else {
            None
        }
    }

    /// Discards sequential, ordered packets in incoming buffer, starting from
    /// the most recently acknowledged to the most recent, as long as there are
    /// no missing packets. The discarded packets' payload is written to the
    /// slice `buf`, starting in position `start`.
    /// Returns the last written index.
    fn flush_incoming_buffer(&mut self, buf: &mut [u8]) -> usize {
        fn unsafe_copy(src: &[u8], dst: &mut [u8]) -> usize {
            let max_len = min(src.len(), dst.len());
            unsafe {
                use std::ptr::copy;
                copy(src.as_ptr(), dst.as_mut_ptr(), max_len);
            }
            return max_len;
        }

        // Return pending data from a partially read packet
        if !self.pending_data.is_empty() {
            let flushed = unsafe_copy(&self.pending_data[..], buf);

            if flushed == self.pending_data.len() {
                self.pending_data.clear();
                self.advance_incoming_buffer();
            } else {
                self.pending_data = self.pending_data[flushed..].to_vec();
            }

            return flushed;
        }

        if !self.incoming_buffer.is_empty() &&
            (self.ack_nr == self.incoming_buffer[0].seq_nr() ||
             self.ack_nr + 1 == self.incoming_buffer[0].seq_nr())
        {
            let flushed = unsafe_copy(&self.incoming_buffer[0].payload[..], buf);

            if flushed == self.incoming_buffer[0].payload.len() {
                self.advance_incoming_buffer();
            } else {
                self.pending_data = self.incoming_buffer[0].payload[flushed..].to_vec();
            }

            return flushed;
        }

        return 0;
    }

    /// Sends data on the socket to the remote peer. On success, returns the number of bytes written.
    //
    // # Implementation details
    //
    // This method inserts packets into the send buffer and keeps trying to
    // advance the send window until an ACK corresponding to the last packet is
    // received.
    //
    // Note that the buffer passed to `send_to` might exceed the maximum packet
    // size, which will result in the data being split over several packets.
    pub fn send_to(&mut self, buf: &[u8]) -> Result<usize> {
        if self.state == SocketState::Closed {
            return Err(Error::from(SocketError::ConnectionClosed));
        }

        let total_length = buf.len();

        for chunk in buf.chunks(MSS as usize - HEADER_SIZE) {
            let mut packet = Packet::with_payload(chunk);
            packet.set_seq_nr(self.seq_nr);
            packet.set_ack_nr(self.ack_nr);
            packet.set_connection_id(self.sender_connection_id);

            self.unsent_queue.push_back(packet);

            // `OverflowingOps` is marked unstable, so we can't use `overflowing_add` here
            if self.seq_nr == ::std::u16::MAX {
                self.seq_nr = 0;
            } else {
                self.seq_nr += 1;
            }
        }

        // Send every packet in the queue
        try!(self.send());

        Ok(total_length)
    }

    /// Consumes acknowledgements for every pending packet.
    pub fn flush(&mut self) -> Result<()> {
        let mut buf = [0u8; BUF_SIZE];
        while !self.send_window.is_empty() {
            debug!("packets in send window: {}", self.send_window.len());
            try!(self.recv(&mut buf));
        }

        Ok(())
    }

    /// Sends every packet in the unsent packet queue.
    fn send(&mut self) -> Result<()> {
        while let Some(packet) = self.unsent_queue.pop_front() {
            let mut packet = packet;
            try!(self.send_packet(&mut packet));
            self.curr_window += packet.len() as u32;
            self.send_window.push(packet);
        }
        Ok(())
    }

    /// Send one packet.
    #[inline(always)]
    fn send_packet(&mut self, packet: &mut Packet) -> Result<()> {
        let dst = self.connected_to;
        debug!("current window: {}", self.send_window.len());
        let max_inflight = min(self.cwnd, self.remote_wnd_size);
        let max_inflight = max(MIN_CWND * MSS, max_inflight);
        while self.curr_window >= max_inflight && self.duplicate_ack_count < 3 {
            debug!("self.curr_window: {}", self.curr_window);
            debug!("max_inflight: {}", max_inflight);
            debug!("self.duplicate_ack_count: {}", self.duplicate_ack_count);
            let mut buf = [0; BUF_SIZE];
            try!(self.recv(&mut buf));
        }

        // TODO: Check if it still makes to send packet --- we might be trying to resend a lost
        // packet that was acknowledged in the previous recv loop

        packet.set_timestamp_microseconds(now_microseconds());
        packet.set_timestamp_difference_microseconds(self.their_delay);
        try!(self.socket.send_to(&packet.to_bytes()[..], dst));
        debug!("sent {:?}", packet);

        Ok(())
    }

    // Insert a new sample in the base delay list.
    //
    // The base delay list contains at most `BASE_HISTORY` samples, each sample is the minimum
    // measured over a period of a minute.
    fn update_base_delay(&mut self, base_delay: i64, now: i64) {
        let minute_in_microseconds = 60 * 10i64.pow(6);

        if self.base_delays.is_empty() || now - self.last_rollover > minute_in_microseconds {
            // Update last rollover
            self.last_rollover = now;

            // Drop the oldest sample, if need be
            if self.base_delays.len() == BASE_HISTORY {
                self.base_delays.pop_front();
            }

            // Insert new sample
            self.base_delays.push_back(base_delay);
        } else {
            // Replace sample for the current minute if the delay is lower
            let last_idx = self.base_delays.len() - 1;
            if base_delay < self.base_delays[last_idx] {
                self.base_delays[last_idx] = base_delay;
            }
        }
    }

    /// Inserts a new sample in the current delay list after removing samples older than one RTT, as
    /// specified in RFC6817.
    fn update_current_delay(&mut self, v: i64, now: i64) {
        // Remove samples more than one RTT old
        let rtt = self.rtt as i64 * 100;
        while !self.current_delays.is_empty() && now - self.current_delays[0].received_at > rtt {
            self.current_delays.remove(0);
        }

        // Insert new measurement
        self.current_delays.push(DelayDifferenceSample{ received_at: now, difference: v });
    }

    fn update_congestion_timeout(&mut self, current_delay: i32) {
        let delta = self.rtt - current_delay;
        self.rtt_variance += (delta.abs() - self.rtt_variance) / 4;
        self.rtt += (current_delay - self.rtt) / 8;
        self.congestion_timeout = max((self.rtt + self.rtt_variance * 4) as u64, MIN_CONGESTION_TIMEOUT);
        self.congestion_timeout = min(self.congestion_timeout, MAX_CONGESTION_TIMEOUT);

        debug!("current_delay: {}", current_delay);
        debug!("delta: {}", delta);
        debug!("self.rtt_variance: {}", self.rtt_variance);
        debug!("self.rtt: {}", self.rtt);
        debug!("self.congestion_timeout: {}", self.congestion_timeout);
    }

    /// Calculates the filtered current delay in the current window.
    ///
    /// The current delay is calculated through application of the exponential
    /// weighted moving average filter with smoothing factor 0.333 over the
    /// current delays in the current window.
    fn filtered_current_delay(&self) -> i64 {
        let input = self.current_delays.iter().map(|&ref x| x.difference).collect();
        ewma(input, 0.333) as i64
    }

    /// Calculates the lowest base delay in the current window.
    fn min_base_delay(&self) -> i64 {
        self.base_delays.iter().map(|x| *x).min().unwrap_or(0)
    }

    /// Builds the selective acknowledgment extension data for usage in packets.
    fn build_selective_ack(&self) -> Vec<u8> {
        let stashed = self.incoming_buffer.iter()
            .filter(|&pkt| pkt.seq_nr() > self.ack_nr);

        let mut sack = Vec::new();
        for packet in stashed {
            let diff = packet.seq_nr() - self.ack_nr - 2;
            let byte = (diff / 8) as usize;
            let bit = (diff % 8) as usize;

            // Make sure the amount of elements in the SACK vector is a
            // multiple of 4 and enough to represent the lost packets
            while byte >= sack.len() || sack.len() % 4 != 0 {
                sack.push(0u8);
            }

            sack[byte] |= 1 << bit;
        }

        return sack;
    }

    fn resend_lost_packet(&mut self, lost_packet_nr: u16) {
        debug!("---> resend_lost_packet({}) <---", lost_packet_nr);
        match self.send_window.iter().position(|pkt| pkt.seq_nr() == lost_packet_nr) {
            None => debug!("Packet {} not found", lost_packet_nr),
            Some(position) => {
                debug!("self.send_window.len(): {}", self.send_window.len());
                debug!("position: {}", position);
                let mut packet = self.send_window[position].clone();
                // FIXME: Unchecked result
                let _ = self.send_packet(&mut packet);

                // We intentionally don't increase `curr_window` because otherwise a packet's length
                // would be counted more than once
            }
        }
        debug!("---> END resend_lost_packet <---");
    }

    /// Forgets sent packets that were acknowledged by the remote peer.
    fn advance_send_window(&mut self) {
        // The reason I'm not removing the first element in a loop while its sequence number is
        // smaller than `last_acked` is because of wrapping sequence numbers, which would create the
        // sequence [..., 65534, 65535, 0, 1, ...]. If `last_acked` is smaller than the first
        // packet's sequence number because of wraparound (for instance, 1), no packets would be
        // removed, as the condition `seq_nr < last_acked` would fail immediately.
        //
        // On the other hand, I can't keep removing the first packet in a loop until its sequence
        // number matches `last_acked` because it might never match, and in that case no packets
        // should be removed.
        if let Some(position) = self.send_window.iter()
            .position(|pkt| pkt.seq_nr() == self.last_acked)
        {
            for _ in (0..position + 1) {
                let packet = self.send_window.remove(0);
                self.curr_window -= packet.len() as u32;
            }
        }
        debug!("self.curr_window: {}", self.curr_window);
    }

    /// Handles an incoming packet, updating socket state accordingly.
    ///
    /// Returns the appropriate reply packet, if needed.
    fn handle_packet(&mut self, packet: &Packet, src: SocketAddr) -> Result<Option<Packet>> {
        debug!("({:?}, {:?})", self.state, packet.get_type());

        // Acknowledge only if the packet strictly follows the previous one
        if packet.seq_nr().wrapping_sub(self.ack_nr) == 1 {
            self.ack_nr = packet.seq_nr();
        }

        // Reset connection if connection id doesn't match and this isn't a SYN
        if packet.get_type() != PacketType::Syn &&
            self.state != SocketState::SynSent &&
            !(packet.connection_id() == self.sender_connection_id ||
              packet.connection_id() == self.receiver_connection_id) {
            return Ok(Some(self.prepare_reply(packet, PacketType::Reset)));
        }

        // Update remote window size
        self.remote_wnd_size = packet.wnd_size();
        debug!("self.remote_wnd_size: {}", self.remote_wnd_size);

        // Update remote peer's delay between them sending the packet and us receiving it
        let now = now_microseconds();
        self.their_delay = if now > packet.timestamp_microseconds() {
            now - packet.timestamp_microseconds()
        } else {
            packet.timestamp_microseconds() - now
        };
        debug!("self.their_delay: {}", self.their_delay);

        match (self.state, packet.get_type()) {
            (SocketState::New, PacketType::Syn) => {
                self.connected_to = src;
                self.ack_nr = packet.seq_nr();
                self.seq_nr = rand::random();
                self.receiver_connection_id = packet.connection_id() + 1;
                self.sender_connection_id = packet.connection_id();
                self.state = SocketState::Connected;
                self.last_dropped = self.ack_nr;

                Ok(Some(self.prepare_reply(packet, PacketType::State)))
            },
            (_, PacketType::Syn) => {
                Ok(Some(self.prepare_reply(packet, PacketType::Reset)))
            }
            (SocketState::SynSent, PacketType::State) => {
                self.connected_to = src;
                self.ack_nr = packet.seq_nr();
                self.seq_nr += 1;
                self.state = SocketState::Connected;
                self.last_acked = packet.ack_nr();
                self.last_acked_timestamp = now_microseconds();
                Ok(None)
            },
            (SocketState::SynSent, _) => {
                Err(Error::from(SocketError::InvalidReply))
            }
            (SocketState::Connected, PacketType::Data) => {
                Ok(self.handle_data_packet(packet))
            },
            (SocketState::Connected, PacketType::State) => {
                self.handle_state_packet(packet);
                Ok(None)
            },
            (SocketState::Connected, PacketType::Fin) |
            (SocketState::FinSent,   PacketType::Fin) => {
                // If all packets are received and handled
                if packet.ack_nr() == self.seq_nr {
                    self.state = SocketState::Closed;
                    Ok(Some(self.prepare_reply(packet, PacketType::State)))
                } else {
                    debug!("FIN received but there are missing packets");
                    self.handle_state_packet(packet);
                    Ok(None)
                }
            }
            (SocketState::FinSent, PacketType::State) => {
                if packet.ack_nr() == self.seq_nr {
                    self.state = SocketState::Closed;
                } else {
                    self.handle_state_packet(packet);
                }
                Ok(None)
            }
            (_, PacketType::Reset) => {
                self.state = SocketState::ResetReceived;
                Err(Error::from(SocketError::ConnectionReset))
            },
            (state, ty) => {
                let message = format!("Unimplemented handling for ({:?},{:?})", state, ty);
                debug!("{}", message);
                Err(Error::new(ErrorKind::Other, message))
            }
        }
    }

    fn handle_data_packet(&mut self, packet: &Packet) -> Option<Packet> {
        let mut reply = self.prepare_reply(packet, PacketType::State);

        if packet.seq_nr().wrapping_sub(self.ack_nr) > 1 {
            debug!("current ack_nr ({}) is behind received packet seq_nr ({})",
                   self.ack_nr, packet.seq_nr());

            // Set SACK extension payload if the packet is not in order
            let sack = self.build_selective_ack();

            if sack.len() > 0 {
                reply.set_sack(sack);
            }
        }

        Some(reply)
    }

    fn queuing_delay(&self) -> i64 {
        let filtered_current_delay = self.filtered_current_delay();
        let min_base_delay = self.min_base_delay();
        let queuing_delay = filtered_current_delay - min_base_delay;

        debug!("filtered_current_delay: {}", filtered_current_delay);
        debug!("min_base_delay: {}", min_base_delay);
        debug!("queuing_delay: {}", queuing_delay);

        return queuing_delay;
    }

    /// Calculates the new congestion window size, increasing it or decreasing it.
    ///
    /// This is the core of uTP, the [LEDBAT][ledbat_rfc] congestion algorithm. It depends on
    /// estimating the queuing delay between the two peers, and adjusting the congestion window
    /// accordingly.
    ///
    /// `off_target` is a normalized value representing the difference between the current queuing
    /// delay and a fixed target delay (`TARGET`). `off_target` ranges between -1.0 and 1.0. A
    /// positive value makes the congestion window increase, while a negative value makes the
    /// congestion window decrease.
    ///
    /// `bytes_newly_acked` is the number of bytes acknowledged by an inbound `State` packet. It may
    /// be the size of the packet explicitly acknowledged by the inbound packet (i.e., with sequence
    /// number equal to the inbound packet's acknowledgement number), or every packet implicitly
    /// acknowledged (every packet with sequence number between the previous inbound `State` packet's
    /// acknowledgement number and the current inbound `State` packet's acknowledgement number).
    ///
    ///[ledbat_rfc]: https://tools.ietf.org/html/rfc6817
    fn update_congestion_window(&mut self, off_target: f64, bytes_newly_acked: u32) {
        let flightsize = self.curr_window;

        let cwnd_increase = GAIN * off_target * bytes_newly_acked as f64 * MSS as f64;
        let cwnd_increase = cwnd_increase / self.cwnd as f64;
        debug!("cwnd_increase: {}", cwnd_increase);

        self.cwnd = (self.cwnd as f64 + cwnd_increase) as u32;
        let max_allowed_cwnd = flightsize + ALLOWED_INCREASE * MSS;
        self.cwnd = min(self.cwnd, max_allowed_cwnd);
        self.cwnd = max(self.cwnd, MIN_CWND * MSS);

        debug!("cwnd: {}", self.cwnd);
        debug!("max_allowed_cwnd: {}", max_allowed_cwnd);
    }

    fn handle_state_packet(&mut self, packet: &Packet) {
        if packet.ack_nr() == self.last_acked {
            self.duplicate_ack_count += 1;
        } else {
            self.last_acked = packet.ack_nr();
            self.last_acked_timestamp = now_microseconds();
            self.duplicate_ack_count = 1;
        }

        // Update congestion window size
        if let Some(index) = self.send_window.iter().position(|p| packet.ack_nr() == p.seq_nr()) {
            // Calculate the sum of the size of every packet implicitly and explictly acknowledged
            // by the inbout packet (i.e., every packet whose sequence number precedes the inbound
            // packet's acknowledgement number, plus the packet whose sequence number matches)
            let bytes_newly_acked = self.send_window.iter()
                .take(index + 1)
                .fold(0, |acc, p| acc + p.len());

            // Update base and current delay
            let now = now_microseconds() as i64;
            let our_delay = now - self.send_window[index].timestamp_microseconds() as i64;
            debug!("our_delay: {}", our_delay);
            self.update_base_delay(our_delay, now);
            self.update_current_delay(our_delay, now);

            let off_target: f64 = (TARGET as f64 - self.queuing_delay() as f64) / TARGET as f64;
            debug!("off_target: {}", off_target);

            self.update_congestion_window(off_target, bytes_newly_acked as u32);

            // Update congestion timeout
            let rtt = (TARGET - off_target as i64) / 1000; // in milliseconds
            self.update_congestion_timeout(rtt as i32);
        }

        let mut packet_loss_detected: bool = !self.send_window.is_empty() &&
                                             self.duplicate_ack_count == 3;

        // Process extensions, if any
        for extension in packet.extensions.iter() {
            if extension.get_type() == ExtensionType::SelectiveAck {
                // If three or more packets are acknowledged past the implicit missing one,
                // assume it was lost.
                if extension.iter().count_ones() >= 3 {
                    self.resend_lost_packet(packet.ack_nr() + 1);
                    packet_loss_detected = true;
                }

                for seq_nr in extension.iter().enumerate()
                    .filter(|&(_idx, received)| !received)
                    .map(|(idx, _received)| packet.ack_nr() + 2 + idx as u16) {
                    if self.send_window.last().map(|p| seq_nr < p.seq_nr()).unwrap_or(false) {
                        debug!("SACK: packet {} lost", seq_nr);
                        self.resend_lost_packet(seq_nr);
                        packet_loss_detected = true;
                    } else {
                        break;
                    }
                }
            } else {
                debug!("Unknown extension {:?}, ignoring", extension.get_type());
            }
        }

        // Three duplicate ACKs, must resend packets since `ack_nr + 1`
        // TODO: checking if the send buffer isn't empty isn't a
        // foolproof way to differentiate between triple-ACK and three
        // keep alives spread in time
        if !self.send_window.is_empty() && self.duplicate_ack_count == 3 {
            for i in (0..self.send_window.len()) {
                let seq_nr = self.send_window[i].seq_nr();
                if seq_nr <= packet.ack_nr() { continue; }
                self.resend_lost_packet(seq_nr);
            }
        }

        // Packet lost, halve the congestion window
        if packet_loss_detected {
            debug!("packet loss detected, halving congestion window");
            self.cwnd = max(self.cwnd / 2, MIN_CWND * MSS);
            debug!("cwnd: {}", self.cwnd);
        }

        // Success, advance send window
        self.advance_send_window();
    }

    /// Inserts a packet into the socket's buffer.
    ///
    /// The packet is inserted in such a way that the buffer is
    /// ordered ascendingly by their sequence number. This allows
    /// storing packets that were received out of order.
    ///
    /// Inserting a duplicate of a packet will replace the one in the buffer if
    /// it's more recent (larger timestamp).
    fn insert_into_buffer(&mut self, packet: Packet) {
        // Immediately push to the end if the packet's sequence number comes after the last
        // packet's.
        if self.incoming_buffer.last().map(|p| packet.seq_nr() > p.seq_nr()).unwrap_or(false) {
            self.incoming_buffer.push(packet);
        } else {
            // Find index following the most recent packet before the one we wish to insert
            let i = self.incoming_buffer.iter().filter(|p| p.seq_nr() < packet.seq_nr()).count();

            // Remove packet if it's a duplicate
            if self.incoming_buffer.get(i).map(|p| p.seq_nr() == packet.seq_nr()).unwrap_or(false) {
                self.incoming_buffer.remove(i);
            }

            self.incoming_buffer.insert(i, packet);
        }
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// A structure representing a socket server.
///
/// # Examples
///
/// ```no_run
/// use utp::{UtpListener, UtpSocket};
/// use std::thread;
///
/// fn handle_client(socket: UtpSocket) {
///     // ...
/// }
///
/// fn main() {
///     // Create a listener
///     let addr = "127.0.0.1:8080";
///     let listener = UtpListener::bind(addr).unwrap();
///
///     for connection in listener.incoming() {
///         // Spawn a new handler for each new connection
///         match connection {
///             Ok((socket, _src)) => { thread::spawn(move || { handle_client(socket) }); },
///             _ => ()
///         }
///     }
/// }
/// ```
pub struct UtpListener {
    /// The public facing UDP socket
    socket: UdpSocket,
}

impl UtpListener {
    /// Creates a new `UtpListener` bound to a specific address.
    ///
    /// The resulting listener is ready for accepting connections.
    ///
    /// The address type can be any implementor of the `ToSocketAddr` trait. See its documentation
    /// for concrete examples.
    ///
    /// If more than one valid address is specified, only the first will be used.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<UtpListener> {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        UdpSocket::bind(addr).and_then(|s| Ok(UtpListener { socket: s}))
    }

    /// Accepts a new incoming connection from this listener.
    ///
    /// This function will block the caller until a new uTP connection is established. When
    /// established, the corresponding `UtpSocket` and the peer's remote address will be returned.
    ///
    /// Notice that the resulting `UtpSocket` is bound to a different local port than the public
    /// listening port (which `UtpListener` holds). This may confuse the remote peer!
    pub fn accept(&self) -> Result<(UtpSocket, SocketAddr)> {
        let mut buf = [0; BUF_SIZE];

        match self.socket.recv_from(&mut buf) {
            Ok((nread, src)) => {
                let packet = try!(Packet::from_bytes(&buf[..nread]).or(Err(SocketError::InvalidPacket)));

                // Ignore non-SYN packets
                if packet.get_type() != PacketType::Syn {
                    return Err(Error::from(SocketError::InvalidPacket));
                }

                // The address of the new socket will depend on the type of the listener.
                let inner_socket = match self.socket.local_addr().unwrap() {
                    SocketAddr::V4(_) => UdpSocket::bind("0.0.0.0:0"),
                    SocketAddr::V6(_) => UdpSocket::bind(":::0"),
                };

                let mut socket = UtpSocket {
                    socket: inner_socket.unwrap(),
                    connected_to: src,
                    receiver_connection_id: 0,
                    sender_connection_id: 0,
                    seq_nr: 1,
                    ack_nr: 0,
                    state: SocketState::New,
                    incoming_buffer: Vec::new(),
                    send_window: Vec::new(),
                    unsent_queue: VecDeque::new(),
                    duplicate_ack_count: 0,
                    last_acked: 0,
                    last_acked_timestamp: 0,
                    last_dropped: 0,
                    rtt: 0,
                    rtt_variance: 0,
                    pending_data: Vec::new(),
                    curr_window: 0,
                    remote_wnd_size: 0,
                    current_delays: Vec::new(),
                    base_delays: VecDeque::with_capacity(BASE_HISTORY),
                    their_delay: 0,
                    last_rollover: 0,
                    congestion_timeout: INITIAL_CONGESTION_TIMEOUT,
                    cwnd: INIT_CWND * MSS,
                };

                // Establish connection with remote peer
                match socket.handle_packet(&packet, src) {
                    Ok(Some(reply)) => { try!(socket.socket.send_to(&reply.to_bytes()[..], src)) },
                    Ok(None) => return Err(Error::new(ErrorKind::Other, "Unexpected error handling packet")),
                    Err(e) => return Err(e)
                };

                Ok((socket, src))
            },
            Err(e) => Err(e)
        }
    }

    /// Returns an iterator over the connections being received by this listener.
    ///
    /// The returned iterator will never return `None`.
    pub fn incoming(&self) -> Incoming {
        Incoming { listener: self }
    }

    /// Returns the local socket address of this listener.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr()
    }
}

pub struct Incoming<'a> { listener: &'a UtpListener }

impl<'a> Iterator for Incoming<'a> {
    type Item = Result<(UtpSocket, SocketAddr)>;

    fn next(&mut self) -> Option<Result<(UtpSocket, SocketAddr)>> {
        Some(self.listener.accept())
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::net::ToSocketAddrs;
    use std::io::ErrorKind;
    use super::{UtpSocket, UtpListener, SocketState, BUF_SIZE};
    use packet::{Packet, PacketType, Encodable, Decodable};
    use util::now_microseconds;
    use rand;

    macro_rules! iotry {
        ($e:expr) => (match $e { Ok(e) => e, Err(e) => panic!("{:?}", e) })
    }

    fn next_test_port() -> u16 {
        use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
        static NEXT_OFFSET: AtomicUsize = ATOMIC_USIZE_INIT;
        const BASE_PORT: u16 = 9600;
        BASE_PORT + NEXT_OFFSET.fetch_add(1, Ordering::Relaxed) as u16
    }

    fn next_test_ip4<'a>() -> (&'a str, u16) {
        ("127.0.0.1", next_test_port())
    }

    fn next_test_ip6<'a>() -> (&'a str, u16) {
        ("::1", next_test_port())
    }

    #[test]
    fn test_socket_ipv4() {
        let server_addr = next_test_ip4();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        assert!(server.state == SocketState::New);

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);
            // Check proper difference in client's send connection id and receive connection id
            assert_eq!(client.sender_connection_id, client.receiver_connection_id + 1);
            assert_eq!(client.connected_to,
                       server_addr.to_socket_addrs().unwrap().next().unwrap());
            iotry!(client.close());
            drop(client);
        });

        let mut buf = [0u8; BUF_SIZE];
        match server.recv_from(&mut buf) {
            e => println!("{:?}", e),
        }
        // After establishing a new connection, the server's ids are a mirror of the client's.
        assert_eq!(server.receiver_connection_id, server.sender_connection_id + 1);

        assert!(server.state == SocketState::Closed);
        drop(server);
    }

    #[test]
    fn test_socket_ipv6() {
        let server_addr = next_test_ip6();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        assert!(server.state == SocketState::New);

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);
            // Check proper difference in client's send connection id and receive connection id
            assert_eq!(client.sender_connection_id, client.receiver_connection_id + 1);
            assert_eq!(client.connected_to,
                       server_addr.to_socket_addrs().unwrap().next().unwrap());
            iotry!(client.close());
            drop(client);
        });

        let mut buf = [0u8; BUF_SIZE];
        match server.recv_from(&mut buf) {
            e => println!("{:?}", e),
        }
        // After establishing a new connection, the server's ids are a mirror of the client's.
        assert_eq!(server.receiver_connection_id, server.sender_connection_id + 1);

        assert!(server.state == SocketState::Closed);
        drop(server);
    }

    #[test]
    fn test_recvfrom_on_closed_socket() {
        let server_addr = next_test_ip4();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        assert!(server.state == SocketState::New);

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);
            assert!(client.close().is_ok());
        });

        // Make the server listen for incoming connections until the end of the input
        let mut buf = [0u8; BUF_SIZE];
        let _resp = server.recv_from(&mut buf);
        assert!(server.state == SocketState::Closed);

        // Trying to receive again returns Ok(0) [EndOfFile]
        match server.recv_from(&mut buf) {
            Ok((0, _src)) => {},
            e => panic!("Expected Ok(0), got {:?}", e),
        }
        assert_eq!(server.state, SocketState::Closed);
    }

    #[test]
    fn test_sendto_on_closed_socket() {
        let server_addr = next_test_ip4();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        assert!(server.state == SocketState::New);

        thread::spawn(move || {
            let client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);
            let mut client = client;
            iotry!(client.close());
        });

        // Make the server listen for incoming connections
        let mut buf = [0u8; BUF_SIZE];
        let (_read, _src) = iotry!(server.recv_from(&mut buf));
        assert_eq!(server.state, SocketState::Closed);

        // Trying to send to the socket after closing it raises an error
        match server.send_to(&buf) {
            Err(ref e) if e.kind() == ErrorKind::NotConnected => (),
            v => panic!("expected {:?}, got {:?}", ErrorKind::NotConnected, v),
        }
    }

    #[test]
    fn test_acks_on_socket() {
        use std::sync::mpsc::channel;
        let server_addr = next_test_ip4();
        let (tx, rx) = channel();

        let server = iotry!(UtpSocket::bind(server_addr));

        thread::spawn(move || {
            // Make the server listen for incoming connections
            let mut server = server;
            let mut buf = [0u8; BUF_SIZE];
            let _resp = server.recv(&mut buf);
            tx.send(server.seq_nr).unwrap();

            // Close the connection
            iotry!(server.recv_from(&mut buf));

            drop(server);
        });

        let mut client = iotry!(UtpSocket::connect(server_addr));
        assert!(client.state == SocketState::Connected);
        let sender_seq_nr = rx.recv().unwrap();
        let ack_nr = client.ack_nr;
        assert!(ack_nr != 0);
        assert!(ack_nr == sender_seq_nr);
        assert!(client.close().is_ok());

        // The reply to both connect (SYN) and close (FIN) should be
        // STATE packets, which don't increase the sequence number
        // and, hence, the receiver's acknowledgement number.
        assert!(client.ack_nr == ack_nr);
        drop(client);
    }

    #[test]
    fn test_handle_packet() {
        //fn test_connection_setup() {
        let initial_connection_id: u16 = rand::random();
        let sender_connection_id = initial_connection_id + 1;
        let (server_addr, client_addr) = (next_test_ip4().to_socket_addrs().unwrap().next().unwrap(),
                                          next_test_ip4().to_socket_addrs().unwrap().next().unwrap());
        let mut socket = iotry!(UtpSocket::bind(server_addr));

        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(initial_connection_id);

        // Do we have a response?
        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());

        // Is is of the correct type?
        let response = response.unwrap();
        assert!(response.get_type() == PacketType::State);

        // Same connection id on both ends during connection establishment
        assert!(response.connection_id() == packet.connection_id());

        // Response acknowledges SYN
        assert!(response.ack_nr() == packet.seq_nr());

        // No payload?
        assert!(response.payload.is_empty());
        //}

        // ---------------------------------

        // fn test_connection_usage() {
        let old_packet = packet;
        let old_response = response;

        let mut packet = Packet::new();
        packet.set_type(PacketType::Data);
        packet.set_connection_id(sender_connection_id);
        packet.set_seq_nr(old_packet.seq_nr() + 1);
        packet.set_ack_nr(old_response.seq_nr());

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());

        let response = response.unwrap();
        assert!(response.get_type() == PacketType::State);

        // Sender (i.e., who initated connection and sent SYN) has connection id
        // equal to initial connection id + 1
        // Receiver (i.e., who accepted connection) has connection id equal to
        // initial connection id
        assert!(response.connection_id() == initial_connection_id);
        assert!(response.connection_id() == packet.connection_id() - 1);

        // Previous packets should be ack'ed
        assert!(response.ack_nr() == packet.seq_nr());

        // Responses with no payload should not increase the sequence number
        assert!(response.payload.is_empty());
        assert!(response.seq_nr() == old_response.seq_nr());
        // }

        //fn test_connection_teardown() {
        let old_packet = packet;
        let old_response = response;

        let mut packet = Packet::new();
        packet.set_type(PacketType::Fin);
        packet.set_connection_id(sender_connection_id);
        packet.set_seq_nr(old_packet.seq_nr() + 1);
        packet.set_ack_nr(old_response.seq_nr());

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());

        let response = response.unwrap();

        assert!(response.get_type() == PacketType::State);

        // FIN packets have no payload but the sequence number shouldn't increase
        assert!(packet.seq_nr() == old_packet.seq_nr() + 1);

        // Nor should the ACK packet's sequence number
        assert!(response.seq_nr() == old_response.seq_nr());

        // FIN should be acknowledged
        assert!(response.ack_nr() == packet.seq_nr());

        //}
    }

    #[test]
    fn test_response_to_keepalive_ack() {
        // Boilerplate test setup
        let initial_connection_id: u16 = rand::random();
        let (server_addr, client_addr) = (next_test_ip4().to_socket_addrs().unwrap().next().unwrap(),
                                          next_test_ip4().to_socket_addrs().unwrap().next().unwrap());
        let mut socket = iotry!(UtpSocket::bind(server_addr));

        // Establish connection
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(initial_connection_id);

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        let response = response.unwrap();
        assert!(response.get_type() == PacketType::State);

        let old_packet = packet;
        let old_response = response;

        // Now, send a keepalive packet
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::State);
        packet.set_connection_id(initial_connection_id);
        packet.set_seq_nr(old_packet.seq_nr() + 1);
        packet.set_ack_nr(old_response.seq_nr());

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_none());

        // Send a second keepalive packet, identical to the previous one
        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_none());

        // Mark socket as closed
        socket.state = SocketState::Closed;
    }

    #[test]
    fn test_response_to_wrong_connection_id() {
        // Boilerplate test setup
        let initial_connection_id: u16 = rand::random();
        let (server_addr, client_addr) = (next_test_ip4().to_socket_addrs().unwrap().next().unwrap(),
                                          next_test_ip4().to_socket_addrs().unwrap().next().unwrap());
        let mut socket = iotry!(UtpSocket::bind(server_addr));

        // Establish connection
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(initial_connection_id);

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        assert!(response.unwrap().get_type() == PacketType::State);

        // Now, disrupt connection with a packet with an incorrect connection id
        let new_connection_id = initial_connection_id.wrapping_mul(2);

        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::State);
        packet.set_connection_id(new_connection_id);

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());

        let response = response.unwrap();
        assert!(response.get_type() == PacketType::Reset);
        assert!(response.ack_nr() == packet.seq_nr());

        // Mark socket as closed
        socket.state = SocketState::Closed;
    }

    #[test]
    fn test_unordered_packets() {
        // Boilerplate test setup
        let initial_connection_id: u16 = rand::random();
        let (server_addr, client_addr) = (next_test_ip4().to_socket_addrs().unwrap().next().unwrap(),
                                        next_test_ip4().to_socket_addrs().unwrap().next().unwrap());
        let mut socket = iotry!(UtpSocket::bind(server_addr));

        // Establish connection
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(initial_connection_id);

        let response = socket.handle_packet(&packet, client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        let response = response.unwrap();
        assert!(response.get_type() == PacketType::State);

        let old_packet = packet;
        let old_response = response;

        let mut window: Vec<Packet> = Vec::new();

        // Now, send a keepalive packet
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Data);
        packet.set_connection_id(initial_connection_id);
        packet.set_seq_nr(old_packet.seq_nr() + 1);
        packet.set_ack_nr(old_response.seq_nr());
        packet.payload = vec!(1,2,3);
        window.push(packet);

        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Data);
        packet.set_connection_id(initial_connection_id);
        packet.set_seq_nr(old_packet.seq_nr() + 2);
        packet.set_ack_nr(old_response.seq_nr());
        packet.payload = vec!(4,5,6);
        window.push(packet);

        // Send packets in reverse order
        let response = socket.handle_packet(&window[1], client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());
        let response = response.unwrap();
        assert!(response.ack_nr() != window[1].seq_nr());

        let response = socket.handle_packet(&window[0], client_addr);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert!(response.is_some());

        // Mark socket as closed
        socket.state = SocketState::Closed;
    }

    #[test]
    fn test_socket_unordered_packets() {
        let server_addr = next_test_ip4();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        assert!(server.state == SocketState::New);

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);
            // Check proper difference in client's send connection id and receive connection id
            assert_eq!(client.sender_connection_id, client.receiver_connection_id + 1);
            let s = client.socket.try_clone().ok().expect("Error cloning internal UDP socket");
            let mut window: Vec<Packet> = Vec::new();

            for data in (1..13u8).collect::<Vec<u8>>()[..].chunks(3) {
                let mut packet = Packet::new();
                packet.set_wnd_size(BUF_SIZE as u32);
                packet.set_type(PacketType::Data);
                packet.set_connection_id(client.sender_connection_id);
                packet.set_seq_nr(client.seq_nr);
                packet.set_ack_nr(client.ack_nr);
                packet.payload = data.to_vec();
                window.push(packet.clone());
                client.send_window.push(packet.clone());
                client.seq_nr += 1;
                client.curr_window += packet.len() as u32;
            }

            let mut packet = Packet::new();
            packet.set_wnd_size(BUF_SIZE as u32);
            packet.set_type(PacketType::Fin);
            packet.set_connection_id(client.sender_connection_id);
            packet.set_seq_nr(client.seq_nr);
            packet.set_ack_nr(client.ack_nr);
            window.push(packet);
            client.seq_nr += 1;

            iotry!(s.send_to(&window[3].to_bytes()[..], server_addr));
            iotry!(s.send_to(&window[2].to_bytes()[..], server_addr));
            iotry!(s.send_to(&window[1].to_bytes()[..], server_addr));
            iotry!(s.send_to(&window[0].to_bytes()[..], server_addr));
            iotry!(s.send_to(&window[4].to_bytes()[..], server_addr));

            for _ in (0u8..2) {
                let mut buf = [0; BUF_SIZE];
                iotry!(s.recv_from(&mut buf));
            }
        });

        let mut buf = [0; BUF_SIZE];
        let expected: Vec<u8> = (1..13u8).collect();
        let mut received: Vec<u8> = vec!();
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => received.extend(buf[..len].to_vec()),
                Err(e) => panic!("{:?}", e)
            }
        }

        // After establishing a new connection, the server's ids are a mirror of the client's.
        assert_eq!(server.receiver_connection_id, server.sender_connection_id + 1);
        assert_eq!(server.state, SocketState::Closed);
        assert_eq!(received.len(), expected.len());
        assert_eq!(received, expected);
    }

    #[test]
    fn test_response_to_triple_ack() {
        let server_addr = next_test_ip4();
        let mut server = iotry!(UtpSocket::bind(server_addr));

        // Fits in a packet
        const LEN: usize = 1024;
        let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
        let d = data.clone();
        assert_eq!(LEN, data.len());

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            iotry!(client.send_to(&d[..]));
            iotry!(client.close());
        });

        let mut buf = [0; BUF_SIZE];
        // Expect SYN
        iotry!(server.recv(&mut buf));

        // Receive data
        let data_packet = match server.socket.recv_from(&mut buf) {
            Ok((read, _src)) => iotry!(Packet::from_bytes(&buf[..read])),
            Err(e) => panic!("{}", e),
        };
        assert_eq!(data_packet.get_type(), PacketType::Data);
        assert_eq!(data_packet.payload, data);
        assert_eq!(data_packet.payload.len(), data.len());

        // Send triple ACK
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::State);
        packet.set_seq_nr(server.seq_nr);
        packet.set_ack_nr(data_packet.seq_nr() - 1);
        packet.set_connection_id(server.sender_connection_id);

        for _ in (0u8..3) {
            iotry!(server.socket.send_to(&packet.to_bytes()[..], server.connected_to));
        }

        // Receive data again and check that it's the same we reported as missing
        let client_addr = server.connected_to;
        match server.socket.recv_from(&mut buf) {
            Ok((0, _)) => panic!("Received 0 bytes from socket"),
            Ok((read, _src)) => {
                let packet = iotry!(Packet::from_bytes(&buf[..read]));
                assert_eq!(packet.get_type(), PacketType::Data);
                assert_eq!(packet.seq_nr(), data_packet.seq_nr());
                assert!(packet.payload == data_packet.payload);
                let response = server.handle_packet(&packet, client_addr);
                assert!(response.is_ok());
                let response = response.unwrap();
                assert!(response.is_some());
                let response = response.unwrap();
                iotry!(server.socket.send_to(&response.to_bytes()[..], server.connected_to));
            },
            Err(e) => panic!("{}", e),
        }

        // Receive close
        iotry!(server.recv_from(&mut buf));
    }

    // #[test]
    // #[ignore]
    // // `std::net::UdpSocket` no longer supports timeouts, so this test is deprecated for now.
    // fn test_socket_timeout_request() {
    //     let (server_addr, client_addr) = (next_test_ip4().to_socket_addrs().unwrap().next().unwrap(),
    //                                       next_test_ip4().to_socket_addrs().unwrap().next().unwrap());

    //     let client = iotry!(UtpSocket::bind(client_addr));
    //     let mut server = iotry!(UtpSocket::bind(server_addr));
    //     const LEN: usize = 512;
    //     let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
    //     let d = data.clone();

    //     assert!(server.state == SocketState::New);
    //     assert!(client.state == SocketState::New);

    //     // Check proper difference in client's send connection id and receive connection id
    //     assert_eq!(client.sender_connection_id, client.receiver_connection_id + 1);

    //     thread::spawn(move || {
    //         let mut client = iotry!(UtpSocket::connect(server_addr));
    //         assert!(client.state == SocketState::Connected);
    //         assert_eq!(client.connected_to, server_addr);
    //         iotry!(client.send_to(&d[..]));
    //         drop(client);
    //     });

    //     let mut buf = [0u8; BUF_SIZE];
    //     match server.recv(&mut buf) {
    //         e => println!("{:?}", e),
    //     }
    //     // After establishing a new connection, the server's ids are a mirror of the client's.
    //     assert_eq!(server.receiver_connection_id, server.sender_connection_id + 1);
    //     assert_eq!(server.connected_to, client_addr);

    //     assert!(server.state == SocketState::Connected);

    //     // Purposefully read from UDP socket directly and discard it, in order
    //     // to behave as if the packet was lost and thus trigger the timeout
    //     // handling in the *next* call to `UtpSocket.recv_from`.
    //     iotry!(server.socket.recv_from(&mut buf));

    //     // Set a much smaller than usual timeout, for quicker test completion
    //     server.congestion_timeout = 50;

    //     // Now wait for the previously discarded packet
    //     loop {
    //         match server.recv_from(&mut buf) {
    //             Ok((0, _)) => continue,
    //             Ok(_) => break,
    //             Err(e) => panic!("{:?}", e),
    //         }
    //     }

    //     drop(server);
    // }

    #[test]
    fn test_sorted_buffer_insertion() {
        let server_addr = next_test_ip4();
        let mut socket = iotry!(UtpSocket::bind(server_addr));

        let mut packet = Packet::new();
        packet.set_seq_nr(1);

        assert!(socket.incoming_buffer.is_empty());

        socket.insert_into_buffer(packet.clone());
        assert_eq!(socket.incoming_buffer.len(), 1);

        packet.set_seq_nr(2);
        packet.set_timestamp_microseconds(128);

        socket.insert_into_buffer(packet.clone());
        assert_eq!(socket.incoming_buffer.len(), 2);
        assert_eq!(socket.incoming_buffer[1].seq_nr(), 2);
        assert_eq!(socket.incoming_buffer[1].timestamp_microseconds(), 128);

        packet.set_seq_nr(3);
        packet.set_timestamp_microseconds(256);

        socket.insert_into_buffer(packet.clone());
        assert_eq!(socket.incoming_buffer.len(), 3);
        assert_eq!(socket.incoming_buffer[2].seq_nr(), 3);
        assert_eq!(socket.incoming_buffer[2].timestamp_microseconds(), 256);

        // Replace a packet with a more recent version
        packet.set_seq_nr(2);
        packet.set_timestamp_microseconds(456);

        socket.insert_into_buffer(packet.clone());
        assert_eq!(socket.incoming_buffer.len(), 3);
        assert_eq!(socket.incoming_buffer[1].seq_nr(), 2);
        assert_eq!(socket.incoming_buffer[1].timestamp_microseconds(), 456);
    }

    #[test]
    fn test_duplicate_packet_handling() {
        let (server_addr, client_addr) = (next_test_ip4(), next_test_ip4());

        let client = iotry!(UtpSocket::bind(client_addr));
        let mut server = iotry!(UtpSocket::bind(server_addr));

        assert!(server.state == SocketState::New);
        assert!(client.state == SocketState::New);

        // Check proper difference in client's send connection id and receive connection id
        assert_eq!(client.sender_connection_id, client.receiver_connection_id + 1);

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            assert!(client.state == SocketState::Connected);

            let mut packet = Packet::new();
            packet.set_wnd_size(BUF_SIZE as u32);
            packet.set_type(PacketType::Data);
            packet.set_connection_id(client.sender_connection_id);
            packet.set_seq_nr(client.seq_nr);
            packet.set_ack_nr(client.ack_nr);
            packet.payload = vec!(1,2,3);

            // Send two copies of the packet, with different timestamps
            for _ in (0u8..2) {
                packet.set_timestamp_microseconds(now_microseconds());
                iotry!(client.socket.send_to(&packet.to_bytes()[..], server_addr));
            }
            client.seq_nr += 1;

            // Receive one ACK
            for _ in (0u8..1) {
                let mut buf = [0; BUF_SIZE];
                iotry!(client.socket.recv_from(&mut buf));
            }

            iotry!(client.close());
        });

        let mut buf = [0u8; BUF_SIZE];
        iotry!(server.recv(&mut buf));
        // After establishing a new connection, the server's ids are a mirror of the client's.
        assert_eq!(server.receiver_connection_id, server.sender_connection_id + 1);

        assert!(server.state == SocketState::Connected);

        let expected: Vec<u8> = vec!(1,2,3);
        let mut received: Vec<u8> = vec!();
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => received.extend(buf[..len].to_vec()),
                Err(e) => panic!("{:?}", e)
            }
        }
        assert_eq!(received.len(), expected.len());
        assert_eq!(received, expected);
    }

    // #[test]
    // #[ignore]
    // fn test_selective_ack_response() {
    //     let (server_addr, client_addr) = (next_test_ip4(), next_test_ip4());
    //     const LEN: usize = 1024 * 10;
    //     let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
    //     let to_send = data.clone();

    //     // Client
    //     thread::spawn(move || {
    //         let client = iotry!(UtpSocket::bind(client_addr));
    //         let mut client = iotry!(UtpSocket::connect(server_addr));
    //         client.congestion_timeout = 50;

    //         iotry!(client.send_to(&to_send[..]));
    //         iotry!(client.close());
    //     });

    //     // Server
    //     let mut server = iotry!(UtpSocket::bind(server_addr));

    //     let mut buf = [0; BUF_SIZE];

    //     // Connect
    //     iotry!(server.recv_from(&mut buf));

    //     // Discard packets
    //     iotry!(server.socket.recv_from(&mut buf));
    //     iotry!(server.socket.recv_from(&mut buf));
    //     iotry!(server.socket.recv_from(&mut buf));

    //     // Generate SACK
    //     let mut packet = Packet::new();
    //     packet.set_seq_nr(server.seq_nr);
    //     packet.set_ack_nr(server.ack_nr - 1);
    //     packet.set_connection_id(server.sender_connection_id);
    //     packet.set_timestamp_microseconds(now_microseconds());
    //     packet.set_type(PacketType::State);
    //     packet.set_sack(vec!(12, 0, 0, 0));

    //     // Send SACK
    //     iotry!(server.socket.send_to(&packet.to_bytes()[..], server.connected_to.clone()));

    //     // Expect to receive "missing" packets
    //     let mut received: Vec<u8> = vec!();
    //     loop {
    //         match server.recv_from(&mut buf) {
    //             Ok((0, _src)) => break,
    //             Ok((len, _src)) => received.extend(buf[..len].to_vec()),
    //             Err(e) => panic!("{:?}", e)
    //         }
    //     }
    //     assert!(!received.is_empty());
    //     assert_eq!(received.len(), data.len());
    //     assert_eq!(received, data);
    // }

    #[test]
    fn test_correct_packet_loss() {
        let server_addr = next_test_ip4();

        let mut server = iotry!(UtpSocket::bind(server_addr));
        const LEN: usize = 1024 * 10;
        let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
        let to_send = data.clone();

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));

            // Send everything except the odd chunks
            let chunks = to_send[..].chunks(BUF_SIZE);
            let dst = client.connected_to;
            for (index, chunk) in chunks.enumerate() {
                let mut packet = Packet::new();
                packet.set_seq_nr(client.seq_nr);
                packet.set_ack_nr(client.ack_nr);
                packet.set_connection_id(client.sender_connection_id);
                packet.set_timestamp_microseconds(now_microseconds());
                packet.payload = chunk.to_vec();
                packet.set_type(PacketType::Data);

                if index % 2 == 0 {
                    iotry!(client.socket.send_to(&packet.to_bytes()[..], dst));
                }

                client.curr_window += packet.len() as u32;
                client.send_window.push(packet);
                client.seq_nr += 1;
            }

            iotry!(client.close());
        });

        let mut buf = [0; BUF_SIZE];
        let mut received: Vec<u8> = vec!();
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => received.extend(buf[..len].to_vec()),
                Err(e) => panic!("{}", e)
            }
        }
        assert_eq!(received.len(), data.len());
        assert_eq!(received, data);
    }

    #[test]
    fn test_tolerance_to_small_buffers() {
        let server_addr = next_test_ip4();
        let mut server = iotry!(UtpSocket::bind(server_addr));
        const LEN: usize = 1024;
        let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
        let to_send = data.clone();

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            iotry!(client.send_to(&to_send[..]));
            iotry!(client.close());
        });

        let mut read = Vec::new();
        while server.state != SocketState::Closed {
            let mut small_buffer = [0; 512];
            match server.recv_from(&mut small_buffer) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => read.extend(small_buffer[..len].to_vec()),
                Err(e) => panic!("{}", e),
            }
        }

        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }

    #[test]
    fn test_sequence_number_rollover() {
        let (server_addr, client_addr) = (next_test_ip4(), next_test_ip4());

        let mut server = iotry!(UtpSocket::bind(server_addr));

        const LEN: usize = BUF_SIZE * 4;
        let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
        let to_send = data.clone();

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::bind(client_addr));

            // Advance socket's sequence number
            client.seq_nr = ::std::u16::MAX - (to_send.len() / (BUF_SIZE * 2)) as u16;

            let mut client = iotry!(UtpSocket::connect(server_addr));
            // Send enough data to rollover
            iotry!(client.send_to(&to_send[..]));
            // Check that the sequence number did rollover
            assert!(client.seq_nr < 50);
            // Close connection
            iotry!(client.close());
        });

        let mut buf = [0; BUF_SIZE];
        let mut received: Vec<u8> = vec!();
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => received.extend(buf[..len].to_vec()),
                Err(e) => panic!("{}", e)
            }
        }
        assert_eq!(received.len(), data.len());
        assert_eq!(received, data);
    }

    #[test]
    fn test_drop_unused_socket() {
        let server_addr = next_test_ip4();
        let server = iotry!(UtpSocket::bind(server_addr));

        // Explicitly dropping socket. This test should not hang.
        drop(server);
    }

    #[test]
    fn test_invalid_packet_on_connect() {
        use std::net::UdpSocket;
        let server_addr = next_test_ip4();
        let server = iotry!(UdpSocket::bind(server_addr));

        thread::spawn(move || {
            let mut buf = [0; BUF_SIZE];
            match server.recv_from(&mut buf) {
                Ok((_len, client_addr)) => { iotry!(server.send_to(&[], client_addr)); },
                _ => panic!()
            }
        });

        match UtpSocket::connect(server_addr) {
            Err(ref e) if e.kind() == ErrorKind::Other => (), // OK
            Err(e) => panic!("Expected ErrorKind::Other, got {:?}", e),
            Ok(_) => panic!("Expected Err, got Ok")
        }
    }

    #[test]
    fn test_receive_unexpected_reply_type_on_connect() {
        use std::net::UdpSocket;
        let server_addr = next_test_ip4();
        let server = iotry!(UdpSocket::bind(server_addr));

        thread::spawn(move || {
            let mut buf = [0; BUF_SIZE];
            let mut packet = Packet::new();
            packet.set_type(PacketType::Data);

            match server.recv_from(&mut buf) {
                Ok((_len, client_addr)) => {
                    iotry!(server.send_to(&packet.to_bytes()[..], client_addr));
                },
                _ => panic!()
            }
        });

        match UtpSocket::connect(server_addr) {
            Err(ref e) if e.kind() == ErrorKind::ConnectionRefused => (), // OK
            Err(e) => panic!("Expected ErrorKind::ConnectionRefused, got {:?}", e),
            Ok(_) => panic!("Expected Err, got Ok")
        }
    }

    #[test]
    fn test_receiving_syn_on_established_connection() {
        // Establish connection
        let server_addr = next_test_ip4();
        let mut server = iotry!(UtpSocket::bind(server_addr));

        thread::spawn(move || {
            let mut buf = [0; BUF_SIZE];
            loop {
                match server.recv_from(&mut buf) {
                    Ok((0, _src)) => break,
                    Ok(_) => (),
                    Err(e) => panic!("{:?}", e)
                }
            }
        });

        let client = iotry!(UtpSocket::connect(server_addr));
        let mut packet = Packet::new();
        packet.set_wnd_size(BUF_SIZE as u32);
        packet.set_type(PacketType::Syn);
        packet.set_connection_id(client.sender_connection_id);
        packet.set_seq_nr(client.seq_nr);
        packet.set_ack_nr(client.ack_nr);
        iotry!(client.socket.send_to(&packet.to_bytes()[..], server_addr));
        let mut buf = [0; BUF_SIZE];
        match client.socket.recv_from(&mut buf) {
            Ok((len, _src)) => {
                let reply = Packet::from_bytes(&buf[..len]).ok().unwrap();
                assert_eq!(reply.get_type(), PacketType::Reset);
            }
            Err(e) => panic!("{:?}", e)
        }
    }

    #[test]
    fn test_receiving_reset_on_established_connection() {
        // Establish connection
        let server_addr = next_test_ip4();
        let mut server = iotry!(UtpSocket::bind(server_addr));

        thread::spawn(move || {
            let client = iotry!(UtpSocket::connect(server_addr));
            let mut packet = Packet::new();
            packet.set_wnd_size(BUF_SIZE as u32);
            packet.set_type(PacketType::Reset);
            packet.set_connection_id(client.sender_connection_id);
            packet.set_seq_nr(client.seq_nr);
            packet.set_ack_nr(client.ack_nr);
            iotry!(client.socket.send_to(&packet.to_bytes()[..], server_addr));
            let mut buf = [0; BUF_SIZE];
            match client.socket.recv_from(&mut buf) {
                Ok((_len, _src)) => (),
                Err(e) => panic!("{:?}", e)
            }
        });

        let mut buf = [0; BUF_SIZE];
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok(_) => (),
                Err(ref e) if e.kind() == ErrorKind::ConnectionReset => return,
                Err(e) => panic!("{:?}", e)
            }
        }
        panic!("Should have received Reset");
    }

    #[test]
    fn test_premature_fin() {
        let (server_addr, client_addr) = (next_test_ip4(), next_test_ip4());
        let mut server = iotry!(UtpSocket::bind(server_addr));

        const LEN: usize = BUF_SIZE * 4;
        let data = (0..LEN).map(|idx| idx as u8).collect::<Vec<u8>>();
        let to_send = data.clone();

        thread::spawn(move || {
            let mut client = iotry!(UtpSocket::connect(server_addr));
            iotry!(client.send_to(&to_send[..]));
            iotry!(client.close());
        });

        let mut buf = [0; BUF_SIZE];

        // Accept connection
        iotry!(server.recv(&mut buf));

        // Send FIN without acknowledging packets received
        let mut packet = Packet::new();
        packet.set_connection_id(server.sender_connection_id);
        packet.set_seq_nr(server.seq_nr);
        packet.set_ack_nr(server.ack_nr);
        packet.set_timestamp_microseconds(now_microseconds());
        packet.set_type(PacketType::Fin);
        iotry!(server.socket.send_to(&packet.to_bytes()[..], client_addr));

        // Receive until end
        let mut received: Vec<u8> = vec!();
        loop {
            match server.recv_from(&mut buf) {
                Ok((0, _src)) => break,
                Ok((len, _src)) => received.extend(buf[..len].to_vec()),
                Err(e) => panic!("{}", e)
            }
        }
        assert_eq!(received.len(), data.len());
        assert_eq!(received, data);
    }

    #[test]
    fn test_base_delay_calculation() {
        let minute_in_microseconds = 60 * 10i64.pow(6);
        let samples = vec![(0, 10), (1, 8), (2, 12), (3, 7),
                           (minute_in_microseconds + 1, 11),
                           (minute_in_microseconds + 2, 19),
                           (minute_in_microseconds + 3, 9)];
        let addr = next_test_ip4();
        let mut socket = UtpSocket::bind(addr).unwrap();

        for (timestamp, delay) in samples{
            socket.update_base_delay(delay, timestamp + delay);
        }

        let expected = vec![7, 9];
        let actual = socket.base_delays.iter().map(|&x| x).collect::<Vec<_>>();
        assert_eq!(expected, actual);
        assert_eq!(socket.min_base_delay(), 7);
    }

    #[test]
    fn test_local_addr() {
        let addr = next_test_ip4();
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let socket = UtpSocket::bind(addr).unwrap();

        assert!(socket.local_addr().is_ok());
        assert_eq!(socket.local_addr().unwrap(), addr);
    }

    #[test]
    fn test_listener_local_addr() {
        let addr = next_test_ip4();
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let listener = UtpListener::bind(addr).unwrap();

        assert!(listener.local_addr().is_ok());
        assert_eq!(listener.local_addr().unwrap(), addr);
    }
}
