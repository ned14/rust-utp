use std::io::{Read, Write, Result};
use std::net::{ToSocketAddrs, SocketAddr};
use std::ops::Deref;
use socket::UtpSocket;

/// A structure that represents a uTP (Micro Transport Protocol) stream between a local socket and a
/// remote socket.
///
/// The connection will be closed when the value is dropped (either explicitly or when it goes out of
/// scope).
///
/// # Examples
///
/// ```no_run
/// use utp::UtpStream;
/// use std::io::{Read, Write};
///
/// let mut stream = UtpStream::bind("127.0.0.1:1234").unwrap();
/// let _ = stream.write(&[1]);
/// let _ = stream.read(&mut [0; 1000]);
/// ```
pub struct UtpStream {
    socket: UtpSocket,
}

impl UtpStream {
    /// Creates a uTP stream listening on the given address.
    ///
    /// The address type can be any implementor of the `ToSocketAddr` trait. See its documentation
    /// for concrete examples.
    ///
    /// If more than one valid address is specified, only the first will be used.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<UtpStream> {
        UtpSocket::bind(addr).and_then(|s| Ok(UtpStream { socket: s }))
    }

    /// Opens a uTP connection to a remote host by hostname or IP address.
    ///
    /// The address type can be any implementor of the `ToSocketAddr` trait. See its documentation
    /// for concrete examples.
    ///
    /// If more than one valid address is specified, only the first will be used.
    pub fn connect<A: ToSocketAddrs>(dst: A) -> Result<UtpStream> {
        // Port 0 means the operating system gets to choose it
        UtpSocket::connect(dst)
            .and_then(|s| Ok(UtpStream { socket: s }))
    }

    /// Gracefully closes connection to peer.
    ///
    /// This method allows both peers to receive all packets still in
    /// flight.
    pub fn close(&mut self) -> Result<()> {
        self.socket.close()
    }

    /// Returns the socket address of the local half of this uTP connection.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl Read for UtpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.socket.recv_from(buf).map(|(read, _src)| read)
    }
}

impl Write for UtpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.socket.send_to(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.socket.flush()
    }
}

impl Into<UtpStream> for UtpSocket {
    fn into(self) -> UtpStream {
        UtpStream { socket: self }
    }
}

impl Deref for UtpStream {
    type Target = UtpSocket;

    fn deref(&self) -> &UtpSocket {
        &self.socket
    }
}
