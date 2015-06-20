// Copyright 2015 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement, version 1.0.  This, along with the
// Licenses can be found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

use std::io::{Error, ErrorKind};
use std::net::{UdpSocket, SocketAddr};
use libc;

#[cfg(any(target_os = "macos", target_os = "ios"))]
mod select {
    use libc;
    pub const FD_SETSIZE: usize = 1024;

    #[repr(C)]
    pub struct fd_set {
        fds_bits: [i32; (FD_SETSIZE / 32)]
    }
    
    pub fn fd_set(set: &mut fd_set, fd: i32) {
        set.fds_bits[(fd / 32) as usize] |= 1 << ((fd % 32) as usize);
    }
    
    extern {
        pub fn select(nfds: libc::c_int,
                  readfds: *mut fd_set,
                  writefds: *mut fd_set,
                  errorfds: *mut fd_set,
                  timeout: *mut libc::timeval) -> libc::c_int;
    }
}

#[cfg(any(target_os = "android",
          target_os = "freebsd",
          target_os = "dragonfly",
          target_os = "bitrig",
          target_os = "openbsd",
          target_os = "linux"))]
mod select {
    use libc;
    pub const FD_SETSIZE: usize = 1024;

    #[repr(C)]
    pub struct fd_set {
        // FIXME: shouldn't this be a c_ulong?
        fds_bits: [libc::uintptr_t; (FD_SETSIZE / usize::BITS)]
    }

    pub fn fd_set(set: &mut fd_set, fd: i32) {
        let fd = fd as usize;
        set.fds_bits[fd / usize::BITS] |= 1 << (fd % usize::BITS);
    }
    
    extern {
        pub fn select(nfds: libc::c_int,
                  readfds: *mut fd_set,
                  writefds: *mut fd_set,
                  errorfds: *mut fd_set,
                  timeout: *mut libc::timeval) -> libc::c_int;
    }
}

#[cfg(windows)]
mod select {
    use libc;
    pub const FD_SETSIZE: usize = 64;

    #[repr(C)]
    pub struct fd_set {
        fd_count: libc::c_uint,
        fd_array: [libc::SOCKET; FD_SETSIZE],
    }

    pub fn fd_set(set: &mut fd_set, s: libc::SOCKET) {
        set.fd_array[set.fd_count as usize] = s;
        set.fd_count += 1;
    }

    #[link(name = "ws2_32")]
    extern "system" {
        pub fn select(nfds: libc::c_int,
                  readfds: *mut fd_set,
                  writefds: *mut fd_set,
                  errorfds: *mut fd_set,
                  timeout: *mut libc::timeval) -> libc::c_int;
    }
}

use self::select::{fd_set, select};

/// Read a packet from a UdpSocket, if none arrives within timeout return false
pub fn timed_read(socket: &UdpSocket, buf: &mut [u8], timeout : u32) -> Result<(usize, SocketAddr)> {
    let mut readfds : fd_set;
    let mut writefds : fd_set;
    let mut errorfds : fd_set;
    
    fd_set(&readfds, socket.as_raw_fd());
    let mut tv = libc::timeval { tv_sec: timeout / 1000, tv_usec : (timeout % 1000) * 1000 };
    match select(&readfds, &writefds, &errorfds, &tv) {
        -1 => Error::last_os_error(),
        _ => socket.recv_from(buf),
    }
}

#[cfg(test)]
mod test {
    use super::timed_read;
    
    #[test]
    fn test_timed_read() {
        let s = UdpSocket::bind("127.0.0.1:0");
        let r = timed_read(&s, 500);
        assert!(r.is_err() && r.err().unwrap().kind()==ErrorKind::TimedOut);
    }
}
