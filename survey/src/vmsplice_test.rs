use crate::common::*;

/// An experiment that shows the racing of `vmsplice`.
///
/// The function forks into two processes. The parent initializes a memory block
/// and passes it to `vmsplice`. Then, repeatedly fills the block immediately.
/// The child receives the block from `vmsplice`, waits for 1 second, and then reads
/// the block repeatedly.
///
/// You will discover the child does not get the initial values from parent, but gets
/// the filled values from 1st, 2nd, or later iterations from parent. It seems the `vmsplice`
/// does not take effect immediately.
pub fn vmsplice_racing() -> Result<()> {
    type Data = [u8; 32];

    unsafe {
        let (read_fd, write_fd) = unistd::pipe()?;

        match unistd::fork()? {
            ForkResult::Parent { child } => run_parent::<Data>(child, write_fd)?,
            ForkResult::Child => run_child::<Data>(read_fd)?,
        }
    }

    Ok(())
}

fn run_parent<T>(_child: Pid, write_fd: RawFd) -> Result<()>
where
    T: Debug,

    Standard: Distribution<T>,
{
    let mut rng = rand::thread_rng();

    unsafe {
        let data = {
            let data: Box<T> = Box::new(rng.gen());
            println!("producer: init {:?}", data);
            data
        };

        let ptr = Box::into_raw(data);

        {
            let slice = slice::from_raw_parts(ptr as *const u8, mem::size_of::<T>());
            let iovec = uio::IoVec::from_slice(slice);
            fcntl::vmsplice(write_fd, &[iovec], fcntl::SpliceFFlags::empty())?;
        }

        let instant = Instant::now();
        let data_ref = ptr.as_mut().unwrap();

        while instant.elapsed() < Duration::from_millis(100) {
            *data_ref = rng.gen();
            println!("producer: fill {:?}", data_ref);
        }
    }

    Ok(())
}

fn run_child<T>(read_fd: RawFd) -> Result<()>
where
    T: Debug,

    Standard: Distribution<T>,
{
    let mut rng = rand::thread_rng();

    unsafe {
        let data = {
            let data: Box<T> = Box::new(rng.gen());
            println!("consumer: init {:?}", data);
            data
        };

        let ptr = Box::into_raw(data);

        {
            let slice = slice::from_raw_parts(ptr as *const u8, mem::size_of::<T>());
            let iovec = uio::IoVec::from_slice(slice);
            fcntl::vmsplice(read_fd, &[iovec], fcntl::SpliceFFlags::empty())?;
        }

        let data_ref = ptr.as_ref().unwrap();

        thread::sleep(Duration::from_millis(1000));
        let instant = Instant::now();

        while instant.elapsed() < Duration::from_millis(100) {
            println!("consumer: read {:?}", data_ref);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::common::*;

    #[test]
    fn vmsplice_racing_test() -> Result<()> {
        super::vmsplice_racing()
    }
}
