//! Descriptor-anchored, create-only publication of private artifacts.

use std::{
    ffi::CString,
    fs::File,
    io::Write,
    os::fd::{AsRawFd, FromRawFd},
    os::unix::ffi::OsStrExt,
    path::{Component, Path},
};

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureOutputCode {
    UnsafeOutputPath,
    OutputExists,
    AtomicCreateUnavailable,
    AtomicCreateFailed,
    OutputIo,
}

impl SecureOutputCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsafeOutputPath => "unsafe_output_path",
            Self::OutputExists => "output_exists",
            Self::AtomicCreateUnavailable => "atomic_create_unavailable",
            Self::AtomicCreateFailed => "atomic_create_failed",
            Self::OutputIo => "output_io",
        }
    }
}

impl std::fmt::Display for SecureOutputCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Error)]
#[error("{code}: {context}")]
pub struct SecureOutputError {
    pub code: SecureOutputCode,
    pub context: &'static str,
    /// True only when the complete target has been published but the parent
    /// directory fsync failed. Callers must never unlink in this state.
    pub published: bool,
    errno: Option<i32>,
}

impl SecureOutputError {
    fn new(code: SecureOutputCode, context: &'static str) -> Self {
        Self {
            code,
            context,
            published: false,
            errno: None,
        }
    }
}

fn c_name(path: &std::ffi::OsStr) -> Result<CString, SecureOutputError> {
    CString::new(path.as_bytes()).map_err(|_| {
        SecureOutputError::new(SecureOutputCode::UnsafeOutputPath, "path contains NUL")
    })
}

fn io(context: &'static str) -> SecureOutputError {
    SecureOutputError::new(SecureOutputCode::OutputIo, context)
}

fn open_dir_at(parent: &File, name: &std::ffi::OsStr) -> Result<File, SecureOutputError> {
    let name = c_name(name)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        let errno = std::io::Error::last_os_error().raw_os_error();
        let mut error = SecureOutputError::new(
            SecureOutputCode::UnsafeOutputPath,
            "parent component is not a real directory",
        );
        error.errno = errno;
        return Err(error);
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn open_parent(path: &Path) -> Result<(File, CString), SecureOutputError> {
    #[cfg(target_os = "macos")]
    let normalized = super::archive::normalize_macos_trusted_root_alias(path);
    #[cfg(target_os = "macos")]
    let path = normalized.as_path();
    let name = path.file_name().ok_or_else(|| {
        SecureOutputError::new(
            SecureOutputCode::UnsafeOutputPath,
            "output has no file name",
        )
    })?;
    if name == "." || name == ".." || name.as_bytes().is_empty() {
        return Err(SecureOutputError::new(
            SecureOutputCode::UnsafeOutputPath,
            "invalid output name",
        ));
    }
    let parent_path = path.parent().unwrap_or_else(|| Path::new("."));
    let start = if path.is_absolute() {
        Path::new("/")
    } else {
        Path::new(".")
    };
    let start_name = c_name(start.as_os_str())?;
    let fd = unsafe {
        libc::open(
            start_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(io("cannot anchor output path"));
    }
    let mut dir = unsafe { File::from_raw_fd(fd) };
    let components: Vec<_> = parent_path.components().collect();
    let normal_count = components
        .iter()
        .filter(|c| matches!(c, Component::Normal(_)))
        .count();
    let mut seen = 0usize;
    for component in components {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(part) => {
                seen += 1;
                match open_dir_at(&dir, part) {
                    Ok(next) => dir = next,
                    Err(error) if seen == normal_count && error.errno == Some(libc::ENOENT) => {
                        let part = c_name(part)?;
                        if unsafe { libc::mkdirat(dir.as_raw_fd(), part.as_ptr(), 0o700) } != 0 {
                            return Err(io("cannot create final output parent"));
                        }
                        dir = open_dir_at(&dir, std::ffi::OsStr::from_bytes(part.as_bytes()))?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(SecureOutputError::new(
                    SecureOutputCode::UnsafeOutputPath,
                    "parent traversal is not allowed",
                ))
            }
        }
    }
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(dir.as_raw_fd(), &mut stat) } != 0 {
        return Err(io("cannot inspect output parent"));
    }
    let uid = unsafe { libc::geteuid() };
    let mode = stat.st_mode as libc::mode_t;
    let owner_private = stat.st_uid == uid && mode & 0o022 == 0;
    #[cfg(target_os = "linux")]
    let trusted_tmp_path = parent_path == Path::new("/tmp");
    #[cfg(target_os = "macos")]
    let trusted_tmp_path = parent_path == Path::new("/private/tmp");
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let trusted_tmp_path = false;
    let trusted_sticky = trusted_tmp_path && stat.st_uid == 0 && mode & 0o7777 == 0o1777;
    if !owner_private && !trusted_sticky {
        return Err(SecureOutputError::new(
            SecureOutputCode::UnsafeOutputPath,
            "final parent must be owner-private or trusted sticky",
        ));
    }
    Ok((dir, c_name(name)?))
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::{
            fs::{symlink, PermissionsExt},
            net::UnixListener,
        },
        sync::{Arc, Barrier},
        thread,
    };

    #[test]
    fn writes_complete_private_file_and_optional_parent() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("new").join("bundle.jsonl");
        write_secure_output(&target, b"one\ntwo\n").unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"one\ntwo\n");
        assert_eq!(
            fs::metadata(target.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(fs::read_dir(target.parent().unwrap()).unwrap().count(), 1);
    }

    #[test]
    fn refuses_every_existing_target_kind_without_blocking_or_mutation() {
        let root = tempfile::tempdir().unwrap();
        let regular = root.path().join("regular");
        fs::write(&regular, b"old").unwrap();
        let hard = root.path().join("hard");
        fs::hard_link(&regular, &hard).unwrap();
        let link = root.path().join("link");
        symlink("missing", &link).unwrap();
        let dir = root.path().join("dir");
        fs::create_dir(&dir).unwrap();
        let fifo = root.path().join("fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        let socket = root.path().join("socket");
        let _listener = UnixListener::bind(&socket).unwrap();
        for path in [&regular, &hard, &link, &dir, &fifo, &socket] {
            let error = write_secure_output(path, b"new").unwrap_err();
            assert_eq!(
                error.code,
                SecureOutputCode::OutputExists,
                "{}",
                path.display()
            );
        }
        assert_eq!(fs::read(regular).unwrap(), b"old");
    }

    #[test]
    fn rejects_symlink_and_other_writable_parent_but_accepts_sticky_tmp() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = root.path().join("link");
        symlink(&real, &link).unwrap();
        assert_eq!(
            write_secure_output(&link.join("x"), b"x").unwrap_err().code,
            SecureOutputCode::UnsafeOutputPath
        );
        fs::set_permissions(&real, fs::Permissions::from_mode(0o777)).unwrap();
        assert_eq!(
            write_secure_output(&real.join("x"), b"x").unwrap_err().code,
            SecureOutputCode::UnsafeOutputPath
        );
        fs::set_permissions(&real, fs::Permissions::from_mode(0o1777)).unwrap();
        assert_eq!(
            write_secure_output(&real.join("fake-sticky"), b"x")
                .unwrap_err()
                .code,
            SecureOutputCode::UnsafeOutputPath
        );
        let tmp_target = Path::new("/tmp").join(format!("ctx-secure-output-{}", Uuid::new_v4()));
        write_secure_output(&tmp_target, b"tmp").unwrap();
        assert_eq!(fs::read(&tmp_target).unwrap(), b"tmp");
        fs::remove_file(tmp_target).unwrap();
    }

    #[test]
    fn publication_race_has_exactly_one_winner_and_no_residue() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("winner");
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let target = target.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    write_secure_output(&target, format!("{i}").as_bytes())
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| e.code == SecureOutputCode::OutputExists));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn rejects_invalid_or_missing_intermediate_paths() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            write_secure_output(&root.path().join("a/b/out"), b"x")
                .unwrap_err()
                .code,
            SecureOutputCode::UnsafeOutputPath
        );
        assert_eq!(
            write_secure_output(Path::new("..").join("out").as_path(), b"x")
                .unwrap_err()
                .code,
            SecureOutputCode::UnsafeOutputPath
        );
    }

    #[test]
    fn unavailable_primitive_cleans_temp_and_leaves_target_absent() {
        fn unavailable(_: &File, _: &CString, _: &CString) -> Result<(), i32> {
            Err(libc::ENOSYS)
        }
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("bundle");
        let error = write_secure_output_with(&target, b"complete", unavailable).unwrap_err();
        assert_eq!(error.code, SecureOutputCode::AtomicCreateUnavailable);
        assert!(!target.exists());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }
}

fn target_absent(parent: &File, target: &CString) -> Result<(), SecureOutputError> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            target.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 {
        return Err(SecureOutputError::new(
            SecureOutputCode::OutputExists,
            "output target already exists",
        ));
    }
    if std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(io("cannot inspect output target"))
    }
}

type Publish = fn(&File, &CString, &CString) -> Result<(), i32>;

fn publish_noreplace(parent: &File, source: &CString, target: &CString) -> Result<(), i32> {
    let rc = unsafe {
        #[cfg(target_os = "linux")]
        {
            libc::syscall(
                libc::SYS_renameat2,
                parent.as_raw_fd(),
                source.as_ptr(),
                parent.as_raw_fd(),
                target.as_ptr(),
                1_u32,
            ) as libc::c_int
        }
        #[cfg(target_os = "macos")]
        {
            libc::renameatx_np(
                parent.as_raw_fd(),
                source.as_ptr(),
                parent.as_raw_fd(),
                target.as_ptr(),
                libc::RENAME_EXCL,
            )
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            return Err(libc::ENOSYS);
        }
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    }
}

/// Publish `bytes` at an absent target. The target is never opened.
pub fn write_secure_output(path: &Path, bytes: &[u8]) -> Result<(), SecureOutputError> {
    write_secure_output_with(path, bytes, publish_noreplace)
}

fn write_secure_output_with(
    path: &Path,
    bytes: &[u8],
    publish: Publish,
) -> Result<(), SecureOutputError> {
    let (parent, target) = open_parent(path)?;
    target_absent(&parent, &target)?;
    let temp =
        CString::new(format!(".ctx-evidence-{}.tmp", Uuid::new_v4())).expect("UUID temp name");
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            temp.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io("cannot create private temporary output"));
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    let prepublish = (|| {
        file.write_all(bytes)
            .map_err(|_| io("cannot write private temporary output"))?;
        file.sync_all()
            .map_err(|_| io("cannot sync private temporary output"))?;
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
            return Err(io("cannot verify private temporary output"));
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG
            || stat.st_uid != unsafe { libc::geteuid() }
            || stat.st_nlink != 1
            || stat.st_size < 0
            || stat.st_size as usize != bytes.len()
        {
            return Err(io("private temporary output failed verification"));
        }
        target_absent(&parent, &target)?;
        if let Err(errno) = publish(&parent, &temp, &target) {
            if errno == libc::EEXIST {
                return Err(SecureOutputError::new(
                    SecureOutputCode::OutputExists,
                    "output target won publication race",
                ));
            }
            if matches!(errno, libc::ENOSYS | libc::ENOTSUP) {
                return Err(SecureOutputError::new(
                    SecureOutputCode::AtomicCreateUnavailable,
                    "conditional no-replace publication is unavailable",
                ));
            }
            return Err(SecureOutputError::new(
                SecureOutputCode::AtomicCreateFailed,
                "conditional no-replace publication failed",
            ));
        }
        Ok(())
    })();
    drop(file);
    if let Err(error) = prepublish {
        unsafe { libc::unlinkat(parent.as_raw_fd(), temp.as_ptr(), 0) };
        return Err(error);
    }
    if parent.sync_all().is_err() {
        return Err(SecureOutputError {
            code: SecureOutputCode::OutputIo,
            context: "output published but parent directory sync failed",
            published: true,
            errno: None,
        });
    }
    Ok(())
}
