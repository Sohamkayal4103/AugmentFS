## AugmentFS

This project is a simple, custom "passthrough" filesystem written in C++ using FUSE (Filesystem in Userspace). It is intended as a learning project to understand the basics of how filesystems operate.

---
## Initial version (Current)

This initial version of a custom filesystem. It successfully:

- Compiles and mounts a directory.

- Handles the `getattr` and `readdir` operations for the root directory (/).

- Shows an empty, browsable directory at the mount point.

## Requirements

- Linux (e.g., Ubuntu)

- g++ (C++17)

```
make
```

- libfuse-dev (or fuse on some systems)

- pkg-config

## How to Build

- First, ensure you have the necessary libraries installed:
```
sudo apt update
sudo apt install g++ make pkg-config libfuse-dev
```

- Then, compile the program by running make:
```
make
```

- This will create a new executable file named augmentfs.

## How to Run

- In one terminal, run the filesystem:
```
make run
```

- This command will create a ./mount_point directory and then launch the filesystem in the foreground. Your terminal will "hang" and show the "Mounting..." message. This is normal.

- Open a second terminal to test your new filesystem.

- See the contents of your new filesystem
```
ls -l ./mount_point
# This will (correctly) show: total 0
```

- Test asking for a file that doesn't exist
```
ls -l ./mount_point/fake.txt
```
- This will (correctly) show an error:
```
# ls: cannot access './mount_point/fake.txt': No such file or directory
```
- To stop the filesystem, go back to the first terminal and press Cmd+C.

- Clean Up : To remove the compiled executable:
```
make clean
```