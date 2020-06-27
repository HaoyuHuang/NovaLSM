### KERNEL TUNING ###

# Increase size of file handles and inode cache
sudo sysctl -w fs.file-max=10000000
sudo sysctl -w fs.nr_open=10000000

# Do less swapping
sudo sysctl -w vm.swappiness=10
sudo sysctl -w vm.dirty_ratio=60
sudo sysctl -w vm.dirty_background_ratio=2

# Sets the time before the kernel considers migrating a proccess to another core
sudo sysctl -w kernel.sched_migration_cost_ns=5000000

# Group tasks by TTY
#kernel.sched_autogroup_enabled=0

### GENERAL NETWORK SECURITY OPTIONS ###

# Number of times SYNACKs for passive TCP connection.
sudo sysctl -w net.ipv4.tcp_synack_retries=2

# Allowed local port range
sudo sysctl -w net.ipv4.ip_local_port_range="512 65535"

# Protect Against TCP Time-Wait
sudo sysctl -w net.ipv4.tcp_rfc1337=1

# Control Syncookies
sudo sysctl -w net.ipv4.tcp_syncookies=1

# Decrease the time default value for tcp_fin_timeout connection
sudo sysctl -w net.ipv4.tcp_fin_timeout=15

# Decrease the time default value for connections to keep alive
sudo sysctl -w net.ipv4.tcp_keepalive_time=300
sudo sysctl -w net.ipv4.tcp_keepalive_probes=5
sudo sysctl -w net.ipv4.tcp_keepalive_intvl=15

### TUNING NETWORK PERFORMANCE ###

# Default Socket Receive Buffer
sudo sysctl -w net.core.rmem_default=4194304

# Maximum Socket Receive Buffer
sudo sysctl -w net.core.rmem_max=4194304

# Default Socket Send Buffer
sudo sysctl -w net.core.wmem_default=4194304

# Maximum Socket Send Buffer
sudo sysctl -w net.core.wmem_max=4194304

# Increase number of incoming connections
sudo sysctl -w net.core.somaxconn=250000

# Increase number of incoming connections backlog
sudo sysctl -w net.core.netdev_max_backlog=250000

# Increase the maximum amount of option memory buffers
sudo sysctl -w net.core.optmem_max=25165824

# Increase the maximum total buffer-space allocatable
# This is measured in units of pages (4096 bytes)
sudo sysctl -w net.ipv4.tcp_mem="4194304 4194304 4194304"

# Increase the read-buffer space allocatable 32 MB
sudo sysctl -w net.ipv4.tcp_rmem="8192 87380 4194304"

# Increase the write-buffer-space allocatable 32 MB
sudo sysctl -w net.ipv4.tcp_wmem="8192 65536 4194304"

# Increase the tcp-time-wait buckets pool size to prevent simple DOS attacks
sudo sysctl -w net.ipv4.tcp_max_tw_buckets=1440000
sudo sysctl -w net.ipv4.tcp_tw_recycle=1
sudo sysctl -w net.ipv4.tcp_tw_reuse=1

sudo sysctl -w net.ipv4.tcp_timestamps=0
sudo sysctl -w net.ipv4.tcp_sack=1
sudo sysctl -w net.ipv4.tcp_adv_win_scale=1

sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0


sudo sysctl -w net.ipv4.ip_local_port_range="512 65535"

# sudo sysctl -w net.ipv4.route.flush=1