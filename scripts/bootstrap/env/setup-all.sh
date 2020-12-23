#!/bin/bash
basedir="/proj/bg-PG0/haoyu"

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt --yes autoremove

bash $basedir/scripts/env/setup-rdma.sh


nic=`netstat -i`
echo "$nic"
IFS=' ' read -r -a array <<< $nic
echo "${array[0]}"
echo "${#array[@]}"
iface=""
for element in "${array[@]}"
do
    if [[ $element == *"enp"* ]]; then
        echo "awefwef:$element"
        iface=$element
    fi
done
echo $iface


sudo bash $basedir/scripts/env/nic.sh -i $iface
sudo bash $basedir/scripts/env/sysctl.sh

sudo apt-get --yes install screen
sudo apt-get --yes install htop
sudo apt-get --yes install maven
sudo apt-get --yes install cmake
sudo apt-get --yes install run-one

sudo mkfs.ext4 /dev/sdb
sudo mkdir /db
sudo mount /dev/sdb /db
sudo chmod 777 /db

sudo mkfs.ext4 /dev/sda4
sudo mkdir /db
sudo mount /dev/sda4 /db
sudo chmod 777 /db

sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o rw,size=20G tmpfs /mnt/ramdisk
sudo chmod 777 /mnt/ramdisk

cd $basedir/gflags && sudo make install

sudo apt-get --yes install elfutils

sudo su -c "echo 'logfile /tmp/screenlog' >> /etc/screenrc"

sudo apt-get install -y software-properties-common #python-software-properties debconf-utils
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
#sudo apt-get install -y oracle-java8-installer
sudo apt-get --yes install openjdk-8-jdk
#sudo apt-get install -y memcached
echo "============"
echo ""
echo "============"
sudo apt-get install -y sysstat
echo "============"
echo ""



# Enable gdb history
echo 'set history save on' >> ~/.gdbinit && chmod 600 ~/.gdbinit

cd /tmp/
wget https://github.com/fmtlib/fmt/releases/download/6.1.2/fmt-6.1.2.zip
unzip fmt-6.1.2.zip
cd fmt-6.1.2/ && cmake . && make -j32 && sudo make install


cd /tmp/
wget https://github.com/Kitware/CMake/releases/download/v3.13.3/cmake-3.13.3.tar.gz
tar -xf cmake-3.13.3.tar.gz
cd cmake-3.13.3 && ./bootstrap && make -j32 && sudo make install

sudo apt install libpopt-dev
sudo apt-get install libiberty-dev
sudo apt-get install binutils-dev

cd /tmp/
wget https://prdownloads.sourceforge.net/oprofile/oprofile-1.3.0.tar.gz
tar -xf oprofile-1.3.0.tar.gz
cd oprofile-1.3.0 && ./configure && make -j32 && sudo make install

# operf /path/to/mybinary
# opreport --symbols

rm -rf /tmp/YCSB-Nova
cp -r "$basedir/YCSB-Nova/" /tmp/
cd /tmp/YCSB-Nova
mvn -pl com.yahoo.ycsb:jdbc-binding -am clean package -DskipTests
