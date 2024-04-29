#!/bin/bash

# Fetching container name
CONTAINER_NAME="${CONTAINER_NAME:-DefaultWorkerName}"

# Starting sesinetd service
/etc/init.d/sesinetd start

wget https://github.com/intel/libva/archive/refs/tags/libva-1.8.3.tar.gz
tar -xzvf libva-1.8.3.tar.gz
cd libva-libva-1.8.3/
chmod +x ./autogen.sh
./autogen.sh
make -j4
make install

apt-get update && apt-get -y upgrade \
  && apt-get install -y \
    apt-utils \
    valgrind \
    unzip \
    tar \
    curl \
    xz-utils \
    ocl-icd-libopencl1 \
    opencl-headers \
    clinfo \
    libopengl0 \
    ;

curl -s -L https://nvidia.github.io/nvidia-container-runtime/gpgkey | \
  apt-key add -
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-container-runtime/$distribution/nvidia-container-runtime.list | \
  tee /etc/apt/sources.list.d/nvidia-container-runtime.list
apt update
apt install -y nvidia-container-runtime

mkdir -p /var/temp/nuke
chmod -R 777 /var/temp/nuke

mkdir -p /etc/OpenCL/vendors && \
    echo "libnvidia-opencl.so.1" > /etc/OpenCL/vendors/nvidia.icd
export NVIDIA_VISIBLE_DEVICES=all
export NVIDIA_DRIVER_CAPABILITIES="compute,utility"

# Starting Deadline Worker without GUI
su -c '/opt/Thinkbox/Deadline10/bin/deadlineworker -nogui -name $CONTAINER_NAME' sadmin