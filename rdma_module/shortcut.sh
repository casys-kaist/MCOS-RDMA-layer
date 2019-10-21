set -x

sudo modprobe -r rdma_rmm || true
make
if [ $? -ne 0 ]; then
    echo "Fail(make) ... $?"
        exit 1
	fi
#sudo make install
sudo mv rdma_rmm.ko /lib/modules/$(uname -r)/kernel/drivers/
sudo depmod -a
sudo modprobe rdma_rmm debug=$1 server=$2

