set -x

sudo modprobe -r rdma_krping || true
make
if [ $? -ne 0 ]; then
    echo "Fail(make) ... $?"
        exit 1
	fi
#sudo make install
sudo mv rdma_krping.ko /lib/modules/$(uname -r)/kernel/drivers/
sudo depmod -a
sudo modprobe rdma_krping debug=$1

