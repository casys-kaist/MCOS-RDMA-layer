set -xeu
sudo modprobe rdma_cm
sudo modprobe ib_uverbs
sudo modprobe rdma_ucm
sudo modprobe ib_umad
sudo modprobe ib_ipoib

sudo modprobe device_dax
sudo modprobe dax_pmem
sudo modprobe dax_pmem_core
sudo modprobe remote_pmem

pmem_nid=0
sudo ndctl create-namespace -fe namespace$pmem_nid.0 -m dax -M mem
echo dax$pmem_nid.0 | sudo tee /sys/bus/dax/drivers/device_dax/unbind
echo dax$pmem_nid.0 | sudo tee /sys/bus/dax/drivers/remote_pmem/new_id
