KSRC=/lib/modules/$(shell uname -r)/build
KOBJ=/lib/modules/$(shell uname -r)/build


obj-m += rdma_rmm.o
rdma_rmm-y := main.o ring_buffer.o rpc.o mcos.o test.o

default:
	make -C $(KSRC) M=$(PWD) modules 

install:
	make -C $(KSRC) M=$(PWD) modules_install INSTALL_MOD_STRIP=1
	depmod -a

.PHONY: clean
clean:
	rm -f *.o
	rm -f *.ko
	rm -f rdma_rmm.mod.c
	rm -f Module.symvers
	rm -f Module.markers
