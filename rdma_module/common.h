/*
 * common.h
 * Copyright (C) 2017 jackchuang <jackchuang@echo3>
 *
 * Distributed under terms of the MIT license.
 */

#ifndef _MSG_LAYER_COMMON_H_
#define _MSG_LAYER_COMMON_H_

#include <linux/inet.h>
#include <linux/inetdevice.h>
#include <linux/netdevice.h>

#include "config.h"

#define __SERVER__ 0 

#ifndef __SERVER__

#define MAX_NUM_NODES		(ARRAY_SIZE(ip_addresses))
#else
#define MAX_NUM_NODES		(16)
#endif

#define NUM_BACKUP		MAX_NUM_NODES	

static uint32_t ip_table[MAX_NUM_NODES] = { 0 };
static uint32_t ip_table_backup[NUM_BACKUP] = { 0 };

static uint32_t __init __get_host_ip(void)
{
	struct net_device *d;
	for_each_netdev(&init_net, d) {
		struct in_ifaddr *ifaddr;

		for (ifaddr = d->ip_ptr->ifa_list; ifaddr; ifaddr = ifaddr->ifa_next) {
			int i;
			uint32_t addr = ifaddr->ifa_local;
			for (i = 0; i < MAX_NUM_NODES; i++) {
				if (addr == ip_table[i]) {
					return addr;
				}
			}
		}
	}
	return -1;
}

bool __init identify_myself(uint32_t *my_ip)
{
	int i;

	printk("rmm: Loading node configuration...");

	for (i = 0; i < MAX_NUM_NODES && i < ARRAY_SIZE(ip_addresses); i++) {
		ip_table[i] = in_aton(ip_addresses[i]);
	}

#ifdef CONFIG_RM
	for (i = 0; i < NUM_BACKUP && i < ARRAY_SIZE(ip_addresses_backup); i++) {
		printk("rmm: %d, %d\n", i, NUM_BACKUP);
		ip_table_backup[i] = in_aton(ip_addresses_backup[i]);
	}
#endif

	*my_ip = __get_host_ip();
	/*
	for (i = 0; i < MAX_NUM_NODES; i++) {
		char *me = " ";
		if (my_ip == ip_table[i]) {
			my_nid = i;
			me = "*";
		}
		printk("RMM: " "%s %d: %pI4\n", me, i, ip_table + i);
	}

	if (my_nid < 0) {
		printk(KERN_ERR "RMM: My IP is not listed in the node configuration\n");
		return false;
	}
	*/

	return true;
}
#endif
