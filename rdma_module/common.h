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

#define MAX_NUM_NODES		(16)
#define NUM_BACKUPS		(1)	
#define NUM_RNICS		2

static uint32_t ip_table[MAX_NUM_NODES] = { 0 };
static uint32_t backup_ip_table[NUM_BACKUPS] = { 0 };
static uint32_t rnic_ip_table[NUM_RNICS] = { 0 };

static uint32_t __init __get_host_ip(void)
{
	struct net_device *d;
	for_each_netdev(&init_net, d) {
		struct in_ifaddr *ifaddr;

		for (ifaddr = d->ip_ptr->ifa_list; ifaddr; ifaddr = ifaddr->ifa_next) {
			int i;
			uint32_t addr = ifaddr->ifa_local;
			for (i = 0; i < NUM_RNICS; i++) {
				if (addr == rnic_ip_table[i]) {
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
	for (i = 0; i < NUM_BACKUPS && i < ARRAY_SIZE(backup_ip_addresses); i++) {
		backup_ip_table[i] = in_aton(backup_ip_addresses[i]);
	}
	for (i = 0; i < NUM_BACKUPS && i < ARRAY_SIZE(rnic_ip_addresses); i++) {
		rnic_ip_table[i] = in_aton(rnic_ip_addresses[i]);
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
