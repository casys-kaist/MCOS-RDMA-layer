#ifndef __MSG_LAYER_CONFIG_H__
#define __MSG_LAYER_CONFIG_H__

/**
 * XXX DO NOT commit your local config file!!! XXX
 */

const char *ip_addresses[] = {
	/* Memory */ "10.0.0.24",
	/* Backup */
};

const char *backup_ip_addresses[] = {
	/* Node 0 */ 
};

const char *rnic_ip_addresses[] = {
	/* Node 0 */ "10.0.0.26",
};

#define NID 0

/* SANGJIN START */
// True if asynchronous replication
#define BACKUP 1
/* SANGJIN END */

#endif


/* SANGJIN DOCUMENTATION 
 *
 * +++++++++++++++++   sync   +++++++++++++++++    async   +++++++++++++++++++
 * + Memory Server +  ----->  + Backup Server +    ----->  + Recovery Server +
 * +++++++++++++++++          +++++++++++++++++            +++++++++++++++++++
 *       ^		            ^
 *       | sync	                    |	
 *       |                          | recovery rpc
 *  ++++++++++++++		    |	
 *  + CPU Server +  _________________
 *  ++++++++++++++
 *
 * 
 *  1) Memory Server -> Backup Server : synchronous replication
 *  2) Backup Server -> Recovery Server : asynchronous replication
 *  3) Memory Server dead
 *  4) CPU Server --- recovery rpc ---> Backup Server : Wait until Backup Server and Recovery Server consistent 
 *  5) (Memory Server, Backup Server, Recovery Server) -> (Recovery Server, Memory Server, Backup Server)
 *
 * ____________________________________________
 *
 * CPU Server Config
 *
 * const char *ip_addresses[] = {
 *	Memory Server 
 *	Backup Server
 * };
 *
 * const char *backup_ip_addresses[] = {
 * };
 *
 * const char *rnic_ip_addresses[] = {
 *	CPU Server
 * }
 * 
 * ____________________________________________
 * 
 * Memory Server Config
 *
 * const char *ip_addresses[] = {
 * };
 *
 * const char *backup_ip_addresses[] = {
 *	Backup Server
 * };
 *
 * const char *rnic_ip_addresses[] = {
 *	Memory Server
 * };
 *
 * ___________________________________________
 *
 * Backup Server Config
 *
 * const char *ip_addresses[] = {
 * };
 *
 * const char *backup_ip_addresses[] = {
 *	Recovery Server
 * };
 *
 * const char *rnic_ip_addresses[] = {
 *	Backup Server
 * };
 *
 * #define BACKUP 1
 *
 * __________________________________________
 *
 * Recovery Server Config
 *
 * const char *ip_addresses[] = {
 * };
 *
 * const char *backup_ip_addresses[] = {
 * };
 * 
 * const char *rnic_ip_addresses[] = {
 * 	Recovery Server
 * };
 *
 */
