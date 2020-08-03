#ifndef __MSG_LAYER_CONFIG_H__
#define __MSG_LAYER_CONFIG_H__

/**
 * XXX DO NOT commit your local config file!!! XXX
 */

/*IP order has to follow NID */
const char *ip_addresses[] = {
	"10.0.0.34",
	"10.0.0.36",
	"10.0.0.21",
	"10.0.0.24",
};

#define NID 0 

/* SANGJIN START */
// True if asynchronous replication
// #define CONFIG_RECOVERY 0
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
 * #define CONFIG_RECOVERY 1
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
