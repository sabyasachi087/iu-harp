package org.apache.hadoop.mapred;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

public class MapCollectiveScheduler extends CapacityScheduler {
	/**
	 * With the Capacity Scheduler , a separate dedicated queue allows the small
	 * job to start as soon as it is submitted, although this is at the cost of
	 * overall cluster utilization since the queue capacity is reserved for jobs
	 * in that queue. This means that the large job finishes later than when
	 * using the FIFO Scheduler.
	 **/
}
