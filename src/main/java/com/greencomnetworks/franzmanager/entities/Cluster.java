package com.greencomnetworks.franzmanager.entities;

public class Cluster {
    public String name;
    public String brokersConnectString;
    public String jmxConnectString;
    public String zookeeperConnectString;

    public Cluster() {
    }

    public Cluster(String name, String brokersConnectString, String jmxConnectString, String zookeeperConnectString) {
        this.name = name;
        this.brokersConnectString = brokersConnectString;
        this.jmxConnectString = jmxConnectString;
        this.zookeeperConnectString = zookeeperConnectString;
    }

    @Override
    public String toString() {
        return "Cluster " + name + "\n" + "connectString:" + brokersConnectString +
                "\njmxConnectString: " + jmxConnectString + "\nzookeeperConnectString: " + zookeeperConnectString;
    }
}
