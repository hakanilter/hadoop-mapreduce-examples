package com.devveri.cassandra.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.devveri.cassandra.ProductCluster;
import junit.framework.Assert;
import org.junit.Test;

public class ProductClusterTest {

    @Test
    public void test() {
        Cluster cluster = Cluster.builder()
                .addContactPoint("localhost")
                .build();
        Session session = cluster.connect("product");

        Mapper<ProductCluster> mapper = new MappingManager(session)
                .mapper(ProductCluster.class);

        ProductCluster instance = new ProductCluster();
        instance.setProductId(1001);
        instance.setTitle("iPhone 6");
        instance.setClusterId("1-1");
        mapper.save(instance);

        ProductCluster found = mapper.get(1001);
        Assert.assertNotNull(found);
        Assert.assertEquals(instance.getProductId(), found.getProductId());
        Assert.assertEquals(instance.getTitle(), found.getTitle());
        Assert.assertEquals(instance.getClusterId(), found.getClusterId());
    }

}
